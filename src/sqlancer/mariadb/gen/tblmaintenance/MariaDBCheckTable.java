package sqlancer.mariadb.gen.tblmaintenance;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.mariadb.MariaDBGlobalState;
import sqlancer.mariadb.MariaDBSchema.MariaDBTable;

/**
 * @see <a href="https://dev.mariadb.com/doc/refman/8.0/en/check-table.html">CHECK TABLE Statement</a>
 */
public class MariaDBCheckTable {

    private final List<MariaDBTable> tables;
    private final StringBuilder sb = new StringBuilder();

    public MariaDBCheckTable(List<MariaDBTable> tables) {
        this.tables = tables;
    }

    public static SQLQueryAdapter check(MariaDBGlobalState globalState) {
        return new MariaDBCheckTable(globalState.getSchema().getDatabaseTablesRandomSubsetNotEmpty()).generate();
    }

    // CHECK TABLE tbl_name [, tbl_name] ... [option] ...
    //
    // option: {
    // FOR UPGRADE
    // | QUICK
    // | FAST
    // | MEDIUM
    // | EXTENDED
    // | CHANGED
    // }
    private SQLQueryAdapter generate() {
        sb.append("CHECK TABLE ");
        sb.append(tables.stream().map(t -> t.getName()).collect(Collectors.joining(", ")));
        sb.append(" ");
        List<String> options = Randomly.subset("FOR UPGRADE", "QUICK", "FAST", "MEDIUM", "EXTENDED", "CHANGED");
        sb.append(options.stream().collect(Collectors.joining(" ")));
        return new SQLQueryAdapter(sb.toString());
    }

}
