package sqlancer.mariadb.gen.tblmaintenance;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.mariadb.MariaDBGlobalState;
import sqlancer.mariadb.MariaDBSchema.MariaDBTable;

/**
 * @see <a href="https://dev.mariadb.com/doc/refman/8.0/en/checksum-table.html">CHECKSUM TABLE Statement</a>
 */
public class MariaDBChecksum {

    private final List<MariaDBTable> tables;
    private final StringBuilder sb = new StringBuilder();

    public MariaDBChecksum(List<MariaDBTable> tables) {
        this.tables = tables;
    }

    public static SQLQueryAdapter checksum(MariaDBGlobalState globalState) {
        return new MariaDBChecksum(globalState.getSchema().getDatabaseTablesRandomSubsetNotEmpty()).checksum();
    }

    // CHECKSUM TABLE tbl_name [, tbl_name] ... [QUICK | EXTENDED]
    private SQLQueryAdapter checksum() {
        sb.append("CHECKSUM TABLE ");
        sb.append(tables.stream().map(t -> t.getName()).collect(Collectors.joining(", ")));
        if (Randomly.getBoolean()) {
            sb.append(" ");
            sb.append(Randomly.fromOptions("QUICK", "EXTENDED"));
        }
        return new SQLQueryAdapter(sb.toString());
    }

}
