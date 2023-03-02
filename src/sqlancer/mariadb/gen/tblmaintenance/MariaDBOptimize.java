package sqlancer.mariadb.gen.tblmaintenance;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.mariadb.MariaDBGlobalState;
import sqlancer.mariadb.MariaDBSchema.MariaDBTable;

/**
 * @see <a href="https://dev.mariadb.com/doc/refman/8.0/en/optimize-table.html">OPTIMIZE TABLE Statement</a>
 */
public class MariaDBOptimize {

    private final List<MariaDBTable> tables;
    private final StringBuilder sb = new StringBuilder();

    public MariaDBOptimize(List<MariaDBTable> tables) {
        this.tables = tables;
    }

    public static SQLQueryAdapter optimize(MariaDBGlobalState globalState) {
        return new MariaDBOptimize(globalState.getSchema().getDatabaseTablesRandomSubsetNotEmpty()).optimize();
    }

    // OPTIMIZE [NO_WRITE_TO_BINLOG | LOCAL]
    // TABLE tbl_name [, tbl_name] ...
    private SQLQueryAdapter optimize() {
        sb.append("OPTIMIZE");
        if (Randomly.getBoolean()) {
            sb.append(" ");
            sb.append(Randomly.fromOptions("NO_WRITE_TO_BINLOG", "LOCAL"));
        }
        sb.append(" TABLE ");
        sb.append(tables.stream().map(t -> t.getName()).collect(Collectors.joining(", ")));
        return new SQLQueryAdapter(sb.toString());
    }

}
