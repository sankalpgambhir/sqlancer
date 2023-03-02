package sqlancer.mariadb.gen.tblmaintenance;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.mariadb.MariaDBGlobalState;
import sqlancer.mariadb.MariaDBSchema.MariaDBTable;
import sqlancer.mariadb.MariaDBSchema.MariaDBTable.MariaDBEngine;

/**
 * @see <a href="https://dev.mariadb.com/doc/refman/8.0/en/repair-table.html">REPAIR TABLE Statement</a>
 */
public class MariaDBRepair {

    private final List<MariaDBTable> tables;
    private final StringBuilder sb = new StringBuilder();

    public MariaDBRepair(List<MariaDBTable> tables) {
        this.tables = tables;
    }

    public static SQLQueryAdapter repair(MariaDBGlobalState globalState) {
        List<MariaDBTable> tables = globalState.getSchema().getDatabaseTablesRandomSubsetNotEmpty();
        for (MariaDBTable table : tables) {
            // see https://bugs.mariadb.com/bug.php?id=95820
            if (table.getEngine() == MariaDBEngine.MY_ISAM) {
                return new SQLQueryAdapter("SELECT 1");
            }
        }
        return new MariaDBRepair(tables).repair();
    }

    // REPAIR [NO_WRITE_TO_BINLOG | LOCAL]
    // TABLE tbl_name [, tbl_name] ...
    // [QUICK] [EXTENDED] [USE_FRM]
    private SQLQueryAdapter repair() {
        sb.append("REPAIR");
        if (Randomly.getBoolean()) {
            sb.append(" ");
            sb.append(Randomly.fromOptions("NO_WRITE_TO_BINLOG", "LOCAL"));
        }
        sb.append(" TABLE ");
        sb.append(tables.stream().map(t -> t.getName()).collect(Collectors.joining(", ")));
        if (Randomly.getBoolean()) {
            sb.append(" QUICK");
        }
        if (Randomly.getBoolean()) {
            sb.append(" EXTENDED");
        }
        if (Randomly.getBoolean()) {
            sb.append(" USE_FRM");
        }
        return new SQLQueryAdapter(sb.toString());
    }

}
