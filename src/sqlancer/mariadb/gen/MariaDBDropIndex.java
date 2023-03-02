package sqlancer.mariadb.gen;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.mariadb.MariaDBGlobalState;
import sqlancer.mariadb.MariaDBSchema.MariaDBTable;

/**
 * @see <a href="https://dev.mariadb.com/doc/refman/8.0/en/drop-index.html">DROP INDEX Statement</a>
 */
public final class MariaDBDropIndex {

    private MariaDBDropIndex() {
    }

    // DROP INDEX index_name ON tbl_name
    // [algorithm_option | lock_option] ...
    //
    // TODO: no options for MariaDB?
    // algorithm_option:
    // ALGORITHM [=] {DEFAULT|INPLACE|COPY}
    //
    // lock_option:
    // LOCK [=] {DEFAULT|NONE|SHARED|EXCLUSIVE}

    public static SQLQueryAdapter generate(MariaDBGlobalState globalState) {
        MariaDBTable table = globalState.getSchema().getRandomTable();
        if (!table.hasIndexes()) {
            throw new IgnoreMeException();
        }
        StringBuilder sb = new StringBuilder();
        sb.append("DROP INDEX ");
        sb.append(table.getRandomIndex().getIndexName());
        sb.append(" ON ");
        sb.append(table.getName());
        // if (Randomly.getBoolean()) {
        //     sb.append(" ALGORITHM=");
        //     sb.append(Randomly.fromOptions("DEFAULT", "INPLACE", "COPY"));
        // }
        // if (Randomly.getBoolean()) {
        //     sb.append(" LOCK=");
        //     sb.append(Randomly.fromOptions("DEFAULT", "NONE", "SHARED", "EXCLUSIVE"));
        // }
        return new SQLQueryAdapter(sb.toString(),
                ExpectedErrors.from());
    }

}
