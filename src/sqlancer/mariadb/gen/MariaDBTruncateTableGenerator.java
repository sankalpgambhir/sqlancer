package sqlancer.mariadb.gen;

import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.mariadb.MariaDBGlobalState;

public final class MariaDBTruncateTableGenerator {

    private MariaDBTruncateTableGenerator() {
    }

    public static SQLQueryAdapter generate(MariaDBGlobalState globalState) {
        StringBuilder sb = new StringBuilder("TRUNCATE TABLE ");
        sb.append(globalState.getSchema().getRandomTable().getName());
        return new SQLQueryAdapter(sb.toString(), ExpectedErrors.from("doesn't have this option"));
    }

}
