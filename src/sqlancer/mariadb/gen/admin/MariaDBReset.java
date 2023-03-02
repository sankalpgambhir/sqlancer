package sqlancer.mariadb.gen.admin;

import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.mariadb.MariaDBGlobalState;

public final class MariaDBReset {

    private MariaDBReset() {
    }

    public static SQLQueryAdapter create(MariaDBGlobalState globalState) {
        StringBuilder sb = new StringBuilder();
        sb.append("RESET ");
        sb.append(Randomly.nonEmptySubset("MASTER", "SLAVE").stream().collect(Collectors.joining(", ")));
        return new SQLQueryAdapter(sb.toString());
    }

}
