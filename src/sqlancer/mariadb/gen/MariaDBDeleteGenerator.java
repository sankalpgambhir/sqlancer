package sqlancer.mariadb.gen;

import java.util.Arrays;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.mariadb.MariaDBErrors;
import sqlancer.mariadb.MariaDBGlobalState;
import sqlancer.mariadb.MariaDBSchema.MariaDBTable;
import sqlancer.mariadb.MariaDBVisitor;

public class MariaDBDeleteGenerator {

    private final StringBuilder sb = new StringBuilder();
    private final MariaDBGlobalState globalState;

    public MariaDBDeleteGenerator(MariaDBGlobalState globalState) {
        this.globalState = globalState;
    }

    public static SQLQueryAdapter delete(MariaDBGlobalState globalState) {
        return new MariaDBDeleteGenerator(globalState).generate();
    }

    private SQLQueryAdapter generate() {
        MariaDBTable randomTable = globalState.getSchema().getRandomTable();
        MariaDBExpressionGenerator gen = new MariaDBExpressionGenerator(globalState).setColumns(randomTable.getColumns());
        ExpectedErrors errors = new ExpectedErrors();
        sb.append("DELETE");
        if (Randomly.getBoolean()) {
            sb.append(" LOW_PRIORITY");
        }
        if (Randomly.getBoolean()) {
            sb.append(" QUICK");
        }
        if (Randomly.getBoolean()) {
            sb.append(" IGNORE");
        }
        // TODO: support partitions
        sb.append(" FROM ");
        sb.append(randomTable.getName());
        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            sb.append(MariaDBVisitor.asString(gen.generateExpression()));
            MariaDBErrors.addExpressionErrors(errors);
        }
        errors.addAll(Arrays.asList("doesn't have this option",
                "Truncated incorrect DOUBLE value" /*
                                                    * ignore as a workaround for https://bugs.mariadb.com/bug.php?id=95997
                                                    */, "Truncated incorrect INTEGER value",
                "Truncated incorrect DECIMAL value", "Data truncated for functional index"));
        // TODO: support ORDER BY
        return new SQLQueryAdapter(sb.toString(), errors);
    }

}
