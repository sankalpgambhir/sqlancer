package sqlancer.mariadb.gen;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.mariadb.MariaDBGlobalState;
import sqlancer.mariadb.MariaDBSchema.MariaDBColumn;
import sqlancer.mariadb.MariaDBSchema.MariaDBTable;
import sqlancer.mariadb.MariaDBVisitor;

public class MariaDBInsertGenerator {

    private final MariaDBTable table;
    private final StringBuilder sb = new StringBuilder();
    private final ExpectedErrors errors = new ExpectedErrors();
    private final MariaDBGlobalState globalState;

    public MariaDBInsertGenerator(MariaDBGlobalState globalState) {
        this.globalState = globalState;
        table = globalState.getSchema().getRandomTable();
    }

    public static SQLQueryAdapter insertRow(MariaDBGlobalState globalState) throws SQLException {
        if (Randomly.getBoolean()) {
            return new MariaDBInsertGenerator(globalState).generateInsert();
        } else {
            return new MariaDBInsertGenerator(globalState).generateReplace();
        }
    }

    private SQLQueryAdapter generateReplace() {
        sb.append("REPLACE");
        // TODO: Check DELAYED usage nicely
        // if (Randomly.getBoolean()) {
        //     sb.append(" ");
        //     sb.append(Randomly.fromOptions("LOW_PRIORITY", "DELAYED"));
        // }
        return generateInto();

    }

    private SQLQueryAdapter generateInsert() {
        sb.append("INSERT");
        if (Randomly.getBoolean()) {
            sb.append(" ");
            sb.append(Randomly.fromOptions("LOW_PRIORITY", "HIGH_PRIORITY"));
            // sb.append(Randomly.fromOptions("LOW_PRIORITY", "DELAYED", "HIGH_PRIORITY"));
        }
        if (Randomly.getBoolean()) {
            sb.append(" IGNORE");
        }
        return generateInto();
    }

    private SQLQueryAdapter generateInto() {
        sb.append(" INTO ");
        sb.append(table.getName());
        List<MariaDBColumn> columns = table.getRandomNonEmptyColumnSubset();
        sb.append("(");
        sb.append(columns.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
        sb.append(") ");
        sb.append("VALUES");
        MariaDBExpressionGenerator gen = new MariaDBExpressionGenerator(globalState);
        int nrRows;
        if (Randomly.getBoolean()) {
            nrRows = 1;
        } else {
            nrRows = 1 + Randomly.smallNumber();
        }
        for (int row = 0; row < nrRows; row++) {
            if (row != 0) {
                sb.append(", ");
            }
            sb.append("(");
            for (int c = 0; c < columns.size(); c++) {
                if (c != 0) {
                    sb.append(", ");
                }
                sb.append(MariaDBVisitor.asString(gen.generateConstant()));

            }
            sb.append(")");
        }
        errors.add("Out of range value");
        errors.add("Incorrect double value");
        errors.add("doesn't have a default value");
        errors.add("Data truncation");
        errors.add("Incorrect integer value");
        errors.add("Duplicate entry");
        errors.add("Data truncated for functional index");
        errors.add("Data truncated for column");
        errors.add("cannot be null");
        errors.add("Incorrect decimal value");
        return new SQLQueryAdapter(sb.toString(), errors);
    }

}
