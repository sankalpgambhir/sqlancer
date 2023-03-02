package sqlancer.mariadb.oracle;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.oracle.PivotedQuerySynthesisBase;
import sqlancer.common.query.Query;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.mariadb.MariaDBErrors;
import sqlancer.mariadb.MariaDBGlobalState;
import sqlancer.mariadb.MariaDBSchema.MariaDBColumn;
import sqlancer.mariadb.MariaDBSchema.MariaDBRowValue;
import sqlancer.mariadb.MariaDBSchema.MariaDBTable;
import sqlancer.mariadb.MariaDBSchema.MariaDBTables;
import sqlancer.mariadb.MariaDBVisitor;
import sqlancer.mariadb.ast.MariaDBColumnReference;
import sqlancer.mariadb.ast.MariaDBConstant;
import sqlancer.mariadb.ast.MariaDBExpression;
import sqlancer.mariadb.ast.MariaDBSelect;
import sqlancer.mariadb.ast.MariaDBTableReference;
import sqlancer.mariadb.ast.MariaDBUnaryPostfixOperation;
import sqlancer.mariadb.ast.MariaDBUnaryPostfixOperation.UnaryPostfixOperator;
import sqlancer.mariadb.ast.MariaDBUnaryPrefixOperation;
import sqlancer.mariadb.ast.MariaDBUnaryPrefixOperation.MariaDBUnaryPrefixOperator;
import sqlancer.mariadb.gen.MariaDBExpressionGenerator;

public class MariaDBPivotedQuerySynthesisOracle
        extends PivotedQuerySynthesisBase<MariaDBGlobalState, MariaDBRowValue, MariaDBExpression, SQLConnection> {

    private List<MariaDBExpression> fetchColumns;
    private List<MariaDBColumn> columns;

    public MariaDBPivotedQuerySynthesisOracle(MariaDBGlobalState globalState) throws SQLException {
        super(globalState);
        MariaDBErrors.addExpressionErrors(errors);
        errors.add("in 'order clause'"); // e.g., Unknown column '2067708013' in 'order clause'
    }

    @Override
    public Query<SQLConnection> getRectifiedQuery() throws SQLException {
        MariaDBTables randomFromTables = globalState.getSchema().getRandomTableNonEmptyTables();
        List<MariaDBTable> tables = randomFromTables.getTables();

        MariaDBSelect selectStatement = new MariaDBSelect();
        selectStatement.setSelectType(Randomly.fromOptions(MariaDBSelect.SelectType.values()));
        columns = randomFromTables.getColumns();
        pivotRow = randomFromTables.getRandomRowValue(globalState.getConnection());

        selectStatement.setFromList(tables.stream().map(t -> new MariaDBTableReference(t)).collect(Collectors.toList()));

        fetchColumns = columns.stream().map(c -> new MariaDBColumnReference(c, null)).collect(Collectors.toList());
        selectStatement.setFetchColumns(fetchColumns);
        MariaDBExpression whereClause = generateRectifiedExpression(columns, pivotRow);
        selectStatement.setWhereClause(whereClause);
        List<MariaDBExpression> groupByClause = generateGroupByClause(columns, pivotRow);
        selectStatement.setGroupByExpressions(groupByClause);
        MariaDBExpression limitClause = generateLimit();
        selectStatement.setLimitClause(limitClause);
        if (limitClause != null) {
            MariaDBExpression offsetClause = generateOffset();
            selectStatement.setOffsetClause(offsetClause);
        }
        List<String> modifiers = Randomly.subset("STRAIGHT_JOIN", "SQL_SMALL_RESULT", "SQL_BIG_RESULT", "SQL_NO_CACHE");
        selectStatement.setModifiers(modifiers);
        List<MariaDBExpression> orderBy = new MariaDBExpressionGenerator(globalState).setColumns(columns)
                .generateOrderBys();
        selectStatement.setOrderByExpressions(orderBy);

        return new SQLQueryAdapter(MariaDBVisitor.asString(selectStatement), errors);
    }

    private List<MariaDBExpression> generateGroupByClause(List<MariaDBColumn> columns, MariaDBRowValue rw) {
        if (Randomly.getBoolean()) {
            return columns.stream().map(c -> MariaDBColumnReference.create(c, rw.getValues().get(c)))
                    .collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }

    private MariaDBConstant generateLimit() {
        if (Randomly.getBoolean()) {
            return MariaDBConstant.createIntConstant(Integer.MAX_VALUE);
        } else {
            return null;
        }
    }

    private MariaDBExpression generateOffset() {
        if (Randomly.getBoolean()) {
            return MariaDBConstant.createIntConstantNotAsBoolean(0);
        } else {
            return null;
        }
    }

    private MariaDBExpression generateRectifiedExpression(List<MariaDBColumn> columns, MariaDBRowValue rw) {
        MariaDBExpression expression = new MariaDBExpressionGenerator(globalState).setRowVal(rw).setColumns(columns)
                .generateExpression();
        MariaDBConstant expectedValue = expression.getExpectedValue();
        MariaDBExpression result;
        if (expectedValue.isNull()) {
            result = new MariaDBUnaryPostfixOperation(expression, UnaryPostfixOperator.IS_NULL, false);
        } else if (expectedValue.asBooleanNotNull()) {
            result = expression;
        } else {
            result = new MariaDBUnaryPrefixOperation(expression, MariaDBUnaryPrefixOperator.NOT);
        }
        rectifiedPredicates.add(result);
        return result;
    }

    @Override
    protected Query<SQLConnection> getContainmentCheckQuery(Query<?> query) throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT * FROM ("); // ANOTHER SELECT TO USE ORDER BY without restrictions
        sb.append(query.getUnterminatedQueryString());
        sb.append(") as result WHERE ");
        int i = 0;
        for (MariaDBColumn c : columns) {
            if (i++ != 0) {
                sb.append(" AND ");
            }
            sb.append("result.");
            sb.append("ref");
            sb.append(i - 1);
            if (pivotRow.getValues().get(c).isNull()) {
                sb.append(" IS NULL");
            } else {
                sb.append(" = ");
                sb.append(pivotRow.getValues().get(c).getTextRepresentation());
            }
        }

        String resultingQueryString = sb.toString();
        return new SQLQueryAdapter(resultingQueryString, query.getExpectedErrors());
    }

    @Override
    protected String getExpectedValues(MariaDBExpression expr) {
        return MariaDBVisitor.asExpectedValues(expr);
    }
}
