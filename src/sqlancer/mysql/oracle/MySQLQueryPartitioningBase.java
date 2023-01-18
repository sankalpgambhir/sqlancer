package sqlancer.mysql.oracle;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.gen.ExpressionGenerator;
import sqlancer.common.oracle.TernaryLogicPartitioningOracleBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.mysql.MySQLErrors;
import sqlancer.mysql.MySQLGlobalState;
import sqlancer.mysql.MySQLSchema;
import sqlancer.mysql.MySQLSchema.MySQLTable;
import sqlancer.mysql.MySQLSchema.MySQLTables;
import sqlancer.mysql.ast.MySQLBetweenOperation;
import sqlancer.mysql.ast.MySQLBinaryComparisonOperation;
import sqlancer.mysql.ast.MySQLBinaryLogicalOperation;
import sqlancer.mysql.ast.MySQLColumnReference;
import sqlancer.mysql.ast.MySQLExpression;
import sqlancer.mysql.ast.MySQLSelect;
import sqlancer.mysql.ast.MySQLTableReference;
import sqlancer.mysql.ast.MySQLBinaryComparisonOperation.BinaryComparisonOperator;
import sqlancer.mysql.ast.MySQLBinaryLogicalOperation.MySQLBinaryLogicalOperator;
import sqlancer.mysql.gen.MySQLExpressionGenerator;

public abstract class MySQLQueryPartitioningBase
        extends TernaryLogicPartitioningOracleBase<MySQLExpression, MySQLGlobalState> implements TestOracle {

    MySQLSchema s;
    MySQLTables targetTables;
    MySQLExpressionGenerator gen;
    MySQLSelect select;

    public MySQLQueryPartitioningBase(MySQLGlobalState state) {
        super(state);
        MySQLErrors.addExpressionErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        s = state.getSchema();
        targetTables = s.getRandomTableNonEmptyTables();
        gen = new MySQLExpressionGenerator(state).setColumns(targetTables.getColumns());
        initializeTernaryPredicateVariants();
        select = new MySQLSelect();
        select.setFetchColumns(generateFetchColumns());
        List<MySQLTable> tables = targetTables.getTables();
        List<MySQLExpression> tableList = tables.stream().map(t -> new MySQLTableReference(t))
                .collect(Collectors.toList());
        // List<MySQLExpression> joins = MySQLJoin.getJoins(tableList, state);
        select.setFromList(tableList);
        select.setWhereClause(null);
        // select.setJoins(joins);
    }

    List<MySQLExpression> generateFetchColumns() {
        return Arrays.asList(MySQLColumnReference.create(targetTables.getColumns().get(0), null));
    }

    @Override
    protected ExpressionGenerator<MySQLExpression> getGen() {
        return gen;
    }

    public MySQLExpression genDiscardedBetweenPredicate(MySQLExpression pred, Boolean left){
        if (pred instanceof MySQLBetweenOperation) {
            MySQLBetweenOperation betweenPred = (MySQLBetweenOperation) pred;

            if (!Randomly.getBooleanWithSmallProbability()) {
            // TODO: implement partitioning for nested negated betweens?

            return new MySQLBinaryComparisonOperation(
                    left ? betweenPred.getLeft() : betweenPred.getRight(), 
                    betweenPred.getExpr(), 
                    left ? BinaryComparisonOperator.LESS_EQUALS : BinaryComparisonOperator.GREATER_EQUALS
                    );
            }
        }
        else {
            // recurse
            if (pred instanceof MySQLBinaryLogicalOperation) {
                MySQLBinaryLogicalOperation binOp = (MySQLBinaryLogicalOperation) pred;

                return new MySQLBinaryLogicalOperation(
                    genDiscardedBetweenPredicate(binOp.getLeft(), left), 
                    genDiscardedBetweenPredicate(binOp.getRight(), left), 
                    binOp.getOp()
                );
            }
        }

        return pred;
    }

    public MySQLExpression genSplitBetweenPredicate(MySQLExpression pred){
        if (pred instanceof MySQLBetweenOperation) {
            MySQLBetweenOperation betweenPred = (MySQLBetweenOperation) pred;

            return new MySQLBinaryLogicalOperation(
                new MySQLBinaryComparisonOperation(
                    betweenPred.getLeft(), 
                    betweenPred.getExpr(), 
                    MySQLBinaryComparisonOperation.BinaryComparisonOperator.LESS_EQUALS
                    ),
                new MySQLBinaryComparisonOperation(
                    betweenPred.getRight(), 
                    betweenPred.getExpr(), 
                    MySQLBinaryComparisonOperation.BinaryComparisonOperator.GREATER_EQUALS
                    ),
                    MySQLBinaryLogicalOperator.AND
            );
        }
        else {
            return pred;
        }
    }

}
