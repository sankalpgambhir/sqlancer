package sqlancer.cockroachdb.oracle.tlp;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.cockroachdb.CockroachDBErrors;
import sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import sqlancer.cockroachdb.CockroachDBSchema;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBColumn;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBTable;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBTables;
import sqlancer.cockroachdb.ast.CockroachDBBetweenOperation;
import sqlancer.cockroachdb.ast.CockroachDBBinaryComparisonOperator;
import sqlancer.cockroachdb.ast.CockroachDBBinaryLogicalOperation;
import sqlancer.cockroachdb.ast.CockroachDBColumnReference;
import sqlancer.cockroachdb.ast.CockroachDBExpression;
import sqlancer.cockroachdb.ast.CockroachDBNotOperation;
import sqlancer.cockroachdb.ast.CockroachDBSelect;
import sqlancer.cockroachdb.ast.CockroachDBTableReference;
import sqlancer.cockroachdb.ast.CockroachDBBinaryComparisonOperator.CockroachDBComparisonOperator;
import sqlancer.cockroachdb.ast.CockroachDBBinaryLogicalOperation.CockroachDBBinaryLogicalOperator;
import sqlancer.cockroachdb.gen.CockroachDBExpressionGenerator;
import sqlancer.cockroachdb.oracle.CockroachDBNoRECOracle;
import sqlancer.common.gen.ExpressionGenerator;
import sqlancer.common.oracle.TernaryLogicPartitioningOracleBase;
import sqlancer.common.oracle.TestOracle;

public class CockroachDBTLPBase extends
        TernaryLogicPartitioningOracleBase<CockroachDBExpression, CockroachDBGlobalState> implements TestOracle {

    CockroachDBSchema s;
    CockroachDBTables targetTables;
    CockroachDBExpressionGenerator gen;
    CockroachDBSelect select;

    public CockroachDBTLPBase(CockroachDBGlobalState state) {
        super(state);
        CockroachDBErrors.addExpressionErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        s = state.getSchema();
        targetTables = s.getRandomTableNonEmptyTables();
        gen = new CockroachDBExpressionGenerator(state).setColumns(targetTables.getColumns());
        initializeTernaryPredicateVariants();
        select = new CockroachDBSelect();
        select.setFetchColumns(generateFetchColumns());
        List<CockroachDBTable> tables = targetTables.getTables();
        List<CockroachDBExpression> tableList = tables.stream().map(t -> new CockroachDBTableReference(t))
                .collect(Collectors.toList());
        List<CockroachDBExpression> joins = CockroachDBNoRECOracle.getJoins(tableList, state);
        select.setJoinList(joins);
        select.setFromList(tableList);
        select.setWhereClause(null);
    }

    List<CockroachDBExpression> generateFetchColumns() {
        List<CockroachDBExpression> columns = new ArrayList<>();
        if (Randomly.getBoolean() || targetTables.getColumns().size() == 0) {
            columns.add(new CockroachDBColumnReference(new CockroachDBColumn("*", null, false, false)));
        } else {
            columns.addAll(Randomly.nonEmptySubset(targetTables.getColumns()).stream()
                    .map(c -> new CockroachDBColumnReference(c)).collect(Collectors.toList()));
        }
        return columns;
    }

    @Override
    protected ExpressionGenerator<CockroachDBExpression> getGen() {
        return gen;
    }



    public CockroachDBExpression genSplitPredicate(CockroachDBExpression pred){
        if(pred instanceof CockroachDBBetweenOperation){
            if(Randomly.getBoolean()){
                // split
                CockroachDBBetweenOperation betweenPred = (CockroachDBBetweenOperation) pred;
                
                switch (betweenPred.getType()) {
                    case BETWEEN:
                        /**
                         * b between a and c -->
                         *      a <= b and b <= c
                         */
                        return new CockroachDBBinaryLogicalOperation(
                            new CockroachDBBinaryComparisonOperator(betweenPred.getLeft(), betweenPred.getExpr(), CockroachDBComparisonOperator.SMALLER_EQUALS), 
                            new CockroachDBBinaryComparisonOperator(betweenPred.getExpr(), betweenPred.getRight(), CockroachDBComparisonOperator.SMALLER_EQUALS), 
                            CockroachDBBinaryLogicalOperator.AND);
                    case NOT_BETWEEN:
                        /**
                         * b not between a and c -->
                         *      not (a <= b and b <= c)
                         *      a > b or b > c
                         * with equal probability
                         */
                        if(Randomly.getBoolean()){
                            return new CockroachDBNotOperation(
                                new CockroachDBBinaryLogicalOperation(
                                    new CockroachDBBinaryComparisonOperator(betweenPred.getLeft(), betweenPred.getExpr(), CockroachDBComparisonOperator.SMALLER_EQUALS), 
                                    new CockroachDBBinaryComparisonOperator(betweenPred.getExpr(), betweenPred.getRight(), CockroachDBComparisonOperator.SMALLER_EQUALS), 
                                    CockroachDBBinaryLogicalOperator.AND)
                                );
                        }
                        else{
                            return new CockroachDBBinaryLogicalOperation(
                            new CockroachDBBinaryComparisonOperator(betweenPred.getLeft(), betweenPred.getExpr(), CockroachDBComparisonOperator.GREATER), 
                            new CockroachDBBinaryComparisonOperator(betweenPred.getExpr(), betweenPred.getRight(), CockroachDBComparisonOperator.GREATER), 
                            CockroachDBBinaryLogicalOperator.OR);
                        }
                    case BETWEEN_SYMMETRIC:
                        /**
                         * b between symmetric a and c -->
                         *      (a <= b and b <= c) or (c <= b and b <= a)
                         */
                        return new CockroachDBBinaryLogicalOperation(
                            new CockroachDBBinaryLogicalOperation(
                                    new CockroachDBBinaryComparisonOperator(betweenPred.getLeft(), betweenPred.getExpr(), CockroachDBComparisonOperator.SMALLER_EQUALS), 
                                    new CockroachDBBinaryComparisonOperator(betweenPred.getExpr(), betweenPred.getRight(), CockroachDBComparisonOperator.SMALLER_EQUALS), 
                                    CockroachDBBinaryLogicalOperator.AND), 
                                new CockroachDBBinaryLogicalOperation(
                                    new CockroachDBBinaryComparisonOperator(betweenPred.getRight(), betweenPred.getExpr(), CockroachDBComparisonOperator.SMALLER_EQUALS), 
                                    new CockroachDBBinaryComparisonOperator(betweenPred.getExpr(), betweenPred.getLeft(), CockroachDBComparisonOperator.SMALLER_EQUALS), 
                                    CockroachDBBinaryLogicalOperator.AND),
                            CockroachDBBinaryLogicalOperator.OR);
                    case NOT_BETWEEN_SYMMETRIC:
                        /**
                         * b not between symmetric a and c -->
                         *      not ((a <= b and b <= c) or (c <= b and b <= a))
                         *      (b < a and b < c) or (b > a and b > c)
                         * with equal probability
                         */
                        if(Randomly.getBoolean()){
                            return new CockroachDBNotOperation(
                                new CockroachDBBinaryLogicalOperation(
                                    new CockroachDBBinaryLogicalOperation(
                                            new CockroachDBBinaryComparisonOperator(betweenPred.getLeft(), betweenPred.getExpr(), CockroachDBComparisonOperator.SMALLER_EQUALS), 
                                            new CockroachDBBinaryComparisonOperator(betweenPred.getExpr(), betweenPred.getRight(), CockroachDBComparisonOperator.SMALLER_EQUALS), 
                                            CockroachDBBinaryLogicalOperator.AND), 
                                        new CockroachDBBinaryLogicalOperation(
                                            new CockroachDBBinaryComparisonOperator(betweenPred.getRight(), betweenPred.getExpr(), CockroachDBComparisonOperator.SMALLER_EQUALS), 
                                            new CockroachDBBinaryComparisonOperator(betweenPred.getExpr(), betweenPred.getLeft(), CockroachDBComparisonOperator.SMALLER_EQUALS), 
                                            CockroachDBBinaryLogicalOperator.AND),
                                    CockroachDBBinaryLogicalOperator.OR)
                                );
                        }
                        else{
                            return new CockroachDBBinaryLogicalOperation(
                                new CockroachDBBinaryLogicalOperation(
                                    new CockroachDBBinaryComparisonOperator(betweenPred.getLeft(), betweenPred.getExpr(), CockroachDBComparisonOperator.SMALLER), 
                                    new CockroachDBBinaryComparisonOperator(betweenPred.getRight(), betweenPred.getExpr(), CockroachDBComparisonOperator.SMALLER), 
                                    CockroachDBBinaryLogicalOperator.AND), 
                                new CockroachDBBinaryLogicalOperation(
                                    new CockroachDBBinaryComparisonOperator(betweenPred.getLeft(), betweenPred.getExpr(), CockroachDBComparisonOperator.GREATER), 
                                    new CockroachDBBinaryComparisonOperator(betweenPred.getRight(), betweenPred.getExpr(), CockroachDBComparisonOperator.GREATER), 
                                    CockroachDBBinaryLogicalOperator.AND), 
                            CockroachDBBinaryLogicalOperator.OR);
                        }
        
                    default:
                        break;
                }
            }
        }

        // TODO: make recurse to find betweens? possibly not much added benefit
        return pred;
    }

    public CockroachDBExpression genBetweenDiscardedPredicate(CockroachDBExpression pred, Boolean right){
        if(pred instanceof CockroachDBBetweenOperation){
            if(Randomly.getBoolean()){
                // split
                CockroachDBBetweenOperation betweenPred = (CockroachDBBetweenOperation) pred;
                
                switch (betweenPred.getType()) {
                    case BETWEEN:
                        /**
                         * b between a and c -->
                         *      a <= b if not right
                         *      b <= c if right
                         * 
                         */
                        return new CockroachDBBinaryComparisonOperator(
                            right ? betweenPred.getLeft() : betweenPred.getExpr(), 
                            right ? betweenPred.getExpr() : betweenPred.getRight(),
                            CockroachDBComparisonOperator.SMALLER_EQUALS);

                    // case NOT_BETWEEN:
                        
                    // case BETWEEN_SYMMETRIC:
                        
                    // case NOT_BETWEEN_SYMMETRIC:
                        
        
                    default:
                        break;
                }
            }
        }

        // recurse
        if(pred instanceof CockroachDBBinaryLogicalOperation){
            if(Randomly.getBoolean()){
                // split
                CockroachDBBinaryLogicalOperation binaryPred = (CockroachDBBinaryLogicalOperation) pred;

                return new CockroachDBBinaryLogicalOperation(
                    genBetweenDiscardedPredicate(binaryPred.getLeft(), right), 
                    genBetweenDiscardedPredicate(binaryPred.getRight(), right), 
                    binaryPred.getOp());
            }
        }

        // else
        return pred;
    }

    public CockroachDBExpression genSplitComparisonPredicate(CockroachDBExpression pred){
        if(pred instanceof CockroachDBBinaryComparisonOperator){
            CockroachDBBinaryComparisonOperator binaryPred = (CockroachDBBinaryComparisonOperator) pred;
            
            if(
                binaryPred.getOp() != CockroachDBComparisonOperator.GREATER_EQUALS ||
                binaryPred.getOp() != CockroachDBComparisonOperator.SMALLER_EQUALS
            ){
                return new CockroachDBBinaryComparisonOperator(
                genSplitComparisonPredicate(binaryPred.getLeft()),
                genSplitComparisonPredicate(binaryPred.getRight()), 
                binaryPred.getOp());
            }
            else{
                // rewrite
                CockroachDBExpression newLeft = genSplitComparisonPredicate(binaryPred.getLeft());
                CockroachDBExpression newRight = genSplitComparisonPredicate(binaryPred.getRight());

                // l <= r --> l < r OR l == r
                // l >= r --> l > r OR l == r
                return new CockroachDBBinaryLogicalOperation(
                new CockroachDBBinaryComparisonOperator(newLeft, newRight, binaryPred.getOp() == CockroachDBComparisonOperator.GREATER_EQUALS ? CockroachDBComparisonOperator.GREATER : CockroachDBComparisonOperator.SMALLER),
                new CockroachDBBinaryComparisonOperator(newLeft, newRight, CockroachDBComparisonOperator.EQUALS),
                CockroachDBBinaryLogicalOperator.OR);
            }
        }

        // else, unimplemented recursion
        return pred;
    }

}
