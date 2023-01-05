package sqlancer.duckdb.test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.common.ast.newast.NewBetweenOperatorNode;
import sqlancer.common.ast.newast.NewBinaryOperatorNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.common.gen.ExpressionGenerator;
import sqlancer.common.oracle.TernaryLogicPartitioningOracleBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.duckdb.DuckDBErrors;
import sqlancer.duckdb.DuckDBProvider.DuckDBGlobalState;
import sqlancer.duckdb.DuckDBSchema;
import sqlancer.duckdb.DuckDBSchema.DuckDBColumn;
import sqlancer.duckdb.DuckDBSchema.DuckDBTable;
import sqlancer.duckdb.DuckDBSchema.DuckDBTables;
import sqlancer.duckdb.ast.DuckDBExpression;
import sqlancer.duckdb.ast.DuckDBJoin;
import sqlancer.duckdb.ast.DuckDBSelect;
import sqlancer.duckdb.gen.DuckDBExpressionGenerator;
import sqlancer.duckdb.gen.DuckDBExpressionGenerator.DuckDBBinaryComparisonOperator;
import sqlancer.duckdb.gen.DuckDBExpressionGenerator.DuckDBBinaryLogicalOperator;

public class DuckDBQueryPartitioningBase
        extends TernaryLogicPartitioningOracleBase<Node<DuckDBExpression>, DuckDBGlobalState> implements TestOracle {

    DuckDBSchema s;
    DuckDBTables targetTables;
    DuckDBExpressionGenerator gen;
    DuckDBSelect select;
    Node<DuckDBExpression> splitPredicate;
    Node<DuckDBExpression> leftPredicate;
    Node<DuckDBExpression> rightPredicate;

    public DuckDBQueryPartitioningBase(DuckDBGlobalState state) {
        super(state);
        DuckDBErrors.addExpressionErrors(errors);
    }

    public static String canonicalizeResultValue(String value) {
        // Rule: -0.0 should be canonicalized to 0.0
        if (Objects.equals(value, "-0.0")) {
            return "0.0";
        }

        return value;
    }

    @Override
    public void check() throws SQLException {
        s = state.getSchema();
        targetTables = s.getRandomTableNonEmptyTables();
        gen = new DuckDBExpressionGenerator(state).setColumns(targetTables.getColumns());
        initializeTernaryPredicateVariants();
        select = new DuckDBSelect();
        select.setFetchColumns(generateFetchColumns());
        List<DuckDBTable> tables = targetTables.getTables();
        List<TableReferenceNode<DuckDBExpression, DuckDBTable>> tableList = tables.stream()
                .map(t -> new TableReferenceNode<DuckDBExpression, DuckDBTable>(t)).collect(Collectors.toList());
        List<Node<DuckDBExpression>> joins = DuckDBJoin.getJoins(tableList, state);
        select.setJoinList(joins.stream().collect(Collectors.toList()));
        select.setFromList(tableList.stream().collect(Collectors.toList()));
        select.setWhereClause(null);
    }

    List<Node<DuckDBExpression>> generateFetchColumns() {
        List<Node<DuckDBExpression>> columns = new ArrayList<>();
        if (Randomly.getBoolean()) {
            columns.add(new ColumnReferenceNode<>(new DuckDBColumn("*", null, false, false)));
        } else {
            columns = Randomly.nonEmptySubset(targetTables.getColumns()).stream()
                    .map(c -> new ColumnReferenceNode<DuckDBExpression, DuckDBColumn>(c)).collect(Collectors.toList());
        }
        return columns;
    }

    @Override
    protected ExpressionGenerator<Node<DuckDBExpression>> getGen() {
        return gen;
    }

    public Node<DuckDBExpression> genSplitPredicate(Node<DuckDBExpression> pred){
        if(pred instanceof NewBetweenOperatorNode){
            if(Randomly.getBoolean()){
                // split
                NewBetweenOperatorNode<DuckDBExpression> betweenPred = (NewBetweenOperatorNode<DuckDBExpression>) pred;
                
                return new NewBinaryOperatorNode<DuckDBExpression>(
                    new NewBinaryOperatorNode<>(
                        betweenPred.getLeft(), 
                        betweenPred.getMiddle(),
                        betweenPred.isTrue() ? DuckDBBinaryComparisonOperator.GREATER_EQUALS : DuckDBBinaryComparisonOperator.SMALLER), 
                    new NewBinaryOperatorNode<>(
                        betweenPred.getLeft(), 
                        betweenPred.getRight(), 
                        betweenPred.isTrue() ? DuckDBBinaryComparisonOperator.SMALLER_EQUALS : DuckDBBinaryComparisonOperator.GREATER),  
                    betweenPred.isTrue() ? DuckDBBinaryLogicalOperator.AND : DuckDBBinaryLogicalOperator.OR);
            }
            else{
                // don't split
                return pred;
            }
        }
        else{
            return pred;
        }
    }

    public Node<DuckDBExpression> genBetweenDiscardedPredicate(Node<DuckDBExpression> pred, Boolean right){
        if(pred instanceof NewBetweenOperatorNode){
            if(Randomly.getBoolean()){
                // split
                NewBetweenOperatorNode<DuckDBExpression> betweenPred = (NewBetweenOperatorNode<DuckDBExpression>) pred;

                if(betweenPred.isTrue()) {
                    return new NewBinaryOperatorNode<DuckDBExpression>(
                    betweenPred.getLeft(),
                    right ? betweenPred.getRight() : betweenPred.getMiddle(), 
                    right ? DuckDBBinaryComparisonOperator.SMALLER_EQUALS : DuckDBBinaryComparisonOperator.GREATER_EQUALS);
                }
                else{
                    return pred;
                }
            }
        }

        // recurse
        if(pred instanceof NewBinaryOperatorNode){
            if(Randomly.getBoolean()){
                // split
                NewBinaryOperatorNode<DuckDBExpression> binaryPred = (NewBinaryOperatorNode<DuckDBExpression>) pred;

                return new NewBinaryOperatorNode<>(
                    genBetweenDiscardedPredicate(binaryPred.getLeft(), right), 
                    genBetweenDiscardedPredicate(binaryPred.getRight(), right), 
                    binaryPred.getOp());
            }
        }

        // else
        return pred;
    }

    public Node<DuckDBExpression> genSplitComparisonPredicate(Node<DuckDBExpression> pred){
        if(pred instanceof NewBinaryOperatorNode){
            NewBinaryOperatorNode<DuckDBExpression> binaryPred = (NewBinaryOperatorNode<DuckDBExpression>) pred;
            
            if(
                binaryPred.getOp() != DuckDBBinaryComparisonOperator.GREATER_EQUALS ||
                binaryPred.getOp() != DuckDBBinaryComparisonOperator.SMALLER_EQUALS
            ){
                return new NewBinaryOperatorNode<DuckDBExpression>(
                genSplitComparisonPredicate(binaryPred.getLeft()),
                genSplitComparisonPredicate(binaryPred.getRight()), 
                binaryPred.getOp());
            }
            else{
                // rewrite
                Node<DuckDBExpression> newLeft = genSplitComparisonPredicate(binaryPred.getLeft());
                Node<DuckDBExpression> newRight = genSplitComparisonPredicate(binaryPred.getRight());

                // l <= r --> l < r OR l == r
                // l >= r --> l > r OR l == r
                return new NewBinaryOperatorNode<DuckDBExpression>(
                new NewBinaryOperatorNode<>(newLeft, newRight, binaryPred.getOp() == DuckDBBinaryComparisonOperator.GREATER_EQUALS ? DuckDBBinaryComparisonOperator.GREATER : DuckDBBinaryComparisonOperator.SMALLER),
                new NewBinaryOperatorNode<>(newLeft, newRight, DuckDBBinaryComparisonOperator.EQUALS),
                DuckDBBinaryLogicalOperator.OR);
            }
        }
        else if(pred instanceof NewBetweenOperatorNode){
            NewBetweenOperatorNode<DuckDBExpression> betweenPred = (NewBetweenOperatorNode<DuckDBExpression>) pred;
            return new NewBetweenOperatorNode<DuckDBExpression>(
            genSplitComparisonPredicate(betweenPred.getLeft()),
            genSplitComparisonPredicate(betweenPred.getMiddle()), 
            genSplitComparisonPredicate(betweenPred.getRight()), 
            betweenPred.isTrue());
        }

        // else, unimplemented recursion
        return pred;
    }

}
