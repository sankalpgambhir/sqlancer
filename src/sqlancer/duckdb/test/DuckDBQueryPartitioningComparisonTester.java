package sqlancer.duckdb.test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.common.ast.newast.NewBinaryOperatorNode;
import sqlancer.duckdb.DuckDBErrors;
import sqlancer.duckdb.DuckDBProvider.DuckDBGlobalState;
import sqlancer.duckdb.ast.DuckDBExpression;
import sqlancer.duckdb.DuckDBToStringVisitor;

public class DuckDBQueryPartitioningComparisonTester extends DuckDBQueryPartitioningBase {

    public DuckDBQueryPartitioningComparisonTester(DuckDBGlobalState state) {
        super(state);
        DuckDBErrors.addGroupByErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        // discard old where clause
        select.setWhereClause(null);

        // comparison partitioning
            // x >= y ==> x > y OR x = y 
            // x <= y ==> x < y OR x = y 

        // TODO: Make generic to use rewrites instead of manual splitting
        // List <Operator, NewBinaryOperatorNode<DuckDBExpression>>
        // binaryRewrites = new ArrayList<>();

        // original
        select.setWhereClause(predicate);
        String firstQueryString = DuckDBToStringVisitor.asString(select);
        List <String> firstResultSet = ComparatorHelper.getResultSetFirstColumnAsString(firstQueryString, errors, state);

        splitPredicate = genSplitComparisonPredicate(predicate);
        if(splitPredicate == predicate){
            return;
        }

        select.setWhereClause(splitPredicate);
        String secondQueryString = DuckDBToStringVisitor.asString(select);
        List <String> secondResultSet = ComparatorHelper.getResultSetFirstColumnAsString(secondQueryString, errors, state);

        List<String> combinedStringBool = new ArrayList<>();
        combinedStringBool.add(secondQueryString);
        
        // compare
        ComparatorHelper.assumeResultSetsAreEqual(firstResultSet, secondResultSet, firstQueryString, combinedStringBool,
        state, DuckDBQueryPartitioningBase::canonicalizeResultValue);
    }

}
