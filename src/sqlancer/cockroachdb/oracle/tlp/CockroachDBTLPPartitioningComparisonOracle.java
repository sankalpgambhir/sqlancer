package sqlancer.cockroachdb.oracle.tlp;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import sqlancer.cockroachdb.CockroachDBVisitor;
import sqlancer.cockroachdb.ast.CockroachDBExpression;

public class CockroachDBTLPPartitioningComparisonOracle extends CockroachDBTLPBase {

    public CockroachDBTLPPartitioningComparisonOracle(CockroachDBGlobalState state) {
        super(state);
        errors.add("GROUP BY term out of range");
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
        String firstQueryString = CockroachDBVisitor.asString(select);
        List <String> firstResultSet = ComparatorHelper.getResultSetFirstColumnAsString(firstQueryString, errors, state);

        CockroachDBExpression splitPredicate = genSplitComparisonPredicate(predicate);
        if(splitPredicate == predicate){
            return;
        }

        select.setWhereClause(splitPredicate);
        String secondQueryString = CockroachDBVisitor.asString(select);
        List <String> secondResultSet = ComparatorHelper.getResultSetFirstColumnAsString(secondQueryString, errors, state);

        List<String> combinedStringBool = new ArrayList<>();
        combinedStringBool.add(secondQueryString);
        
        // compare
        ComparatorHelper.assumeResultSetsAreEqual(firstResultSet, secondResultSet, firstQueryString, combinedStringBool,
        state);
    }

}
