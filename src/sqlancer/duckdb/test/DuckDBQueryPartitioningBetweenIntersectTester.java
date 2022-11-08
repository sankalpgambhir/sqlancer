package sqlancer.duckdb.test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.duckdb.DuckDBErrors;
import sqlancer.duckdb.DuckDBProvider.DuckDBGlobalState;
import sqlancer.duckdb.DuckDBToStringVisitor;

public class DuckDBQueryPartitioningBetweenIntersectTester extends DuckDBQueryPartitioningBase {

    public DuckDBQueryPartitioningBetweenIntersectTester(DuckDBGlobalState state) {
        super(state);
        DuckDBErrors.addGroupByErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        // discard old where clause
        select.setWhereClause(null);
        
        // between partitioning
            // check ast to find a between, if not, return
            // if yes, convert
            // x BETWEEN y AND z ==> x >= y INTERSECT x <= z

        // INTERSECT forces a distinct, so the original query needs one as well
        select.setDistinct(true);

        // original
        select.setWhereClause(predicate);
        String originalQueryString = DuckDBToStringVisitor.asString(select);
        List <String> originalResultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

        leftPredicate = genBetweenDiscardedPredicate(predicate, false);
        select.setWhereClause(leftPredicate);
        String firstQueryString = DuckDBToStringVisitor.asString(select);
        
        rightPredicate = genBetweenDiscardedPredicate(predicate, true);
        select.setWhereClause(rightPredicate);
        String secondQueryString = DuckDBToStringVisitor.asString(select);

        // intersect
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = ComparatorHelper.getIntersectedResultSet(firstQueryString,
                secondQueryString, combinedString, state, errors);
        
        // compare
        ComparatorHelper.assumeResultSetsAreEqual(originalResultSet, secondResultSet, originalQueryString, combinedString,
                state, DuckDBQueryPartitioningBase::canonicalizeResultValue);
    }

}
