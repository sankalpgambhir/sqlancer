package sqlancer.duckdb.test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.duckdb.DuckDBErrors;
import sqlancer.duckdb.DuckDBProvider.DuckDBGlobalState;
import sqlancer.duckdb.DuckDBToStringVisitor;

public class DuckDBQueryPartitioningBetweenAndTester extends DuckDBQueryPartitioningBase {

    public DuckDBQueryPartitioningBetweenAndTester(DuckDBGlobalState state) {
        super(state);
        DuckDBErrors.addGroupByErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        // discard old where clause
        select.setWhereClause(null);

        // between partitioning
            // x BETWEEN y AND z ==> x >= y AND x <= z 

        // original
        select.setWhereClause(predicate);
        String firstQueryString = DuckDBToStringVisitor.asString(select);
        List <String> firstResultSet = ComparatorHelper.getResultSetFirstColumnAsString(firstQueryString, errors, state);

        splitPredicate = genSplitPredicate(predicate);

        select.setWhereClause(splitPredicate);
        String secondQueryString = DuckDBToStringVisitor.asString(select);
        List <String> secondResultSet = ComparatorHelper.getResultSetFirstColumnAsString(secondQueryString, errors, state);
        List<String> combinedString = new ArrayList<>();
        combinedString.add(secondQueryString);
        
        // compare
        ComparatorHelper.assumeResultSetsAreEqual(firstResultSet, secondResultSet, firstQueryString, combinedString,
                state, DuckDBQueryPartitioningBase::canonicalizeResultValue);
    }

}
