package sqlancer.cockroachdb.oracle.tlp;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import sqlancer.cockroachdb.CockroachDBVisitor;

public class CockroachDBTLPGroupByDistinctOracle extends CockroachDBTLPBase {

    public CockroachDBTLPGroupByDistinctOracle(CockroachDBGlobalState state) {
        super(state);
        errors.add("GROUP BY term out of range");
    }

    @Override
    public void check() throws SQLException {
        super.check();

        // discard possibly conflicting values
        select.setDistinct(false);
        select.setGroupByExpressions(Collections.emptyList());
        select.setWhereClause(predicate);

        // group by -- distinct testing
            // select x from y --> select distinct x from y
            //                 --> select x from y group by x


        select.setDistinct(true);
        if (Randomly.getBooleanWithRatherLowProbability()) {
            // with low probability, add them both in
            select.setGroupByExpressions(Collections.emptyList());
        }
        // else group by nothing
        String firstQueryString = CockroachDBVisitor.asString(select);

        select.setGroupByExpressions(select.getFetchColumns());
        if (Randomly.getBooleanWithRatherLowProbability()) {
            // with low probability, add them both in
            select.setDistinct(true);
        }
        else {
            select.setDistinct(false);
        }
        String secondQueryString = CockroachDBVisitor.asString(select);

        List<String> combinedString = new ArrayList<>();
        combinedString.add(secondQueryString);

        List<String> distinctResultSet = ComparatorHelper.getResultSetFirstColumnAsString(firstQueryString, errors, state);
        List<String> groupbyResultSet = ComparatorHelper.getResultSetFirstColumnAsString(secondQueryString, errors, state);
        ComparatorHelper.assumeResultSetsAreEqual(distinctResultSet, groupbyResultSet, firstQueryString, combinedString,
                state);
    }

    // @Override
    // List<CockroachDBExpression> generateFetchColumns() {
    //     return Randomly.nonEmptySubset(targetTables.getColumns()).stream()
    //             .map(c -> new ColumnReferenceNode<DuckDBExpression, DuckDBColumn>(c)).collect(Collectors.toList());
    // }

}
