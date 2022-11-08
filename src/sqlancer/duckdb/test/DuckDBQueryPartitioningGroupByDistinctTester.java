package sqlancer.duckdb.test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.duckdb.DuckDBErrors;
import sqlancer.duckdb.DuckDBProvider.DuckDBGlobalState;
import sqlancer.duckdb.DuckDBSchema.DuckDBColumn;
import sqlancer.duckdb.DuckDBToStringVisitor;
import sqlancer.duckdb.ast.DuckDBExpression;

public class DuckDBQueryPartitioningGroupByDistinctTester extends DuckDBQueryPartitioningBase {

    public DuckDBQueryPartitioningGroupByDistinctTester(DuckDBGlobalState state) {
        super(state);
        DuckDBErrors.addGroupByErrors(errors);
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
        String firstQueryString = DuckDBToStringVisitor.asString(select);

        select.setGroupByExpressions(select.getFetchColumns());
        if (Randomly.getBooleanWithRatherLowProbability()) {
            // with low probability, add them both in
            select.setDistinct(true);
        }
        else {
            select.setDistinct(false);
        }
        String secondQueryString = DuckDBToStringVisitor.asString(select);

        List<String> combinedString = new ArrayList<>();
        combinedString.add(secondQueryString);

        List<String> distinctResultSet = ComparatorHelper.getResultSetFirstColumnAsString(firstQueryString, errors, state);
        List<String> groupbyResultSet = ComparatorHelper.getResultSetFirstColumnAsString(secondQueryString, errors, state);
        ComparatorHelper.assumeResultSetsAreEqual(distinctResultSet, groupbyResultSet, firstQueryString, combinedString,
                state, DuckDBQueryPartitioningBase::canonicalizeResultValue);
    }

    @Override
    List<Node<DuckDBExpression>> generateFetchColumns() {
        return Randomly.nonEmptySubset(targetTables.getColumns()).stream()
                .map(c -> new ColumnReferenceNode<DuckDBExpression, DuckDBColumn>(c)).collect(Collectors.toList());
    }

}
