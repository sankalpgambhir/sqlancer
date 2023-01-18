package sqlancer.mysql.oracle;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.mysql.MySQLGlobalState;
import sqlancer.mysql.MySQLVisitor;
import sqlancer.mysql.ast.MySQLSelect.SelectType;

public class MySQLTLPGroupByDistinctOracle extends MySQLQueryPartitioningBase {

    public MySQLTLPGroupByDistinctOracle(MySQLGlobalState state) {
        super(state);
    }

    @Override
    public void check() throws SQLException {
        super.check();

        // discard possibly conflicting values
        select.setSelectType(SelectType.ALL);
        select.setGroupByExpressions(Collections.emptyList());
        select.setWhereClause(predicate);

        // group by -- distinct testing
            // select x from y --> select distinct x from y
            //                 --> select x from y group by x


        select.setSelectType(SelectType.DISTINCT);
        if (Randomly.getBooleanWithRatherLowProbability()) {
            // with low probability, add them both in
            select.setGroupByExpressions(select.getFetchColumns());
        }
        // else group by nothing
        String firstQueryString = MySQLVisitor.asString(select);

        select.setGroupByExpressions(select.getFetchColumns());
        if (Randomly.getBooleanWithRatherLowProbability()) {
            // with low probability, add them both in
            select.setSelectType(SelectType.DISTINCT);
        }
        else {
            select.setSelectType(SelectType.ALL);
        }
        String secondQueryString = MySQLVisitor.asString(select);

        List<String> combinedString = new ArrayList<>();
        combinedString.add(secondQueryString);

        List<String> distinctResultSet = ComparatorHelper.getResultSetFirstColumnAsString(firstQueryString, errors, state);
        List<String> groupbyResultSet = ComparatorHelper.getResultSetFirstColumnAsString(secondQueryString, errors, state);

        ComparatorHelper.assumeResultSetsAreEqual(distinctResultSet, groupbyResultSet, firstQueryString, combinedString,
                state);
    }

}
