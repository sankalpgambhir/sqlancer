package sqlancer.sqlite3.oracle.tlp;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.sqlite3.SQLite3GlobalState;
import sqlancer.sqlite3.SQLite3Visitor;
import sqlancer.sqlite3.ast.SQLite3Select.SelectType;

public class SQLite3TLPGroupByDistinctOracle extends SQLite3TLPBase {

    public SQLite3TLPGroupByDistinctOracle(SQLite3GlobalState state) {
        super(state);
    }

    @Override
    public void check() throws SQLException {
        super.check();

        // discard possibly conflicting values
        select.setSelectType(SelectType.ALL);
        select.setGroupByClause(Collections.emptyList());
        select.setWhereClause(predicate);

        // group by -- distinct testing
            // select x from y --> select distinct x from y
            //                 --> select x from y group by x


        select.setSelectType(SelectType.DISTINCT);
        if (Randomly.getBooleanWithRatherLowProbability()) {
            // with low probability, add them both in
            select.setGroupByClause(select.getFetchColumns());
        }
        // else group by nothing
        String firstQueryString = SQLite3Visitor.asString(select);

        select.setGroupByClause(select.getFetchColumns());
        if (Randomly.getBooleanWithRatherLowProbability()) {
            // with low probability, add them both in
            select.setSelectType(SelectType.DISTINCT);
        }
        else {
            select.setSelectType(SelectType.ALL);
        }
        String secondQueryString = SQLite3Visitor.asString(select);

        List<String> combinedString = new ArrayList<>();
        combinedString.add(secondQueryString);

        List<String> distinctResultSet = ComparatorHelper.getResultSetFirstColumnAsString(firstQueryString, errors, state);
        List<String> groupbyResultSet = ComparatorHelper.getResultSetFirstColumnAsString(secondQueryString, errors, state);

        ComparatorHelper.assumeResultSetsAreEqual(distinctResultSet, groupbyResultSet, firstQueryString, combinedString,
                state);
    }

}
