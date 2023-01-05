package sqlancer.sqlite3.oracle.tlp;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.sqlite3.SQLite3GlobalState;
import sqlancer.sqlite3.SQLite3Visitor;
import sqlancer.sqlite3.ast.SQLite3Expression;
import sqlancer.sqlite3.ast.SQLite3Expression.BetweenOperation;
import sqlancer.sqlite3.ast.SQLite3Select.SelectType;

public class SQLite3TLPBetweenIntersectOracle extends SQLite3TLPBase {

    public SQLite3TLPBetweenIntersectOracle(SQLite3GlobalState state) {
        super(state);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        // discard any old information
        select.setWhereClause(null);
        
        // between partitioning
            // check ast to find a between, if not, return
            // if yes, convert
            // x BETWEEN y AND z ==> x >= y INTERSECT x <= z

        // INTERSECT forces a distinct, so the original query needs one as well
        select.setSelectType(SelectType.DISTINCT);

        // original
        select.setWhereClause(predicate);
        String originalQueryString = SQLite3Visitor.asString(select);
        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

        // should not affect things, ideally
        boolean orderBy = Randomly.getBooleanWithSmallProbability();
        if (orderBy) {
            select.setOrderByExpressions(gen.generateOrderBys());
        }

        String firstQueryString = SQLite3Visitor.asString(select);
        String secondQueryString = SQLite3Visitor.asString(select);

        // modify the predicate if possible
        if (predicate instanceof BetweenOperation) {
            SQLite3Expression leftPredicate = predicate.genDiscardedBetweenPredicate(true);
            select.setWhereClause(leftPredicate);
            firstQueryString = SQLite3Visitor.asString(select);
            
            SQLite3Expression rightPredicate = predicate.genDiscardedBetweenPredicate(false);
            select.setWhereClause(rightPredicate);
            secondQueryString = SQLite3Visitor.asString(select);
        }

        List<String> combinedString = new ArrayList<>();
        
        List<String> secondResultSet = ComparatorHelper.getIntersectedResultSet(firstQueryString, secondQueryString, combinedString, state, errors);

        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state);
    }

}
