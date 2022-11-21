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

public class SQLite3TLPBetweenAndOracle extends SQLite3TLPBase {

    public SQLite3TLPBetweenAndOracle(SQLite3GlobalState state) {
        super(state);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        // discard any old information
        select.setWhereClause(null);

        // between partitioning
            // x BETWEEN y AND z ==> x >= y AND x <= z 

        // original
        String originalQueryString = SQLite3Visitor.asString(select);
        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

        // should not affect things, ideally
        boolean orderBy = Randomly.getBooleanWithSmallProbability();
        if (orderBy) {
            select.setOrderByExpressions(gen.generateOrderBys());
        }

        // modify the predicate if possible
        if (predicate instanceof BetweenOperation) {
            SQLite3Expression splitPredicate = ((BetweenOperation) predicate).genSplitBetweenPredicate();
            select.setWhereClause(splitPredicate);
        }

        String splitQueryString = SQLite3Visitor.asString(select);

        List<String> combinedString = new ArrayList<>();
        combinedString.add(splitQueryString);

        List<String> secondResultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state);
    }

}
