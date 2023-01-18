package sqlancer.mysql.oracle;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.mysql.MySQLGlobalState;
import sqlancer.mysql.MySQLVisitor;
import sqlancer.mysql.ast.MySQLBetweenOperation;
import sqlancer.mysql.ast.MySQLExpression;

public class MySQLTLPBetweenAndOracle extends MySQLQueryPartitioningBase {

    public MySQLTLPBetweenAndOracle(MySQLGlobalState state) {
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
        String originalQueryString = MySQLVisitor.asString(select);
        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

        // should not affect things, ideally
        boolean orderBy = Randomly.getBooleanWithSmallProbability();
        if (orderBy) {
            select.setOrderByExpressions(gen.generateOrderBys());
        }

        // modify the predicate if possible
        if (predicate instanceof MySQLBetweenOperation) {
            MySQLExpression splitPredicate = genSplitBetweenPredicate(predicate);
            select.setWhereClause(splitPredicate);
        }

        String splitQueryString = MySQLVisitor.asString(select);

        List<String> combinedString = new ArrayList<>();
        combinedString.add(splitQueryString);

        List<String> secondResultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state);
    }

}
