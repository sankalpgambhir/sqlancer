package sqlancer.mysql.oracle;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.mysql.MySQLGlobalState;
import sqlancer.mysql.MySQLVisitor;
import sqlancer.mysql.ast.MySQLExpression;
import sqlancer.mysql.ast.MySQLSelect.SelectType;

public class MySQLTLPBetweenIntersectOracle extends MySQLQueryPartitioningBase {

    public MySQLTLPBetweenIntersectOracle(MySQLGlobalState state) {
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
        String originalQueryString = MySQLVisitor.asString(select);
        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

        // should not affect things, ideally
        boolean orderBy = Randomly.getBooleanWithSmallProbability();
        if (orderBy) {
            select.setOrderByExpressions(gen.generateOrderBys());
        }

        // modify the predicate if possible
        MySQLExpression leftPredicate = genDiscardedBetweenPredicate(predicate, true);
        select.setWhereClause(leftPredicate);
        String firstQueryString = MySQLVisitor.asString(select);
        
        MySQLExpression rightPredicate = genDiscardedBetweenPredicate(predicate, false);
        select.setWhereClause(rightPredicate);
        String secondQueryString = MySQLVisitor.asString(select);

        List<String> combinedString = new ArrayList<>();
        
        List<String> secondResultSet = ComparatorHelper.getIntersectedResultSet(firstQueryString, secondQueryString, combinedString, state, errors);

        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state);
    }

}
