package sqlancer.mariadb.oracle;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.mariadb.MariaDBGlobalState;
import sqlancer.mariadb.MariaDBVisitor;

public class MariaDBTLPWhereOracle extends MariaDBQueryPartitioningBase {

    public MariaDBTLPWhereOracle(MariaDBGlobalState state) {
        super(state);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        select.setWhereClause(null);
        String originalQueryString = MariaDBVisitor.asString(select);

        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

        if (Randomly.getBoolean()) {
            select.setOrderByExpressions(gen.generateOrderBys());
        }
        select.setOrderByExpressions(Collections.emptyList());
        select.setWhereClause(predicate);
        String firstQueryString = MariaDBVisitor.asString(select);
        select.setWhereClause(negatedPredicate);
        String secondQueryString = MariaDBVisitor.asString(select);
        select.setWhereClause(isNullPredicate);
        String thirdQueryString = MariaDBVisitor.asString(select);
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = ComparatorHelper.getCombinedResultSet(firstQueryString, secondQueryString,
                thirdQueryString, combinedString, Randomly.getBoolean(), state, errors);
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state);
    }

}
