package sqlancer.cockroachdb.oracle.tlp;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import sqlancer.cockroachdb.CockroachDBVisitor;
import sqlancer.cockroachdb.ast.CockroachDBExpression;

public class CockroachDBTLPBetweenIntersectOracle extends CockroachDBTLPBase {

    public CockroachDBTLPBetweenIntersectOracle(CockroachDBGlobalState state) {
        super(state);
        errors.add("GROUP BY term out of range");
    }

    @Override
    public void check() throws SQLException {
        super.check();
        // discard old where clause
        select.setWhereClause(null);
        
        // between partitioning
            // check ast to find a between, if not, return
            // if yes, convert
            // x BETWEEN y AND z ==> x >= y INTERSECT x <= z

        // INTERSECT forces a distinct, so the original query needs one as well
        select.setDistinct(true);

        // original
        select.setWhereClause(predicate);
        String originalQueryString = CockroachDBVisitor.asString(select);
        List <String> originalResultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

        CockroachDBExpression leftPredicate = genBetweenDiscardedPredicate(predicate, false);
        select.setWhereClause(leftPredicate);
        String firstQueryString = CockroachDBVisitor.asString(select);
        
        CockroachDBExpression rightPredicate = genBetweenDiscardedPredicate(predicate, true);
        select.setWhereClause(rightPredicate);
        String secondQueryString = CockroachDBVisitor.asString(select);

        // intersect
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = ComparatorHelper.getIntersectedResultSet(firstQueryString,
                secondQueryString, combinedString, state, errors);
        
        // compare
        ComparatorHelper.assumeResultSetsAreEqual(originalResultSet, secondResultSet, originalQueryString, combinedString,
                state);
    }

}
