package sqlancer.cockroachdb.oracle.tlp;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import sqlancer.cockroachdb.CockroachDBVisitor;
import sqlancer.cockroachdb.ast.CockroachDBExpression;

public class CockroachDBTLPBetweenAndOracle extends CockroachDBTLPBase {

    public CockroachDBTLPBetweenAndOracle(CockroachDBGlobalState state) {
        super(state);
        errors.add("GROUP BY term out of range");
    }

    @Override
    public void check() throws SQLException {
        super.check();
        // discard old where clause
        select.setWhereClause(null);

        // between partitioning
            // x BETWEEN y AND z ==> x >= y AND x <= z 

        // original
        select.setWhereClause(predicate);
        String firstQueryString = CockroachDBVisitor.asString(select);
        List <String> firstResultSet = ComparatorHelper.getResultSetFirstColumnAsString(firstQueryString, errors, state);

        CockroachDBExpression splitPredicate = genSplitPredicate(predicate);

        select.setWhereClause(splitPredicate);
        String secondQueryString = CockroachDBVisitor.asString(select);
        List <String> secondResultSet = ComparatorHelper.getResultSetFirstColumnAsString(secondQueryString, errors, state);
        List<String> combinedString = new ArrayList<>();
        combinedString.add(secondQueryString);
        
        // compare
        ComparatorHelper.assumeResultSetsAreEqual(firstResultSet, secondResultSet, firstQueryString, combinedString,
                state);
    }

}
