package sqlancer.mariadb.oracle;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.common.gen.ExpressionGenerator;
import sqlancer.common.oracle.TernaryLogicPartitioningOracleBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.mariadb.MariaDBErrors;
import sqlancer.mariadb.MariaDBGlobalState;
import sqlancer.mariadb.MariaDBSchema;
import sqlancer.mariadb.MariaDBSchema.MariaDBTable;
import sqlancer.mariadb.MariaDBSchema.MariaDBTables;
import sqlancer.mariadb.ast.MariaDBColumnReference;
import sqlancer.mariadb.ast.MariaDBExpression;
import sqlancer.mariadb.ast.MariaDBSelect;
import sqlancer.mariadb.ast.MariaDBTableReference;
import sqlancer.mariadb.gen.MariaDBExpressionGenerator;

public abstract class MariaDBQueryPartitioningBase extends
        TernaryLogicPartitioningOracleBase<MariaDBExpression, MariaDBGlobalState> implements TestOracle<MariaDBGlobalState> {

    MariaDBSchema s;
    MariaDBTables targetTables;
    MariaDBExpressionGenerator gen;
    MariaDBSelect select;

    public MariaDBQueryPartitioningBase(MariaDBGlobalState state) {
        super(state);
        MariaDBErrors.addExpressionErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        s = state.getSchema();
        targetTables = s.getRandomTableNonEmptyTables();
        gen = new MariaDBExpressionGenerator(state).setColumns(targetTables.getColumns());
        initializeTernaryPredicateVariants();
        select = new MariaDBSelect();
        select.setFetchColumns(generateFetchColumns());
        List<MariaDBTable> tables = targetTables.getTables();
        List<MariaDBExpression> tableList = tables.stream().map(t -> new MariaDBTableReference(t))
                .collect(Collectors.toList());
        // List<MariaDBExpression> joins = MariaDBJoin.getJoins(tableList, state);
        select.setFromList(tableList);
        select.setWhereClause(null);
        // select.setJoins(joins);
    }

    List<MariaDBExpression> generateFetchColumns() {
        return Arrays.asList(MariaDBColumnReference.create(targetTables.getColumns().get(0), null));
    }

    @Override
    protected ExpressionGenerator<MariaDBExpression> getGen() {
        return gen;
    }

}
