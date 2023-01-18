package sqlancer.mysql;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import sqlancer.DBMSSpecificOptions;
import sqlancer.OracleFactory;
import sqlancer.common.oracle.CompositeTestOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.mysql.MySQLOptions.MySQLOracleFactory;
import sqlancer.mysql.oracle.MySQLPivotedQuerySynthesisOracle;
import sqlancer.mysql.oracle.MySQLTLPBetweenAndOracle;
import sqlancer.mysql.oracle.MySQLTLPBetweenIntersectOracle;
import sqlancer.mysql.oracle.MySQLTLPGroupByDistinctOracle;
import sqlancer.mysql.oracle.MySQLTLPWhereOracle;

@Parameters(separators = "=", commandDescription = "MySQL (default port: " + MySQLOptions.DEFAULT_PORT
        + ", default host: " + MySQLOptions.DEFAULT_HOST + ")")
public class MySQLOptions implements DBMSSpecificOptions<MySQLOracleFactory> {
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 3306;

    @Parameter(names = "--oracle")
    public List<MySQLOracleFactory> oracles = Arrays.asList(MySQLOracleFactory.TLP_WHERE);

    public enum MySQLOracleFactory implements OracleFactory<MySQLGlobalState> {

        TLP_WHERE {
            @Override
            public TestOracle create(MySQLGlobalState globalState) throws SQLException {
                return new MySQLTLPWhereOracle(globalState);
            }
        },
        PQS {

            @Override
            public TestOracle create(MySQLGlobalState globalState) throws SQLException {
                return new MySQLPivotedQuerySynthesisOracle(globalState);
            }

            @Override
            public boolean requiresAllTablesToContainRows() {
                return true;
            }
        },
        BETWEEN_AND {
            @Override
            public TestOracle create(MySQLGlobalState globalState) throws SQLException {
                return new MySQLTLPBetweenAndOracle(globalState);
            }
        },
        BETWEEN_INTERSECT {
            @Override
            public TestOracle create(MySQLGlobalState globalState) throws SQLException {
                return new MySQLTLPBetweenIntersectOracle(globalState);
            }
        },
        GROUPBY_DISTINCT {
            @Override
            public TestOracle create(MySQLGlobalState globalState) throws SQLException {
                return new MySQLTLPGroupByDistinctOracle(globalState);
            }
        },
        QUERY_PARTITIONING {
            @Override
            public TestOracle create(MySQLGlobalState globalState) throws SQLException {
                List<TestOracle> oracles = new ArrayList<>();
                // oracles.add(new MySQLTLPWhereOracle(globalState));
                oracles.add(new MySQLTLPBetweenAndOracle(globalState));
                // oracles.add(new MySQLTLPBetweenIntersectOracle(globalState));
                // oracles.add(new MySQLTLPGroupByDistinctOracle(globalState));
                return new CompositeTestOracle(oracles, globalState);
            }
        };
    }

    @Override
    public List<MySQLOracleFactory> getTestOracleFactory() {
        return oracles;
    }

}
