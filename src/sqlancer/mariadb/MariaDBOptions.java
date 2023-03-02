package sqlancer.mariadb;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import sqlancer.DBMSSpecificOptions;
import sqlancer.OracleFactory;
import sqlancer.common.oracle.TestOracle;
import sqlancer.mariadb.MariaDBOptions.MariaDBOracleFactory;
import sqlancer.mariadb.oracle.MariaDBPivotedQuerySynthesisOracle;
import sqlancer.mariadb.oracle.MariaDBTLPWhereOracle;

@Parameters(separators = "=", commandDescription = "MariaDB (default port: " + MariaDBOptions.DEFAULT_PORT
        + ", default host: " + MariaDBOptions.DEFAULT_HOST + ")")
public class MariaDBOptions implements DBMSSpecificOptions<MariaDBOracleFactory> {
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 3306;

    @Parameter(names = "--oracle")
    public List<MariaDBOracleFactory> oracles = Arrays.asList(MariaDBOracleFactory.TLP_WHERE);

    public enum MariaDBOracleFactory implements OracleFactory<MariaDBGlobalState> {

        TLP_WHERE {

            @Override
            public TestOracle<MariaDBGlobalState> create(MariaDBGlobalState globalState) throws SQLException {
                return new MariaDBTLPWhereOracle(globalState);
            }

        },
        PQS {

            @Override
            public TestOracle<MariaDBGlobalState> create(MariaDBGlobalState globalState) throws SQLException {
                return new MariaDBPivotedQuerySynthesisOracle(globalState);
            }

            @Override
            public boolean requiresAllTablesToContainRows() {
                return true;
            }

        }
    }

    @Override
    public List<MariaDBOracleFactory> getTestOracleFactory() {
        return oracles;
    }

}
