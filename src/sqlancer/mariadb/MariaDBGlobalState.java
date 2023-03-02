
package sqlancer.mariadb;

import java.sql.SQLException;

import sqlancer.SQLGlobalState;
import sqlancer.mariadb.MariaDBOptions.MariaDBOracleFactory;

public class MariaDBGlobalState extends SQLGlobalState<MariaDBOptions, MariaDBSchema> {

    @Override
    protected MariaDBSchema readSchema() throws SQLException {
        return MariaDBSchema.fromConnection(getConnection(), getDatabaseName());
    }

    public boolean usesPQS() {
        return getDbmsSpecificOptions().oracles.stream().anyMatch(o -> o == MariaDBOracleFactory.PQS);
    }

}
