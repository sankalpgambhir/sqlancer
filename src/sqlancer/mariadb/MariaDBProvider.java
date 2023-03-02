package sqlancer.mariadb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import com.google.auto.service.AutoService;

import sqlancer.AbstractAction;
import sqlancer.DatabaseProvider;
import sqlancer.IgnoreMeException;
import sqlancer.MainOptions;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.SQLProviderAdapter;
import sqlancer.StatementExecutor;
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLQueryProvider;
import sqlancer.mariadb.gen.MariaDBAlterTable;
import sqlancer.mariadb.gen.MariaDBDeleteGenerator;
import sqlancer.mariadb.gen.MariaDBDropIndex;
import sqlancer.mariadb.gen.MariaDBInsertGenerator;
import sqlancer.mariadb.gen.MariaDBSetGenerator;
import sqlancer.mariadb.gen.MariaDBTableGenerator;
import sqlancer.mariadb.gen.MariaDBTruncateTableGenerator;
import sqlancer.mariadb.gen.admin.MariaDBFlush;
import sqlancer.mariadb.gen.admin.MariaDBReset;
import sqlancer.mariadb.gen.datadef.MariaDBIndexGenerator;
import sqlancer.mariadb.gen.tblmaintenance.MariaDBAnalyzeTable;
import sqlancer.mariadb.gen.tblmaintenance.MariaDBCheckTable;
import sqlancer.mariadb.gen.tblmaintenance.MariaDBChecksum;
import sqlancer.mariadb.gen.tblmaintenance.MariaDBOptimize;
import sqlancer.mariadb.gen.tblmaintenance.MariaDBRepair;

@AutoService(DatabaseProvider.class)
public class MariaDBProvider extends SQLProviderAdapter<MariaDBGlobalState, MariaDBOptions> {

    public MariaDBProvider() {
        super(MariaDBGlobalState.class, MariaDBOptions.class);
    }

    enum Action implements AbstractAction<MariaDBGlobalState> {
        SHOW_TABLES((g) -> new SQLQueryAdapter("SHOW TABLES")), //
        INSERT(MariaDBInsertGenerator::insertRow), //
        SET_VARIABLE(MariaDBSetGenerator::set), //
        REPAIR(MariaDBRepair::repair), //
        OPTIMIZE(MariaDBOptimize::optimize), //
        CHECKSUM(MariaDBChecksum::checksum), //
        CHECK_TABLE(MariaDBCheckTable::check), //
        ANALYZE_TABLE(MariaDBAnalyzeTable::analyze), //
        FLUSH(MariaDBFlush::create), RESET(MariaDBReset::create), CREATE_INDEX(MariaDBIndexGenerator::create), //
        ALTER_TABLE(MariaDBAlterTable::create), //
        TRUNCATE_TABLE(MariaDBTruncateTableGenerator::generate), //
        SELECT_INFO((g) -> new SQLQueryAdapter(
                "select TABLE_NAME, ENGINE from information_schema.TABLES where table_schema = '" + g.getDatabaseName()
                        + "'")), //
        CREATE_TABLE((g) -> {
            // TODO refactor
            String tableName = DBMSCommon.createTableName(g.getSchema().getDatabaseTables().size());
            return MariaDBTableGenerator.generate(g, tableName);
        }), //
        DELETE(MariaDBDeleteGenerator::delete), //
        DROP_INDEX(MariaDBDropIndex::generate);

        private final SQLQueryProvider<MariaDBGlobalState> sqlQueryProvider;

        Action(SQLQueryProvider<MariaDBGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        @Override
        public SQLQueryAdapter getQuery(MariaDBGlobalState globalState) throws Exception {
            return sqlQueryProvider.getQuery(globalState);
        }
    }

    private static int mapActions(MariaDBGlobalState globalState, Action a) {
        Randomly r = globalState.getRandomly();
        int nrPerformed = 0;
        switch (a) {
        case DROP_INDEX:
            nrPerformed = r.getInteger(0, 2);
            break;
        case SHOW_TABLES:
            nrPerformed = r.getInteger(0, 1);
            break;
        case CREATE_TABLE:
            nrPerformed = r.getInteger(0, 1);
            break;
        case INSERT:
            nrPerformed = r.getInteger(0, globalState.getOptions().getMaxNumberInserts());
            break;
        case REPAIR:
            nrPerformed = r.getInteger(0, 1);
            break;
        case SET_VARIABLE:
            nrPerformed = r.getInteger(0, 5);
            break;
        case CREATE_INDEX:
            nrPerformed = r.getInteger(0, 5);
            break;
        case FLUSH:
            nrPerformed = Randomly.getBooleanWithSmallProbability() ? r.getInteger(0, 1) : 0;
            break;
        case OPTIMIZE:
            // seems to yield low CPU utilization
            nrPerformed = Randomly.getBooleanWithSmallProbability() ? r.getInteger(0, 1) : 0;
            break;
        case RESET:
            // affects the global state, so do not execute
            nrPerformed = globalState.getOptions().getNumberConcurrentThreads() == 1 ? r.getInteger(0, 1) : 0;
            break;
        case CHECKSUM:
        case CHECK_TABLE:
        case ANALYZE_TABLE:
            nrPerformed = r.getInteger(0, 2);
            break;
        case ALTER_TABLE:
            nrPerformed = r.getInteger(0, 5);
            break;
        case TRUNCATE_TABLE:
            nrPerformed = r.getInteger(0, 2);
            break;
        case SELECT_INFO:
            nrPerformed = r.getInteger(0, 10);
            break;
        case DELETE:
            nrPerformed = r.getInteger(0, 10);
            break;
        default:
            throw new AssertionError(a);
        }
        return nrPerformed;
    }

    @Override
    public void generateDatabase(MariaDBGlobalState globalState) throws Exception {
        while (globalState.getSchema().getDatabaseTables().size() < Randomly.smallNumber() + 1) {
            String tableName = DBMSCommon.createTableName(globalState.getSchema().getDatabaseTables().size());
            SQLQueryAdapter createTable = MariaDBTableGenerator.generate(globalState, tableName);
            globalState.executeStatement(createTable);
        }

        StatementExecutor<MariaDBGlobalState, Action> se = new StatementExecutor<>(globalState, Action.values(),
                MariaDBProvider::mapActions, (q) -> {
                    if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                        throw new IgnoreMeException();
                    }
                });
        se.executeStatements();
    }

    @Override
    public SQLConnection createDatabase(MariaDBGlobalState globalState) throws SQLException {
        String username = globalState.getOptions().getUserName();
        String password = globalState.getOptions().getPassword();
        String host = globalState.getOptions().getHost();
        int port = globalState.getOptions().getPort();
        if (host == null) {
            host = MariaDBOptions.DEFAULT_HOST;
        }
        if (port == MainOptions.NO_SET_PORT) {
            port = MariaDBOptions.DEFAULT_PORT;
        }
        String databaseName = globalState.getDatabaseName();
        globalState.getState().logStatement("DROP DATABASE IF EXISTS " + databaseName);
        globalState.getState().logStatement("CREATE DATABASE " + databaseName);
        globalState.getState().logStatement("USE " + databaseName);
        String url = String.format("jdbc:mariadb://%s:%d?serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true",
                host, port);
        Connection con = DriverManager.getConnection(url, username, password);
        try (Statement s = con.createStatement()) {
            s.execute("DROP DATABASE IF EXISTS " + databaseName);
        }
        try (Statement s = con.createStatement()) {
            s.execute("CREATE DATABASE " + databaseName);
        }
        try (Statement s = con.createStatement()) {
            s.execute("USE " + databaseName);
        }
        return new SQLConnection(con);
    }

    @Override
    public String getDBMSName() {
        return "mariadb";
    }

}
