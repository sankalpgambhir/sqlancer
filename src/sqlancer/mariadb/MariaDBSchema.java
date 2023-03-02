package sqlancer.mariadb;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.schema.AbstractRelationalTable;
import sqlancer.common.schema.AbstractRowValue;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.AbstractTables;
import sqlancer.common.schema.TableIndex;
import sqlancer.mariadb.MariaDBSchema.MariaDBTable;
import sqlancer.mariadb.MariaDBSchema.MariaDBTable.MariaDBEngine;
import sqlancer.mariadb.ast.MariaDBConstant;

public class MariaDBSchema extends AbstractSchema<MariaDBGlobalState, MariaDBTable> {

    private static final int NR_SCHEMA_READ_TRIES = 10;

    public enum MariaDBDataType {
        INT, VARCHAR, FLOAT, DOUBLE, DECIMAL;

        public static MariaDBDataType getRandom(MariaDBGlobalState globalState) {
            if (globalState.usesPQS()) {
                return Randomly.fromOptions(MariaDBDataType.INT, MariaDBDataType.VARCHAR);
            } else {
                return Randomly.fromOptions(values());
            }
        }

        public boolean isNumeric() {
            switch (this) {
            case INT:
            case DOUBLE:
            case FLOAT:
            case DECIMAL:
                return true;
            case VARCHAR:
                return false;
            default:
                throw new AssertionError(this);
            }
        }

    }

    public static class MariaDBColumn extends AbstractTableColumn<MariaDBTable, MariaDBDataType> {

        private final boolean isPrimaryKey;
        private final int precision;

        public enum CollateSequence {
            NOCASE, RTRIM, BINARY;

            public static CollateSequence random() {
                return Randomly.fromOptions(values());
            }
        }

        public MariaDBColumn(String name, MariaDBDataType columnType, boolean isPrimaryKey, int precision) {
            super(name, null, columnType);
            this.isPrimaryKey = isPrimaryKey;
            this.precision = precision;
        }

        public int getPrecision() {
            return precision;
        }

        public boolean isPrimaryKey() {
            return isPrimaryKey;
        }

    }

    public static class MariaDBTables extends AbstractTables<MariaDBTable, MariaDBColumn> {

        public MariaDBTables(List<MariaDBTable> tables) {
            super(tables);
        }

        public MariaDBRowValue getRandomRowValue(SQLConnection con) throws SQLException {
            String randomRow = String.format("SELECT %s FROM %s ORDER BY RAND() LIMIT 1", columnNamesAsString(
                    c -> c.getTable().getName() + "." + c.getName() + " AS " + c.getTable().getName() + c.getName()),
                    // columnNamesAsString(c -> "typeof(" + c.getTable().getName() + "." +
                    // c.getName() + ")")
                    tableNamesAsString());
            Map<MariaDBColumn, MariaDBConstant> values = new HashMap<>();
            try (Statement s = con.createStatement()) {
                ResultSet randomRowValues = s.executeQuery(randomRow);
                if (!randomRowValues.next()) {
                    throw new AssertionError("could not find random row! " + randomRow + "\n");
                }
                for (int i = 0; i < getColumns().size(); i++) {
                    MariaDBColumn column = getColumns().get(i);
                    Object value;
                    int columnIndex = randomRowValues.findColumn(column.getTable().getName() + column.getName());
                    assert columnIndex == i + 1;
                    MariaDBConstant constant;
                    if (randomRowValues.getString(columnIndex) == null) {
                        constant = MariaDBConstant.createNullConstant();
                    } else {
                        switch (column.getType()) {
                        case INT:
                            value = randomRowValues.getLong(columnIndex);
                            constant = MariaDBConstant.createIntConstant((long) value);
                            break;
                        case VARCHAR:
                            value = randomRowValues.getString(columnIndex);
                            constant = MariaDBConstant.createStringConstant((String) value);
                            break;
                        default:
                            throw new AssertionError(column.getType());
                        }
                    }
                    values.put(column, constant);
                }
                assert !randomRowValues.next();
                return new MariaDBRowValue(this, values);
            }

        }

    }

    private static MariaDBDataType getColumnType(String typeString) {
        switch (typeString) {
        case "tinyint":
        case "smallint":
        case "mediumint":
        case "int":
        case "bigint":
            return MariaDBDataType.INT;
        case "varchar":
        case "tinytext":
        case "mediumtext":
        case "text":
        case "longtext":
            return MariaDBDataType.VARCHAR;
        case "double":
            return MariaDBDataType.DOUBLE;
        case "float":
            return MariaDBDataType.FLOAT;
        case "decimal":
            return MariaDBDataType.DECIMAL;
        default:
            throw new AssertionError(typeString);
        }
    }

    public static class MariaDBRowValue extends AbstractRowValue<MariaDBTables, MariaDBColumn, MariaDBConstant> {

        MariaDBRowValue(MariaDBTables tables, Map<MariaDBColumn, MariaDBConstant> values) {
            super(tables, values);
        }

    }

    public static class MariaDBTable extends AbstractRelationalTable<MariaDBColumn, MariaDBIndex, MariaDBGlobalState> {

        public enum MariaDBEngine {
            INNO_DB("InnoDB"), MY_ISAM("MyISAM"), MEMORY("MEMORY"), HEAP("HEAP"), CSV("CSV"), MERGE("MERGE"),
            ARCHIVE("ARCHIVE"), FEDERATED("FEDERATED");

            private String s;

            MariaDBEngine(String s) {
                this.s = s;
            }

            public static MariaDBEngine get(String val) {
                return Stream.of(values()).filter(engine -> engine.s.equalsIgnoreCase(val)).findFirst().get();
            }

        }

        private final MariaDBEngine engine;

        public MariaDBTable(String tableName, List<MariaDBColumn> columns, List<MariaDBIndex> indexes, MariaDBEngine engine) {
            super(tableName, columns, indexes, false /* TODO: support views */);
            this.engine = engine;
        }

        public MariaDBEngine getEngine() {
            return engine;
        }

        public boolean hasPrimaryKey() {
            return getColumns().stream().anyMatch(c -> c.isPrimaryKey());
        }

    }

    public static final class MariaDBIndex extends TableIndex {

        private MariaDBIndex(String indexName) {
            super(indexName);
        }

        public static MariaDBIndex create(String indexName) {
            return new MariaDBIndex(indexName);
        }

        @Override
        public String getIndexName() {
            if (super.getIndexName().contentEquals("PRIMARY")) {
                return "`PRIMARY`";
            } else {
                return super.getIndexName();
            }
        }

    }

    public static MariaDBSchema fromConnection(SQLConnection con, String databaseName) throws SQLException {
        Exception ex = null;
        /* the loop is a workaround for https://bugs.mariadb.com/bug.php?id=95929 */
        for (int i = 0; i < NR_SCHEMA_READ_TRIES; i++) {
            try {
                List<MariaDBTable> databaseTables = new ArrayList<>();
                try (Statement s = con.createStatement()) {
                    try (ResultSet rs = s.executeQuery(
                            "select TABLE_NAME, ENGINE from information_schema.TABLES where table_schema = '"
                                    + databaseName + "';")) {
                        while (rs.next()) {
                            String tableName = rs.getString("TABLE_NAME");
                            String tableEngineStr = rs.getString("ENGINE");
                            MariaDBEngine engine = MariaDBEngine.get(tableEngineStr);
                            List<MariaDBColumn> databaseColumns = getTableColumns(con, tableName, databaseName);
                            List<MariaDBIndex> indexes = getIndexes(con, tableName, databaseName);
                            MariaDBTable t = new MariaDBTable(tableName, databaseColumns, indexes, engine);
                            for (MariaDBColumn c : databaseColumns) {
                                c.setTable(t);
                            }
                            databaseTables.add(t);
                        }
                    }
                }
                return new MariaDBSchema(databaseTables);
            } catch (SQLIntegrityConstraintViolationException e) {
                ex = e;
            }
        }
        throw new AssertionError(ex);
    }

    private static List<MariaDBIndex> getIndexes(SQLConnection con, String tableName, String databaseName)
            throws SQLException {
        List<MariaDBIndex> indexes = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery(String.format(
                    "SELECT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME='%s';",
                    databaseName, tableName))) {
                while (rs.next()) {
                    String indexName = rs.getString("INDEX_NAME");
                    indexes.add(MariaDBIndex.create(indexName));
                }
            }
        }
        return indexes;
    }

    private static List<MariaDBColumn> getTableColumns(SQLConnection con, String tableName, String databaseName)
            throws SQLException {
        List<MariaDBColumn> columns = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery("select * from information_schema.columns where table_schema = '"
                    + databaseName + "' AND TABLE_NAME='" + tableName + "'")) {
                while (rs.next()) {
                    String columnName = rs.getString("COLUMN_NAME");
                    String dataType = rs.getString("DATA_TYPE");
                    int precision = rs.getInt("NUMERIC_PRECISION");
                    boolean isPrimaryKey = rs.getString("COLUMN_KEY").equals("PRI");
                    MariaDBColumn c = new MariaDBColumn(columnName, getColumnType(dataType), isPrimaryKey, precision);
                    columns.add(c);
                }
            }
        }
        return columns;
    }

    public MariaDBSchema(List<MariaDBTable> databaseTables) {
        super(databaseTables);
    }

    public MariaDBTables getRandomTableNonEmptyTables() {
        return new MariaDBTables(Randomly.nonEmptySubset(getDatabaseTables()));
    }

}
