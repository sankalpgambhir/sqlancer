package sqlancer.mariadb.ast;

import sqlancer.mariadb.MariaDBSchema.MariaDBColumn;

public class MariaDBColumnReference implements MariaDBExpression {

    private final MariaDBColumn column;
    private final MariaDBConstant value;

    public MariaDBColumnReference(MariaDBColumn column, MariaDBConstant value) {
        this.column = column;
        this.value = value;
    }

    public static MariaDBColumnReference create(MariaDBColumn column, MariaDBConstant value) {
        return new MariaDBColumnReference(column, value);
    }

    public MariaDBColumn getColumn() {
        return column;
    }

    public MariaDBConstant getValue() {
        return value;
    }

    @Override
    public MariaDBConstant getExpectedValue() {
        return value;
    }

}
