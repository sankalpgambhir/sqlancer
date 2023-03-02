package sqlancer.mariadb.ast;

public class MariaDBCastOperation implements MariaDBExpression {

    private final MariaDBExpression expr;
    private final CastType type;

    public enum CastType {
        SIGNED, UNSIGNED;

        public static CastType getRandom() {
            return SIGNED;
            // return Randomly.fromOptions(CastType.values());
        }

    }

    public MariaDBCastOperation(MariaDBExpression expr, CastType type) {
        this.expr = expr;
        this.type = type;
    }

    public MariaDBExpression getExpr() {
        return expr;
    }

    public CastType getType() {
        return type;
    }

    @Override
    public MariaDBConstant getExpectedValue() {
        return expr.getExpectedValue().castAs(type);
    }

}
