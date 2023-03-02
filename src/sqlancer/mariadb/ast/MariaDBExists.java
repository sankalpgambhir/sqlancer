package sqlancer.mariadb.ast;

public class MariaDBExists implements MariaDBExpression {

    private final MariaDBExpression expr;
    private final MariaDBConstant expected;

    public MariaDBExists(MariaDBExpression expr, MariaDBConstant expectedValue) {
        this.expr = expr;
        this.expected = expectedValue;
    }

    public MariaDBExists(MariaDBExpression expr) {
        this.expr = expr;
        this.expected = expr.getExpectedValue();
        if (expected == null) {
            throw new AssertionError();
        }
    }

    public MariaDBExpression getExpr() {
        return expr;
    }

    @Override
    public MariaDBConstant getExpectedValue() {
        return expected;
    }

}
