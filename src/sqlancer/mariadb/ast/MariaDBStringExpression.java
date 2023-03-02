package sqlancer.mariadb.ast;

public class MariaDBStringExpression implements MariaDBExpression {

    private final String str;
    private final MariaDBConstant expectedValue;

    public MariaDBStringExpression(String str, MariaDBConstant expectedValue) {
        this.str = str;
        this.expectedValue = expectedValue;
    }

    public String getStr() {
        return str;
    }

    @Override
    public MariaDBConstant getExpectedValue() {
        return expectedValue;
    }

}
