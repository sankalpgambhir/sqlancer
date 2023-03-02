package sqlancer.mariadb.ast;

public interface MariaDBExpression {

    default MariaDBConstant getExpectedValue() {
        throw new AssertionError("PQS not supported for this operator");
    }

}
