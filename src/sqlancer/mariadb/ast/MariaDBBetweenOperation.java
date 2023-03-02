package sqlancer.mariadb.ast;

import sqlancer.IgnoreMeException;
import sqlancer.mariadb.ast.MariaDBBinaryComparisonOperation.BinaryComparisonOperator;
import sqlancer.mariadb.ast.MariaDBBinaryLogicalOperation.MariaDBBinaryLogicalOperator;

public class MariaDBBetweenOperation implements MariaDBExpression {

    private final MariaDBExpression expr;
    private final MariaDBExpression left;
    private final MariaDBExpression right;

    public MariaDBBetweenOperation(MariaDBExpression expr, MariaDBExpression left, MariaDBExpression right) {
        this.expr = expr;
        this.left = left;
        this.right = right;
    }

    public MariaDBExpression getExpr() {
        return expr;
    }

    public MariaDBExpression getLeft() {
        return left;
    }

    public MariaDBExpression getRight() {
        return right;
    }

    @Override
    public MariaDBConstant getExpectedValue() {
        MariaDBExpression[] arr = { left, right, expr };
        MariaDBConstant convertedExpr = MariaDBComputableFunction.castToMostGeneralType(expr.getExpectedValue(), arr);
        MariaDBConstant convertedLeft = MariaDBComputableFunction.castToMostGeneralType(left.getExpectedValue(), arr);
        MariaDBConstant convertedRight = MariaDBComputableFunction.castToMostGeneralType(right.getExpectedValue(), arr);

        /* workaround for https://bugs.mariadb.com/bug.php?id=96006 */
        if (convertedLeft.isInt() && convertedLeft.getInt() < 0 || convertedRight.isInt() && convertedRight.getInt() < 0
                || convertedExpr.isInt() && convertedExpr.getInt() < 0) {
            throw new IgnoreMeException();
        }
        MariaDBBinaryComparisonOperation leftComparison = new MariaDBBinaryComparisonOperation(convertedLeft, convertedExpr,
                BinaryComparisonOperator.LESS_EQUALS);
        MariaDBBinaryComparisonOperation rightComparison = new MariaDBBinaryComparisonOperation(convertedExpr,
                convertedRight, BinaryComparisonOperator.LESS_EQUALS);
        MariaDBBinaryLogicalOperation andOperation = new MariaDBBinaryLogicalOperation(leftComparison, rightComparison,
                MariaDBBinaryLogicalOperator.AND);
        return andOperation.getExpectedValue();
    }

}
