package sqlancer.mariadb.ast;

import java.util.List;

import sqlancer.IgnoreMeException;

/**
 * @see <a href="https://dev.mariadb.com/doc/refman/8.0/en/comparison-operators.html#operator_in">Comparison Functions and
 *      Operators</a>
 */
public class MariaDBInOperation implements MariaDBExpression {

    private final MariaDBExpression expr;
    private final List<MariaDBExpression> listElements;
    private final boolean isTrue;

    public MariaDBInOperation(MariaDBExpression expr, List<MariaDBExpression> listElements, boolean isTrue) {
        this.expr = expr;
        this.listElements = listElements;
        this.isTrue = isTrue;
    }

    public MariaDBExpression getExpr() {
        return expr;
    }

    public List<MariaDBExpression> getListElements() {
        return listElements;
    }

    @Override
    public MariaDBConstant getExpectedValue() {
        MariaDBConstant leftVal = expr.getExpectedValue();
        if (leftVal.isNull()) {
            return MariaDBConstant.createNullConstant();
        }
        /* workaround for https://bugs.mariadb.com/bug.php?id=95957 */
        if (leftVal.isInt() && !leftVal.isSigned()) {
            throw new IgnoreMeException();
        }

        boolean isNull = false;
        for (MariaDBExpression rightExpr : listElements) {
            MariaDBConstant rightVal = rightExpr.getExpectedValue();

            /* workaround for https://bugs.mariadb.com/bug.php?id=95957 */
            if (rightVal.isInt() && !rightVal.isSigned()) {
                throw new IgnoreMeException();
            }
            MariaDBConstant convertedRightVal = rightVal;
            MariaDBConstant isEquals = leftVal.isEquals(convertedRightVal);
            if (isEquals.isNull()) {
                isNull = true;
            } else {
                if (isEquals.getInt() == 1) {
                    return MariaDBConstant.createBoolean(isTrue);
                }
            }
        }
        if (isNull) {
            return MariaDBConstant.createNullConstant();
        } else {
            return MariaDBConstant.createBoolean(!isTrue);
        }

    }

    public boolean isTrue() {
        return isTrue;
    }
}
