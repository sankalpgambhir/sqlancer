package sqlancer.mariadb.ast;

import sqlancer.LikeImplementationHelper;
import sqlancer.Randomly;
import sqlancer.mariadb.MariaDBSchema.MariaDBDataType;
import sqlancer.mariadb.ast.MariaDBUnaryPrefixOperation.MariaDBUnaryPrefixOperator;

public class MariaDBBinaryComparisonOperation implements MariaDBExpression {

    public enum BinaryComparisonOperator {
        EQUALS("=") {
            @Override
            public MariaDBConstant getExpectedValue(MariaDBConstant leftVal, MariaDBConstant rightVal) {
                return leftVal.isEquals(rightVal);
            }
        },
        NOT_EQUALS("!=") {
            @Override
            public MariaDBConstant getExpectedValue(MariaDBConstant leftVal, MariaDBConstant rightVal) {
                MariaDBConstant isEquals = leftVal.isEquals(rightVal);
                if (isEquals.getType() == MariaDBDataType.INT) {
                    return MariaDBConstant.createIntConstant(1 - isEquals.getInt());
                }
                return isEquals;
            }
        },
        LESS("<") {

            @Override
            public MariaDBConstant getExpectedValue(MariaDBConstant leftVal, MariaDBConstant rightVal) {
                return leftVal.isLessThan(rightVal);
            }
        },
        LESS_EQUALS("<=") {

            @Override
            public MariaDBConstant getExpectedValue(MariaDBConstant leftVal, MariaDBConstant rightVal) {
                MariaDBConstant lessThan = leftVal.isLessThan(rightVal);
                if (lessThan == null) {
                    return null;
                }
                if (lessThan.getType() == MariaDBDataType.INT && lessThan.getInt() == 0) {
                    return leftVal.isEquals(rightVal);
                } else {
                    return lessThan;
                }
            }
        },
        GREATER(">") {
            @Override
            public MariaDBConstant getExpectedValue(MariaDBConstant leftVal, MariaDBConstant rightVal) {
                MariaDBConstant equals = leftVal.isEquals(rightVal);
                if (equals.getType() == MariaDBDataType.INT && equals.getInt() == 1) {
                    return MariaDBConstant.createFalse();
                } else {
                    MariaDBConstant applyLess = leftVal.isLessThan(rightVal);
                    if (applyLess.isNull()) {
                        return MariaDBConstant.createNullConstant();
                    }
                    return MariaDBUnaryPrefixOperator.NOT.applyNotNull(applyLess);
                }
            }
        },
        GREATER_EQUALS(">=") {

            @Override
            public MariaDBConstant getExpectedValue(MariaDBConstant leftVal, MariaDBConstant rightVal) {
                MariaDBConstant equals = leftVal.isEquals(rightVal);
                if (equals.getType() == MariaDBDataType.INT && equals.getInt() == 1) {
                    return MariaDBConstant.createTrue();
                } else {
                    MariaDBConstant applyLess = leftVal.isLessThan(rightVal);
                    if (applyLess.isNull()) {
                        return MariaDBConstant.createNullConstant();
                    }
                    return MariaDBUnaryPrefixOperator.NOT.applyNotNull(applyLess);
                }
            }

        },
        LIKE("LIKE") {

            @Override
            public MariaDBConstant getExpectedValue(MariaDBConstant leftVal, MariaDBConstant rightVal) {
                if (leftVal.isNull() || rightVal.isNull()) {
                    return MariaDBConstant.createNullConstant();
                }
                String leftStr = leftVal.castAsString();
                String rightStr = rightVal.castAsString();
                boolean matches = LikeImplementationHelper.match(leftStr, rightStr, 0, 0, false);
                return MariaDBConstant.createBoolean(matches);
            }

        };
        // https://bugs.mariadb.com/bug.php?id=95908
        /*
         * IS_EQUALS_NULL_SAFE("<=>") {
         *
         * @Override public MariaDBConstant getExpectedValue(MariaDBConstant leftVal, MariaDBConstant rightVal) { return
         * leftVal.isEqualsNullSafe(rightVal); }
         *
         * };
         */

        private final String textRepresentation;

        public String getTextRepresentation() {
            return textRepresentation;
        }

        BinaryComparisonOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public abstract MariaDBConstant getExpectedValue(MariaDBConstant leftVal, MariaDBConstant rightVal);

        public static BinaryComparisonOperator getRandom() {
            return Randomly.fromOptions(BinaryComparisonOperator.values());
        }
    }

    private final MariaDBExpression left;
    private final MariaDBExpression right;
    private final BinaryComparisonOperator op;

    public MariaDBBinaryComparisonOperation(MariaDBExpression left, MariaDBExpression right, BinaryComparisonOperator op) {
        this.left = left;
        this.right = right;
        this.op = op;
    }

    public MariaDBExpression getLeft() {
        return left;
    }

    public BinaryComparisonOperator getOp() {
        return op;
    }

    public MariaDBExpression getRight() {
        return right;
    }

    @Override
    public MariaDBConstant getExpectedValue() {
        return op.getExpectedValue(left.getExpectedValue(), right.getExpectedValue());
    }

}
