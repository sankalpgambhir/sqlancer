package sqlancer.mariadb.ast;

import sqlancer.Randomly;

public class MariaDBBinaryLogicalOperation implements MariaDBExpression {

    private final MariaDBExpression left;
    private final MariaDBExpression right;
    private final MariaDBBinaryLogicalOperator op;
    private final String textRepresentation;

    public enum MariaDBBinaryLogicalOperator {
        AND("AND", "&&") {
            @Override
            public MariaDBConstant apply(MariaDBConstant left, MariaDBConstant right) {
                if (left.isNull() && right.isNull()) {
                    return MariaDBConstant.createNullConstant();
                } else if (left.isNull()) {
                    if (right.asBooleanNotNull()) {
                        return MariaDBConstant.createNullConstant();
                    } else {
                        return MariaDBConstant.createFalse();
                    }
                } else if (right.isNull()) {
                    if (left.asBooleanNotNull()) {
                        return MariaDBConstant.createNullConstant();
                    } else {
                        return MariaDBConstant.createFalse();
                    }
                } else {
                    return MariaDBConstant.createBoolean(left.asBooleanNotNull() && right.asBooleanNotNull());
                }
            }
        },
        OR("OR", "||") {
            @Override
            public MariaDBConstant apply(MariaDBConstant left, MariaDBConstant right) {
                if (!left.isNull() && left.asBooleanNotNull()) {
                    return MariaDBConstant.createTrue();
                } else if (!right.isNull() && right.asBooleanNotNull()) {
                    return MariaDBConstant.createTrue();
                } else if (left.isNull() || right.isNull()) {
                    return MariaDBConstant.createNullConstant();
                } else {
                    return MariaDBConstant.createFalse();
                }
            }
        },
        XOR("XOR") {
            @Override
            public MariaDBConstant apply(MariaDBConstant left, MariaDBConstant right) {
                if (left.isNull() || right.isNull()) {
                    return MariaDBConstant.createNullConstant();
                }
                boolean xorVal = left.asBooleanNotNull() ^ right.asBooleanNotNull();
                return MariaDBConstant.createBoolean(xorVal);
            }
        };

        private final String[] textRepresentations;

        MariaDBBinaryLogicalOperator(String... textRepresentations) {
            this.textRepresentations = textRepresentations.clone();
        }

        String getTextRepresentation() {
            return Randomly.fromOptions(textRepresentations);
        }

        public abstract MariaDBConstant apply(MariaDBConstant left, MariaDBConstant right);

        public static MariaDBBinaryLogicalOperator getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    public MariaDBBinaryLogicalOperation(MariaDBExpression left, MariaDBExpression right, MariaDBBinaryLogicalOperator op) {
        this.left = left;
        this.right = right;
        this.op = op;
        this.textRepresentation = op.getTextRepresentation();
    }

    public MariaDBExpression getLeft() {
        return left;
    }

    public MariaDBBinaryLogicalOperator getOp() {
        return op;
    }

    public MariaDBExpression getRight() {
        return right;
    }

    public String getTextRepresentation() {
        return textRepresentation;
    }

    @Override
    public MariaDBConstant getExpectedValue() {
        MariaDBConstant leftExpected = left.getExpectedValue();
        MariaDBConstant rightExpected = right.getExpectedValue();
        if (left.getExpectedValue() == null || right.getExpectedValue() == null) {
            return null;
        }
        return op.apply(leftExpected, rightExpected);
    }

}
