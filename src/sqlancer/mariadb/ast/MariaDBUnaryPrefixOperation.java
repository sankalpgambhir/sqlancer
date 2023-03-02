package sqlancer.mariadb.ast;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.common.ast.UnaryOperatorNode;
import sqlancer.mariadb.ast.MariaDBUnaryPrefixOperation.MariaDBUnaryPrefixOperator;

public class MariaDBUnaryPrefixOperation extends UnaryOperatorNode<MariaDBExpression, MariaDBUnaryPrefixOperator>
        implements MariaDBExpression {

    public enum MariaDBUnaryPrefixOperator implements Operator {
        NOT("!", "NOT") {
            @Override
            public MariaDBConstant applyNotNull(MariaDBConstant expr) {
                return MariaDBConstant.createIntConstant(expr.asBooleanNotNull() ? 0 : 1);
            }
        },
        PLUS("+") {
            @Override
            public MariaDBConstant applyNotNull(MariaDBConstant expr) {
                return expr;
            }
        },
        MINUS("-") {
            @Override
            public MariaDBConstant applyNotNull(MariaDBConstant expr) {
                if (expr.isString()) {
                    // TODO: implement floating points
                    throw new IgnoreMeException();
                } else if (expr.isInt()) {
                    if (!expr.isSigned()) {
                        // TODO
                        throw new IgnoreMeException();
                    }
                    return MariaDBConstant.createIntConstant(-expr.getInt());
                } else {
                    throw new AssertionError(expr);
                }
            }
        };

        private String[] textRepresentations;

        MariaDBUnaryPrefixOperator(String... textRepresentations) {
            this.textRepresentations = textRepresentations.clone();
        }

        public abstract MariaDBConstant applyNotNull(MariaDBConstant expr);

        public static MariaDBUnaryPrefixOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return Randomly.fromOptions(textRepresentations);
        }
    }

    public MariaDBUnaryPrefixOperation(MariaDBExpression expr, MariaDBUnaryPrefixOperator op) {
        super(expr, op);
    }

    @Override
    public MariaDBConstant getExpectedValue() {
        MariaDBConstant subExprVal = expr.getExpectedValue();
        if (subExprVal.isNull()) {
            return MariaDBConstant.createNullConstant();
        } else {
            return op.applyNotNull(subExprVal);
        }
    }

    @Override
    public OperatorKind getOperatorKind() {
        return OperatorKind.PREFIX;
    }

}
