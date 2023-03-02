package sqlancer.mariadb.ast;

import java.util.function.BinaryOperator;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.mariadb.ast.MariaDBCastOperation.CastType;

public class MariaDBBinaryOperation implements MariaDBExpression {

    private final MariaDBExpression left;
    private final MariaDBExpression right;
    private final MariaDBBinaryOperator op;

    public enum MariaDBBinaryOperator {

        AND("&") {
            @Override
            public MariaDBConstant apply(MariaDBConstant left, MariaDBConstant right) {
                return applyBitOperation(left, right, (l, r) -> l & r);
            }

        },
        OR("|") {
            @Override
            public MariaDBConstant apply(MariaDBConstant left, MariaDBConstant right) {
                return applyBitOperation(left, right, (l, r) -> l | r);
            }
        },
        XOR("^") {
            @Override
            public MariaDBConstant apply(MariaDBConstant left, MariaDBConstant right) {
                return applyBitOperation(left, right, (l, r) -> l ^ r);
            }
        };

        private String textRepresentation;

        private static MariaDBConstant applyBitOperation(MariaDBConstant left, MariaDBConstant right,
                BinaryOperator<Long> op) {
            if (left.isNull() || right.isNull()) {
                return MariaDBConstant.createNullConstant();
            } else {
                long leftVal = left.castAs(CastType.SIGNED).getInt();
                long rightVal = right.castAs(CastType.SIGNED).getInt();
                long value = op.apply(leftVal, rightVal);
                return MariaDBConstant.createUnsignedIntConstant(value);
            }
        }

        MariaDBBinaryOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public String getTextRepresentation() {
            return textRepresentation;
        }

        public abstract MariaDBConstant apply(MariaDBConstant left, MariaDBConstant right);

        public static MariaDBBinaryOperator getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public MariaDBBinaryOperation(MariaDBExpression left, MariaDBExpression right, MariaDBBinaryOperator op) {
        this.left = left;
        this.right = right;
        this.op = op;
    }

    @Override
    public MariaDBConstant getExpectedValue() {
        MariaDBConstant leftExpected = left.getExpectedValue();
        MariaDBConstant rightExpected = right.getExpectedValue();

        /* workaround for https://bugs.mariadb.com/bug.php?id=95960 */
        if (leftExpected.isString()) {
            String text = leftExpected.castAsString();
            while ((text.startsWith(" ") || text.startsWith("\t")) && text.length() > 0) {
                text = text.substring(1);
            }
            if (text.length() > 0 && (text.startsWith("\n") || text.startsWith("."))) {
                throw new IgnoreMeException();
            }
        }

        if (rightExpected.isString()) {
            String text = rightExpected.castAsString();
            while ((text.startsWith(" ") || text.startsWith("\t")) && text.length() > 0) {
                text = text.substring(1);
            }
            if (text.length() > 0 && (text.startsWith("\n") || text.startsWith("."))) {
                throw new IgnoreMeException();
            }
        }

        return op.apply(leftExpected, rightExpected);
    }

    public MariaDBExpression getLeft() {
        return left;
    }

    public MariaDBBinaryOperator getOp() {
        return op;
    }

    public MariaDBExpression getRight() {
        return right;
    }

}
