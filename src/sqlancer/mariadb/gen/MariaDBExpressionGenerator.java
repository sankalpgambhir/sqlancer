package sqlancer.mariadb.gen;

import java.util.ArrayList;
import java.util.List;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.gen.UntypedExpressionGenerator;
import sqlancer.mariadb.MariaDBBugs;
import sqlancer.mariadb.MariaDBGlobalState;
import sqlancer.mariadb.MariaDBSchema.MariaDBColumn;
import sqlancer.mariadb.MariaDBSchema.MariaDBRowValue;
import sqlancer.mariadb.ast.MariaDBBetweenOperation;
import sqlancer.mariadb.ast.MariaDBBinaryComparisonOperation;
import sqlancer.mariadb.ast.MariaDBBinaryComparisonOperation.BinaryComparisonOperator;
import sqlancer.mariadb.ast.MariaDBBinaryLogicalOperation;
import sqlancer.mariadb.ast.MariaDBBinaryLogicalOperation.MariaDBBinaryLogicalOperator;
import sqlancer.mariadb.ast.MariaDBBinaryOperation;
import sqlancer.mariadb.ast.MariaDBBinaryOperation.MariaDBBinaryOperator;
import sqlancer.mariadb.ast.MariaDBCastOperation;
import sqlancer.mariadb.ast.MariaDBColumnReference;
import sqlancer.mariadb.ast.MariaDBComputableFunction;
import sqlancer.mariadb.ast.MariaDBComputableFunction.MariaDBFunction;
import sqlancer.mariadb.ast.MariaDBConstant;
import sqlancer.mariadb.ast.MariaDBConstant.MariaDBDoubleConstant;
import sqlancer.mariadb.ast.MariaDBExists;
import sqlancer.mariadb.ast.MariaDBExpression;
import sqlancer.mariadb.ast.MariaDBInOperation;
import sqlancer.mariadb.ast.MariaDBOrderByTerm;
import sqlancer.mariadb.ast.MariaDBOrderByTerm.MariaDBOrder;
import sqlancer.mariadb.ast.MariaDBStringExpression;
import sqlancer.mariadb.ast.MariaDBUnaryPostfixOperation;
import sqlancer.mariadb.ast.MariaDBUnaryPrefixOperation;
import sqlancer.mariadb.ast.MariaDBUnaryPrefixOperation.MariaDBUnaryPrefixOperator;

public class MariaDBExpressionGenerator extends UntypedExpressionGenerator<MariaDBExpression, MariaDBColumn> {

    private final MariaDBGlobalState state;
    private MariaDBRowValue rowVal;

    public MariaDBExpressionGenerator(MariaDBGlobalState state) {
        this.state = state;
    }

    public MariaDBExpressionGenerator setRowVal(MariaDBRowValue rowVal) {
        this.rowVal = rowVal;
        return this;
    }

    private enum Actions {
        COLUMN, LITERAL, UNARY_PREFIX_OPERATION, UNARY_POSTFIX, COMPUTABLE_FUNCTION, BINARY_LOGICAL_OPERATOR,
        BINARY_COMPARISON_OPERATION, CAST, IN_OPERATION, BINARY_OPERATION, EXISTS, BETWEEN_OPERATOR;
    }

    @Override
    public MariaDBExpression generateExpression(int depth) {
        if (depth >= state.getOptions().getMaxExpressionDepth()) {
            return generateLeafNode();
        }
        switch (Randomly.fromOptions(Actions.values())) {
        case COLUMN:
            return generateColumn();
        case LITERAL:
            return generateConstant();
        case UNARY_PREFIX_OPERATION:
            MariaDBExpression subExpr = generateExpression(depth + 1);
            MariaDBUnaryPrefixOperator random = MariaDBUnaryPrefixOperator.getRandom();
            if (random == MariaDBUnaryPrefixOperator.MINUS) {
                // workaround for https://bugs.mariadb.com/bug.php?id=99122
                throw new IgnoreMeException();
            }
            return new MariaDBUnaryPrefixOperation(subExpr, random);
        case UNARY_POSTFIX:
            return new MariaDBUnaryPostfixOperation(generateExpression(depth + 1),
                    Randomly.fromOptions(MariaDBUnaryPostfixOperation.UnaryPostfixOperator.values()),
                    Randomly.getBoolean());
        case COMPUTABLE_FUNCTION:
            return getComputableFunction(depth + 1);
        case BINARY_LOGICAL_OPERATOR:
            return new MariaDBBinaryLogicalOperation(generateExpression(depth + 1), generateExpression(depth + 1),
                    MariaDBBinaryLogicalOperator.getRandom());
        case BINARY_COMPARISON_OPERATION:
            return new MariaDBBinaryComparisonOperation(generateExpression(depth + 1), generateExpression(depth + 1),
                    BinaryComparisonOperator.getRandom());
        case CAST:
            return new MariaDBCastOperation(generateExpression(depth + 1), MariaDBCastOperation.CastType.getRandom());
        case IN_OPERATION:
            MariaDBExpression expr = generateExpression(depth + 1);
            List<MariaDBExpression> rightList = new ArrayList<>();
            for (int i = 0; i < 1 + Randomly.smallNumber(); i++) {
                rightList.add(generateExpression(depth + 1));
            }
            return new MariaDBInOperation(expr, rightList, Randomly.getBoolean());
        case BINARY_OPERATION:
            if (MariaDBBugs.bug99135) {
                throw new IgnoreMeException();
            }
            return new MariaDBBinaryOperation(generateExpression(depth + 1), generateExpression(depth + 1),
                    MariaDBBinaryOperator.getRandom());
        case EXISTS:
            return getExists();
        case BETWEEN_OPERATOR:
            if (MariaDBBugs.bug99181) {
                // TODO: there are a number of bugs that are triggered by the BETWEEN operator
                throw new IgnoreMeException();
            }
            return new MariaDBBetweenOperation(generateExpression(depth + 1), generateExpression(depth + 1),
                    generateExpression(depth + 1));
        default:
            throw new AssertionError();
        }
    }

    private MariaDBExpression getExists() {
        if (Randomly.getBoolean()) {
            return new MariaDBExists(new MariaDBStringExpression("SELECT 1", MariaDBConstant.createTrue()));
        } else {
            return new MariaDBExists(new MariaDBStringExpression("SELECT 1 wHERE FALSE", MariaDBConstant.createFalse()));
        }
    }

    private MariaDBExpression getComputableFunction(int depth) {
        MariaDBFunction func = MariaDBFunction.getRandomFunction();
        int nrArgs = func.getNrArgs();
        if (func.isVariadic()) {
            nrArgs += Randomly.smallNumber();
        }
        MariaDBExpression[] args = new MariaDBExpression[nrArgs];
        for (int i = 0; i < args.length; i++) {
            args[i] = generateExpression(depth + 1);
        }
        return new MariaDBComputableFunction(func, args);
    }

    private enum ConstantType {
        INT, NULL, STRING, DOUBLE;

        public static ConstantType[] valuesPQS() {
            return new ConstantType[] { INT, NULL, STRING };
        }
    }

    @Override
    public MariaDBExpression generateConstant() {
        ConstantType[] values;
        if (state.usesPQS()) {
            values = ConstantType.valuesPQS();
        } else {
            values = ConstantType.values();
        }
        switch (Randomly.fromOptions(values)) {
        case INT:
            return MariaDBConstant.createIntConstant((int) state.getRandomly().getInteger());
        case NULL:
            return MariaDBConstant.createNullConstant();
        case STRING:
            /* Replace characters that still trigger open bugs in MariaDB */
            String string = state.getRandomly().getString().replace("\\", "").replace("\n", "");
            if (string.startsWith("\n")) {
                // workaround for https://bugs.mariadb.com/bug.php?id=99130
                throw new IgnoreMeException();
            }
            if (string.startsWith("-0") || string.startsWith("0.") || string.startsWith(".")) {
                // https://bugs.mariadb.com/bug.php?id=99145
                throw new IgnoreMeException();
            }
            MariaDBConstant createStringConstant = MariaDBConstant.createStringConstant(string);
            // if (Randomly.getBoolean()) {
            // return new MariaDBCollate(createStringConstant,
            // Randomly.fromOptions("ascii_bin", "binary"));
            // }
            if (string.startsWith("1e")) {
                // https://bugs.mariadb.com/bug.php?id=99146
                throw new IgnoreMeException();
            }
            return createStringConstant;
        case DOUBLE:
            double val = state.getRandomly().getDouble();
            if (Math.abs(val) <= 1 && val != 0) {
                // https://bugs.mariadb.com/bug.php?id=99145
                throw new IgnoreMeException();
            }
            if (Math.abs(val) > 1.0E30) {
                // https://bugs.mariadb.com/bug.php?id=99146
                throw new IgnoreMeException();
            }
            return new MariaDBDoubleConstant(val);
        default:
            throw new AssertionError();
        }
    }

    @Override
    public MariaDBExpression generateColumn() {
        MariaDBColumn c = Randomly.fromList(columns);
        MariaDBConstant val;
        if (rowVal == null) {
            val = null;
        } else {
            val = rowVal.getValues().get(c);
        }
        return MariaDBColumnReference.create(c, val);
    }

    @Override
    public MariaDBExpression negatePredicate(MariaDBExpression predicate) {
        return new MariaDBUnaryPrefixOperation(predicate, MariaDBUnaryPrefixOperator.NOT);
    }

    @Override
    public MariaDBExpression isNull(MariaDBExpression expr) {
        return new MariaDBUnaryPostfixOperation(expr, MariaDBUnaryPostfixOperation.UnaryPostfixOperator.IS_NULL, false);
    }

    @Override
    public List<MariaDBExpression> generateOrderBys() {
        List<MariaDBExpression> expressions = super.generateOrderBys();
        List<MariaDBExpression> newOrderBys = new ArrayList<>();
        for (MariaDBExpression expr : expressions) {
            if (Randomly.getBoolean()) {
                MariaDBOrderByTerm newExpr = new MariaDBOrderByTerm(expr, MariaDBOrder.getRandomOrder());
                newOrderBys.add(newExpr);
            } else {
                newOrderBys.add(expr);
            }
        }
        return newOrderBys;
    }

}
