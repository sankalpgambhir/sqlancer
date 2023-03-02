package sqlancer.mariadb.ast;

import java.util.function.BinaryOperator;
import java.util.stream.Stream;

import sqlancer.Randomly;
import sqlancer.mariadb.MariaDBSchema.MariaDBDataType;
import sqlancer.mariadb.ast.MariaDBCastOperation.CastType;

public class MariaDBComputableFunction implements MariaDBExpression {

    private final MariaDBFunction func;
    private final MariaDBExpression[] args;

    public MariaDBComputableFunction(MariaDBFunction func, MariaDBExpression... args) {
        this.func = func;
        this.args = args.clone();
    }

    public MariaDBFunction getFunction() {
        return func;
    }

    public MariaDBExpression[] getArguments() {
        return args.clone();
    }

    public enum MariaDBFunction {

        // ABS(1, "ABS") {
        // @Override
        // public MariaDBConstant apply(MariaDBConstant[] args, MariaDBExpression[] origArgs) {
        // if (args[0].isNull()) {
        // return MariaDBConstant.createNullConstant();
        // }
        // MariaDBConstant intVal = args[0].castAs(CastType.SIGNED);
        // return MariaDBConstant.createIntConstant(Math.abs(intVal.getInt()));
        // }
        // },
        /**
         * @see <a href="https://dev.mariadb.com/doc/refman/8.0/en/bit-functions.html#function_bit-count">Bit Functions
         *      and Operators</a>
         */
        BIT_COUNT(1, "BIT_COUNT") {

            @Override
            public MariaDBConstant apply(MariaDBConstant[] evaluatedArgs, MariaDBExpression... args) {
                MariaDBConstant arg = evaluatedArgs[0];
                if (arg.isNull()) {
                    return MariaDBConstant.createNullConstant();
                } else {
                    long val = arg.castAs(CastType.SIGNED).getInt();
                    return MariaDBConstant.createIntConstant(Long.bitCount(val));
                }
            }

        },
        // BENCHMARK(2, "BENCHMARK") {
        //
        // @Override
        // public MariaDBConstant apply(MariaDBConstant[] evaluatedArgs, MariaDBExpression[] args) {
        // if (evaluatedArgs[0].isNull()) {
        // return MariaDBConstant.createNullConstant();
        // }
        // if (evaluatedArgs[0].castAs(CastType.SIGNED).getInt() < 0) {
        // return MariaDBConstant.createNullConstant();
        // }
        // if (Math.abs(evaluatedArgs[0].castAs(CastType.SIGNED).getInt()) > 10) {
        // throw new IgnoreMeException();
        // }
        // return MariaDBConstant.createIntConstant(0);
        // }
        //
        // },
        COALESCE(2, "COALESCE") {

            @Override
            public MariaDBConstant apply(MariaDBConstant[] args, MariaDBExpression... origArgs) {
                MariaDBConstant result = MariaDBConstant.createNullConstant();
                for (MariaDBConstant arg : args) {
                    if (!arg.isNull()) {
                        result = MariaDBConstant.createStringConstant(arg.castAsString());
                        break;
                    }
                }
                return castToMostGeneralType(result, origArgs);
            }

            @Override
            public boolean isVariadic() {
                return true;
            }

        },
        /**
         * @see <a href="https://dev.mariadb.com/doc/refman/8.0/en/control-flow-functions.html#function_if">Flow Control
         *      Functions</a>
         */
        IF(3, "IF") {

            @Override
            public MariaDBConstant apply(MariaDBConstant[] args, MariaDBExpression... origArgs) {
                MariaDBConstant cond = args[0];
                MariaDBConstant left = args[1];
                MariaDBConstant right = args[2];
                MariaDBConstant result;
                if (cond.isNull() || !cond.asBooleanNotNull()) {
                    result = right;
                } else {
                    result = left;
                }
                return castToMostGeneralType(result, new MariaDBExpression[] { origArgs[1], origArgs[2] });

            }

        },
        /**
         * @see <a href="https://dev.mariadb.com/doc/refman/8.0/en/control-flow-functions.html#function_ifnull">IFNULL</a>
         */
        IFNULL(2, "IFNULL") {

            @Override
            public MariaDBConstant apply(MariaDBConstant[] args, MariaDBExpression... origArgs) {
                MariaDBConstant result;
                if (args[0].isNull()) {
                    result = args[1];
                } else {
                    result = args[0];
                }
                return castToMostGeneralType(result, origArgs);
            }

        },
        LEAST(2, "LEAST", true) {

            @Override
            public MariaDBConstant apply(MariaDBConstant[] evaluatedArgs, MariaDBExpression... args) {
                return aggregate(evaluatedArgs, (min, cur) -> cur.isLessThan(min).asBooleanNotNull() ? cur : min);
            }

        },
        GREATEST(2, "GREATEST", true) {
            @Override
            public MariaDBConstant apply(MariaDBConstant[] evaluatedArgs, MariaDBExpression... args) {
                return aggregate(evaluatedArgs, (max, cur) -> cur.isLessThan(max).asBooleanNotNull() ? max : cur);
            }
        };

        private String functionName;
        final int nrArgs;
        private final boolean variadic;

        private static MariaDBConstant aggregate(MariaDBConstant[] evaluatedArgs, BinaryOperator<MariaDBConstant> op) {
            boolean containsNull = Stream.of(evaluatedArgs).anyMatch(arg -> arg.isNull());
            if (containsNull) {
                return MariaDBConstant.createNullConstant();
            }
            MariaDBConstant least = evaluatedArgs[1];
            for (MariaDBConstant arg : evaluatedArgs) {
                MariaDBConstant left = castToMostGeneralType(least, evaluatedArgs);
                MariaDBConstant right = castToMostGeneralType(arg, evaluatedArgs);
                least = op.apply(right, left);
            }
            return castToMostGeneralType(least, evaluatedArgs);
        }

        MariaDBFunction(int nrArgs, String functionName) {
            this.nrArgs = nrArgs;
            this.functionName = functionName;
            this.variadic = false;
        }

        MariaDBFunction(int nrArgs, String functionName, boolean variadic) {
            this.nrArgs = nrArgs;
            this.functionName = functionName;
            this.variadic = variadic;
        }

        /**
         * Gets the number of arguments if the function is non-variadic. If the function is variadic, the minimum number
         * of arguments is returned.
         *
         * @return the number of arguments
         */
        public int getNrArgs() {
            return nrArgs;
        }

        public abstract MariaDBConstant apply(MariaDBConstant[] evaluatedArgs, MariaDBExpression... args);

        public static MariaDBFunction getRandomFunction() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String toString() {
            return functionName;
        }

        public boolean isVariadic() {
            return variadic;
        }

        public String getName() {
            return functionName;
        }
    }

    @Override
    public MariaDBConstant getExpectedValue() {
        MariaDBConstant[] constants = new MariaDBConstant[args.length];
        for (int i = 0; i < constants.length; i++) {
            constants[i] = args[i].getExpectedValue();
            if (constants[i].getExpectedValue() == null) {
                return null;
            }
        }
        return func.apply(constants, args);
    }

    public static MariaDBConstant castToMostGeneralType(MariaDBConstant cons, MariaDBExpression... typeExpressions) {
        if (cons.isNull()) {
            return cons;
        }
        MariaDBDataType type = getMostGeneralType(typeExpressions);
        switch (type) {
        case INT:
            if (cons.isInt()) {
                return cons;
            } else {
                return MariaDBConstant.createIntConstant(cons.castAs(CastType.SIGNED).getInt());
            }
        case VARCHAR:
            return MariaDBConstant.createStringConstant(cons.castAsString());
        default:
            throw new AssertionError(type);
        }
    }

    public static MariaDBDataType getMostGeneralType(MariaDBExpression... expressions) {
        MariaDBDataType type = null;
        for (MariaDBExpression expr : expressions) {
            MariaDBDataType exprType;
            if (expr instanceof MariaDBColumnReference) {
                exprType = ((MariaDBColumnReference) expr).getColumn().getType();
            } else {
                exprType = expr.getExpectedValue().getType();
            }
            if (type == null) {
                type = exprType;
            } else if (exprType == MariaDBDataType.VARCHAR) {
                type = MariaDBDataType.VARCHAR;
            }

        }
        return type;
    }

}
