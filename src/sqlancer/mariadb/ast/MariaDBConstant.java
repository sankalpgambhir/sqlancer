package sqlancer.mariadb.ast;

import java.math.BigInteger;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.mariadb.MariaDBSchema.MariaDBDataType;
import sqlancer.mariadb.ast.MariaDBCastOperation.CastType;

public abstract class MariaDBConstant implements MariaDBExpression {

    public boolean isInt() {
        return false;
    }

    public boolean isNull() {
        return false;
    }

    public abstract static class MariaDBNoPQSConstant extends MariaDBConstant {

        @Override
        public boolean asBooleanNotNull() {
            throw throwException();
        }

        private RuntimeException throwException() {
            throw new UnsupportedOperationException("not applicable for PQS evaluation!");
        }

        @Override
        public MariaDBConstant isEquals(MariaDBConstant rightVal) {
            return null;
        }

        @Override
        public MariaDBConstant castAs(CastType type) {
            throw throwException();
        }

        @Override
        public String castAsString() {
            throw throwException();

        }

        @Override
        public MariaDBDataType getType() {
            throw throwException();
        }

        @Override
        protected MariaDBConstant isLessThan(MariaDBConstant rightVal) {
            throw throwException();
        }

    }

    public static class MariaDBDoubleConstant extends MariaDBNoPQSConstant {

        private final double val;

        public MariaDBDoubleConstant(double val) {
            this.val = val;
            if (Double.isInfinite(val) || Double.isNaN(val)) {
                // seems to not be supported by MariaDB
                throw new IgnoreMeException();
            }
        }

        @Override
        public String getTextRepresentation() {
            return String.valueOf(val);
        }

    }

    public static class MariaDBTextConstant extends MariaDBConstant {

        private final String value;
        private final boolean singleQuotes;

        public MariaDBTextConstant(String value) {
            this.value = value;
            singleQuotes = Randomly.getBoolean();

        }

        private void checkIfSmallFloatingPointText() {
            boolean isSmallFloatingPointText = isString() && asBooleanNotNull()
                    && castAs(CastType.SIGNED).getInt() == 0;
            if (isSmallFloatingPointText) {
                throw new IgnoreMeException();
            }
        }

        @Override
        public boolean asBooleanNotNull() {
            // TODO implement as cast
            for (int i = value.length(); i >= 0; i--) {
                try {
                    String substring = value.substring(0, i);
                    Double val = Double.valueOf(substring);
                    return val != 0 && !Double.isNaN(val);
                } catch (NumberFormatException e) {
                    // ignore
                }
            }
            return false;
            // return castAs(CastType.SIGNED).getInt() != 0;
        }

        @Override
        public String getTextRepresentation() {
            StringBuilder sb = new StringBuilder();
            String quotes = singleQuotes ? "'" : "\"";
            sb.append(quotes);
            String text = value.replace(quotes, quotes + quotes).replace("\\", "\\\\");
            sb.append(text);
            sb.append(quotes);
            return sb.toString();
        }

        @Override
        public MariaDBConstant isEquals(MariaDBConstant rightVal) {
            if (rightVal.isNull()) {
                return MariaDBConstant.createNullConstant();
            } else if (rightVal.isInt()) {
                checkIfSmallFloatingPointText();
                if (asBooleanNotNull()) {
                    // TODO support SELECT .123 = '.123'; by converting to floating point
                    throw new IgnoreMeException();
                }
                return castAs(CastType.SIGNED).isEquals(rightVal);
            } else if (rightVal.isString()) {
                return MariaDBConstant.createBoolean(value.equalsIgnoreCase(rightVal.getString()));
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        public String getString() {
            return value;
        }

        @Override
        public boolean isString() {
            return true;
        }

        @Override
        public MariaDBConstant castAs(CastType type) {
            if (type == CastType.SIGNED || type == CastType.UNSIGNED) {
                String value = this.value;
                while (value.startsWith(" ") || value.startsWith("\t") || value.startsWith("\n")) {
                    if (value.startsWith("\n")) {
                        /* workaround for https://bugs.mariadb.com/bug.php?id=96294 */
                        throw new IgnoreMeException();
                    }
                    value = value.substring(1);
                }
                for (int i = value.length(); i >= 0; i--) {
                    try {
                        String substring = value.substring(0, i);
                        long val = Long.parseLong(substring);
                        return MariaDBConstant.createIntConstant(val, type == CastType.SIGNED);
                    } catch (NumberFormatException e) {
                        // ignore
                    }
                }
                return MariaDBConstant.createIntConstant(0, type == CastType.SIGNED);
            } else {
                throw new AssertionError();
            }
        }

        @Override
        public String castAsString() {
            return value;
        }

        @Override
        public MariaDBDataType getType() {
            return MariaDBDataType.VARCHAR;
        }

        @Override
        protected MariaDBConstant isLessThan(MariaDBConstant rightVal) {
            if (rightVal.isNull()) {
                return MariaDBConstant.createNullConstant();
            } else if (rightVal.isInt()) {
                if (asBooleanNotNull()) {
                    // TODO uspport floating point
                    throw new IgnoreMeException();
                }
                checkIfSmallFloatingPointText();
                return castAs(rightVal.isSigned() ? CastType.SIGNED : CastType.UNSIGNED).isLessThan(rightVal);
            } else if (rightVal.isString()) {
                // unexpected result for '-' < "!";
                // return
                // MariaDBConstant.createBoolean(value.compareToIgnoreCase(rightVal.getString()) <
                // 0);
                throw new IgnoreMeException();
            } else {
                throw new AssertionError(rightVal);
            }
        }

    }

    public static class MariaDBIntConstant extends MariaDBConstant {

        private final long value;
        private final String stringRepresentation;
        private final boolean isSigned;

        public MariaDBIntConstant(long value, boolean isSigned) {
            this.value = value;
            this.isSigned = isSigned;
            if (isSigned) {
                stringRepresentation = String.valueOf(value);
            } else {
                stringRepresentation = Long.toUnsignedString(value);
            }
        }

        public MariaDBIntConstant(long value, String stringRepresentation) {
            this.value = value;
            this.stringRepresentation = stringRepresentation;
            isSigned = true;
        }

        @Override
        public boolean isInt() {
            return true;
        }

        @Override
        public long getInt() {
            return value;
        }

        @Override
        public boolean asBooleanNotNull() {
            return value != 0;
        }

        @Override
        public String getTextRepresentation() {
            return stringRepresentation;
        }

        @Override
        public MariaDBConstant isEquals(MariaDBConstant rightVal) {
            if (rightVal.isInt()) {
                return MariaDBConstant.createBoolean(new BigInteger(getStringRepr())
                        .compareTo(new BigInteger(((MariaDBIntConstant) rightVal).getStringRepr())) == 0);
            } else if (rightVal.isNull()) {
                return MariaDBConstant.createNullConstant();
            } else if (rightVal.isString()) {
                if (rightVal.asBooleanNotNull()) {
                    // TODO support SELECT .123 = '.123'; by converting to floating point
                    throw new IgnoreMeException();
                }
                return isEquals(rightVal.castAs(CastType.SIGNED));
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        public MariaDBConstant castAs(CastType type) {
            if (type == CastType.SIGNED) {
                return new MariaDBIntConstant(value, true);
            } else if (type == CastType.UNSIGNED) {
                return new MariaDBIntConstant(value, false);
            } else {
                throw new AssertionError();
            }
        }

        @Override
        public String castAsString() {
            if (isSigned) {
                return String.valueOf(value);
            } else {
                return Long.toUnsignedString(value);
            }
        }

        @Override
        public MariaDBDataType getType() {
            return MariaDBDataType.INT;
        }

        @Override
        public boolean isSigned() {
            return isSigned;
        }

        private String getStringRepr() {
            if (isSigned) {
                return String.valueOf(value);
            } else {
                return Long.toUnsignedString(value);
            }
        }

        @Override
        protected MariaDBConstant isLessThan(MariaDBConstant rightVal) {
            if (rightVal.isInt()) {
                long intVal = rightVal.getInt();
                if (isSigned && rightVal.isSigned()) {
                    return MariaDBConstant.createBoolean(value < intVal);
                } else {
                    return MariaDBConstant.createBoolean(new BigInteger(getStringRepr())
                            .compareTo(new BigInteger(((MariaDBIntConstant) rightVal).getStringRepr())) < 0);
                    // return MariaDBConstant.createBoolean(Long.compareUnsigned(value, intVal) < 0);
                }
            } else if (rightVal.isNull()) {
                return MariaDBConstant.createNullConstant();
            } else if (rightVal.isString()) {
                if (rightVal.asBooleanNotNull()) {
                    // TODO support float
                    throw new IgnoreMeException();
                }
                return isLessThan(rightVal.castAs(isSigned ? CastType.SIGNED : CastType.UNSIGNED));
            } else {
                throw new AssertionError(rightVal);
            }
        }

    }

    public static class MariaDBNullConstant extends MariaDBConstant {

        @Override
        public boolean isNull() {
            return true;
        }

        @Override
        public boolean asBooleanNotNull() {
            throw new UnsupportedOperationException(this.toString());
        }

        @Override
        public String getTextRepresentation() {
            return "NULL";
        }

        @Override
        public MariaDBConstant isEquals(MariaDBConstant rightVal) {
            return MariaDBConstant.createNullConstant();
        }

        @Override
        public MariaDBConstant castAs(CastType type) {
            return this;
        }

        @Override
        public String castAsString() {
            return "NULL";
        }

        @Override
        public MariaDBDataType getType() {
            return null;
        }

        @Override
        protected MariaDBConstant isLessThan(MariaDBConstant rightVal) {
            return this;
        }

    }

    public long getInt() {
        throw new UnsupportedOperationException();
    }

    public boolean isSigned() {
        return false;
    }

    public String getString() {
        throw new UnsupportedOperationException();
    }

    public boolean isString() {
        return false;
    }

    public static MariaDBConstant createNullConstant() {
        return new MariaDBNullConstant();
    }

    public static MariaDBConstant createIntConstant(long value) {
        return new MariaDBIntConstant(value, true);
    }

    public static MariaDBConstant createIntConstant(long value, boolean signed) {
        return new MariaDBIntConstant(value, signed);
    }

    public static MariaDBConstant createUnsignedIntConstant(long value) {
        return new MariaDBIntConstant(value, false);
    }

    public static MariaDBConstant createIntConstantNotAsBoolean(long value) {
        return new MariaDBIntConstant(value, String.valueOf(value));
    }

    @Override
    public MariaDBConstant getExpectedValue() {
        return this;
    }

    public abstract boolean asBooleanNotNull();

    public abstract String getTextRepresentation();

    public static MariaDBConstant createFalse() {
        return MariaDBConstant.createIntConstant(0);
    }

    public static MariaDBConstant createBoolean(boolean isTrue) {
        return MariaDBConstant.createIntConstant(isTrue ? 1 : 0);
    }

    public static MariaDBConstant createTrue() {
        return MariaDBConstant.createIntConstant(1);
    }

    @Override
    public String toString() {
        return getTextRepresentation();
    }

    public abstract MariaDBConstant isEquals(MariaDBConstant rightVal);

    public abstract MariaDBConstant castAs(CastType type);

    public abstract String castAsString();

    public static MariaDBConstant createStringConstant(String string) {
        return new MariaDBTextConstant(string);
    }

    public abstract MariaDBDataType getType();

    protected abstract MariaDBConstant isLessThan(MariaDBConstant rightVal);

}
