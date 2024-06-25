package sqlancer.common.usr.language;

public class SimpleTypes {

    public enum BaseType {
        INT_TYPE,
        BOOL_TYPE,
        STRING_TYPE,
        BIT_TYPE,
        DOUBLE_TYPE,
        NULL_TYPE,
        DATE_TYPE,
        TIME_TYPE
    }

    public static class Null {
        // NullType has a single interpreted value
    }

    public interface RelType {
    }

    public static class Leaf implements RelType {
        private final BaseType type;

        public Leaf(BaseType type) {
            this.type = type;
        }

        public BaseType getType() throws InvalidTypeException {
            return type;
        }
    }

    public static class Node implements RelType {
        private final RelType left;
        private final RelType right;

        public Node(RelType left, RelType right) {
            this.left = left;
            this.right = right;
        }

        public RelType getLeft() {
            return left;
        }

        public RelType getRight() {
            return right;
        }
    }

    public interface Typed {
        RelType getType();
    }

    public static class InvalidTypeException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public InvalidTypeException() {
            super("Invalid Type");
        }
    }
}
