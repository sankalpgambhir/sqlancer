package sqlancer.common.usr.language;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import sqlancer.common.usr.language.SimpleTypes.RelType;
import sqlancer.common.usr.interfaces.Equatable;
import sqlancer.common.usr.language.SimpleTypes.BaseType;
import sqlancer.common.usr.language.SimpleTypes.Typed;
import sqlancer.common.usr.language.SimpleTypes.Leaf;
import sqlancer.common.usr.language.SimpleTypes.Node;
import sqlancer.common.usr.language.SimpleTypes.InvalidTypeException;

public class USR {

    public static class Label {
        private final String name;
        private final RelType type;

        public Label(String name, RelType type) {
            this.name = name;
            this.type = type;
        }

        public String getName() {
            return name;
        }

        public RelType getType() {
            return type;
        }
    }

    public interface Expr extends Equatable {
        List<Expr> getChildren();

        Expr substituted(Map<Var, Expr> subst);
    }

    public static class One implements Expr {
        public List<Expr> getChildren() {
            return List.of();
        }

        public Expr substituted(Map<Var, Expr> subst) {
            return this;
        }

        public boolean extEquals(Object obj) {
            return obj instanceof One;
        }
    }

    public static class Zero implements Expr {
        public List<Expr> getChildren() {
            return List.of();
        }

        public Expr substituted(Map<Var, Expr> subst) {
            return this;
        }

        public boolean extEquals(Object obj) {
            return obj instanceof Zero;
        }
    }

    public interface Projection extends Expr, Typed {
    }

    public static class Var implements Projection {
        private final String name;
        private final RelType type;

        public Var(String name, RelType type) {
            this.name = name;
            this.type = type;
        }

        public List<Expr> getChildren() {
            return List.of();
        }

        public RelType getType() {
            return type;
        }

        public String getName() {
            return name;
        }

        public Expr substituted(Map<Var, Expr> subst) {
            return subst.getOrDefault(this, null);
        }

        public boolean extEquals(Object obj) {
            if (obj instanceof Var) {
                Var other = (Var) obj;
                return other.getName().equals(getName()) && other.getType().equals(getType());
            }
            return false;
        }
    }

    public static class BoxedConst implements Expr, Typed {
        private final Object value;
        private final BaseType constType;

        public BoxedConst(Object value, BaseType constType) {
            this.value = value;
            this.constType = constType;
        }

        public List<Expr> getChildren() {
            return List.of();
        }

        public RelType getType() {
            return new Leaf(constType);
        }

        public BaseType getConstType() {
            return constType;
        }

        public Object getValue() {
            return value;
        }

        public Expr substituted(Map<Var, Expr> subst) {
            return this;
        }

        public boolean extEquals(Object obj) {
            if (obj instanceof BoxedConst) {
                BoxedConst other = (BoxedConst) obj;
                return other.getValue().equals(getValue()) && other.getConstType().equals(getConstType());
            }
            return false;
        }
    }

    public static class Left implements Projection {
        private final Projection inner;

        public Left(Projection inner) {
            this.inner = inner;
        }

        public Expr getInner() {
            return inner;
        }

        public List<Expr> getChildren() {
            return List.of(inner);
        }

        public RelType getType() {
            RelType type = inner.getType();
            if (type instanceof Node) {
                return ((Node) type).getLeft();
            } else {
                throw new InvalidTypeException();
            }
        }

        public Expr substituted(Map<Var, Expr> subst) {
            return new Left((Projection) inner.substituted(subst));
        }

        public boolean extEquals(Object obj) {
            if (obj instanceof Left) {
                Left other = (Left) obj;
                return other.inner.extEquals(inner);
            }
            return false;
        }
    }

    public static class Right implements Projection {
        private final Projection inner;

        public Right(Projection inner) {
            this.inner = inner;
        }

        public Expr getInner() {
            return inner;
        }

        public List<Expr> getChildren() {
            return List.of(inner);
        }

        public RelType getType() {
            RelType type = inner.getType();
            if (type instanceof Node) {
                return ((Node) type).getRight();
            } else {
                throw new InvalidTypeException();
            }
        }

        public Expr substituted(Map<Var, Expr> subst) {
            return new Right((Projection) inner.substituted(subst));
        }

        public boolean extEquals(Object obj) {
            if (obj instanceof Right) {
                Right other = (Right) obj;
                return other.inner.extEquals(inner);
            }
            return false;
        }
    }

    public static class Mul implements Expr {
        private final Expr left;
        private final Expr right;

        public Mul(Expr left, Expr right) {
            this.left = left;
            this.right = right;
        }

        public Expr getLeft() {
            return left;
        }

        public Expr getRight() {
            return right;
        }

        public List<Expr> getChildren() {
            return List.of(left, right);
        }

        public Expr substituted(Map<Var, Expr> subst) {
            return new Mul(left.substituted(subst), right.substituted(subst));
        }

        public boolean extEquals(Object obj) {
            if (obj instanceof Mul) {
                Mul other = (Mul) obj;
                return other.left.extEquals(left) && other.right.extEquals(right);
            }
            return false;
        }
    }

    public static class Add implements Expr {
        private final Expr left;
        private final Expr right;

        public Add(Expr left, Expr right) {
            this.left = left;
            this.right = right;
        }

        public Expr getLeft() {
            return left;
        }

        public Expr getRight() {
            return right;
        }

        public List<Expr> getChildren() {
            return List.of(left, right);
        }

        public Expr substituted(Map<Var, Expr> subst) {
            return new Add(left.substituted(subst), right.substituted(subst));
        }

        public boolean extEquals(Object obj) {
            if (obj instanceof Add) {
                Add other = (Add) obj;
                return other.left.extEquals(left) && other.right.extEquals(right);
            }
            return false;
        }
    }

    public static class Not implements Expr {
        private final Expr inner;

        public Not(Expr inner) {
            this.inner = inner;
        }

        public Expr getInner() {
            return inner;
        }

        public List<Expr> getChildren() {
            return List.of(inner);
        }

        public Expr substituted(Map<Var, Expr> subst) {
            return new Not(inner.substituted(subst));
        }

        public boolean extEquals(Object obj) {
            if (obj instanceof Not) {
                Not other = (Not) obj;
                return other.inner.extEquals(inner);
            }
            return false;
        }
    }

    public static class Squash implements Expr {
        private final Expr inner;

        public Squash(Expr inner) {
            this.inner = inner;
        }

        public Expr getInner() {
            return inner;
        }

        public List<Expr> getChildren() {
            return List.of(inner);
        }

        public Expr substituted(Map<Var, Expr> subst) {
            return new Squash(inner.substituted(subst));
        }

        public boolean extEquals(Object obj) {
            if (obj instanceof Squash) {
                Squash other = (Squash) obj;
                return other.inner.extEquals(inner);
            }
            return false;
        }
    }

    public static class USum implements Expr {
        private final Var variable;
        private final Expr inner;

        public USum(Var variable, Expr inner) {
            this.variable = variable;
            this.inner = inner;
        }

        public Var getVariable() {
            return variable;
        }

        public Expr getInner() {
            return inner;
        }

        public List<Expr> getChildren() {
            return List.of(variable, inner);
        }

        public Expr substituted(Map<Var, Expr> subst) {
            Map<Var, Expr> newSubst = Map.copyOf(subst);
            newSubst.remove(variable);
            return new USum(variable, inner.substituted(newSubst));
        }

        public boolean extEquals(Object obj) {
            if (obj instanceof USum) {
                USum other = (USum) obj;
                return other.variable.extEquals(variable) && other.inner.extEquals(inner);
            }
            return false;
        }
    }

    public static class Lambda implements Expr {
        private final Var variable;
        private final Expr inner;

        public Lambda(Var variable, Expr inner) {
            this.variable = variable;
            this.inner = inner;
        }

        public Var getVariable() {
            return variable;
        }

        public Expr getInner() {
            return inner;
        }

        public Expr eval(Expr arg) {
            return inner.substituted(Map.of(variable, arg));
        }

        public App applied(Expr arg) {
            return new App(this, arg);
        }

        public List<Expr> getChildren() {
            return List.of(variable, inner);
        }

        public Expr substituted(Map<Var, Expr> subst) {
            Map<Var, Expr> newSubst = Map.copyOf(subst);
            newSubst.remove(variable);
            return new USum(variable, inner.substituted(newSubst));
        }

        public boolean extEquals(Object obj) {
            if (obj instanceof Lambda) {
                Lambda other = (Lambda) obj;
                return other.variable.extEquals(variable) && other.inner.extEquals(inner);
            }
            return false;
        }
    }

    public static class App implements Expr {
        private final Lambda fun;
        private final Expr arg;

        public App(Lambda fun, Expr arg) {
            this.fun = fun;
            this.arg = arg;
        }

        public Lambda getFun() {
            return fun;
        }

        public Expr getArg() {
            return arg;
        }

        public Expr eval() {
            return fun.eval(arg);
        }

        public List<Expr> getChildren() {
            return List.of(fun, arg);
        }

        public Expr substituted(Map<Var, Expr> subst) {
            return new App((Lambda) fun.substituted(subst), arg.substituted(subst));
        }

        public boolean extEquals(Object obj) {
            if (obj instanceof App) {
                App other = (App) obj;
                return other.fun.extEquals(fun) && other.arg.extEquals(arg);
            }
            return false;
        }
    }

    public static class Relation implements Expr {
        private final String name;
        private final RelType type;
        private final Var arg;

        public Relation(String name, RelType type, Var arg) {
            this.name = name;
            this.type = type;
            this.arg = arg;
        }

        public Var getArg() {
            return arg;
        }

        public List<Expr> getChildren() {
            return List.of(arg);
        }

        public String getName() {
            return name;
        }

        public RelType getType() {
            return type;
        }

        public Expr substituted(Map<Var, Expr> subst) {
            return new Relation(name, type, (Var) arg.substituted(subst));
        }

        public boolean extEquals(Object obj) {
            if (obj instanceof Relation) {
                Relation other = (Relation) obj;
                return other.name.equals(name) && other.type.equals(type) && other.arg.extEquals(arg);
            }
            return false;
        }
    }

    public static class Predicate implements Expr {
        private final String name;
        private final List<Expr> args;

        public Predicate(String name, List<Expr> args) {
            this.name = name;
            this.args = args;
        }

        public List<Expr> getChildren() {
            return args;
        }

        public String getName() {
            return name;
        }

        public Expr substituted(Map<Var, Expr> subst) {
            return new Predicate(name, args.stream().map(e -> e.substituted(subst)).collect(Collectors.toList()));
        }

        public boolean extEquals(Object obj) {
            if (obj instanceof Predicate) {
                Predicate other = (Predicate) obj;
                return other.name.equals(name) && other.args.equals(args);
            }
            return false;
        }
    }

    public static class Function implements Expr {
        private final String name;
        private final List<Expr> args;
        private final RelType type;

        public Function(String name, List<Expr> args, RelType type) {
            this.name = name;
            this.args = args;
            this.type = type;
        }

        public List<Expr> getChildren() {
            return args;
        }

        public String getName() {
            return name;
        }

        public RelType getType() {
            return type;
        }

        public Expr substituted(Map<Var, Expr> subst) {
            return new Function(name, args.stream().map(e -> e.substituted(subst)).collect(Collectors.toList()), type);
        }

        public boolean extEquals(Object obj) {
            if (obj instanceof Function) {
                Function other = (Function) obj;
                return other.name.equals(name) && other.args.equals(args) && other.type.equals(type);
            }
            return false;
        }
    }

    public interface NamedPredicate {
        String getName();
    }

    public static class Equal extends Predicate implements NamedPredicate {
        private static final String name = "=";
        private final Expr left;
        private final Expr right;

        public Equal(Expr left, Expr right) {
            super(name, List.of(left, right));
            this.left = left;
            this.right = right;
        }

        public Expr getLeft() {
            return left;
        }

        public Expr getRight() {
            return right;
        }

        public String getName() {
            return name;
        }
    }

    public static class Gt extends Predicate implements NamedPredicate {
        private static final String name = ">";
        private final Expr left;
        private final Expr right;

        public Gt(Expr left, Expr right) {
            super(name, List.of(left, right));
            this.left = left;
            this.right = right;
        }

        public Expr getLeft() {
            return left;
        }

        public Expr getRight() {
            return right;
        }

        public String getName() {
            return name;
        }
    }

    public static class Lt extends Predicate implements NamedPredicate {
        private static final String name = "<";
        private final Expr left;
        private final Expr right;

        public Lt(Expr left, Expr right) {
            super(name, List.of(left, right));
            this.left = left;
            this.right = right;
        }

        public Expr getLeft() {
            return left;
        }

        public Expr getRight() {
            return right;
        }

        public String getName() {
            return name;
        }
    }

    public static Expr add(Expr e1, Expr e2) {
        return new Add(e1, e2);
    }

    public static Expr mul(Expr e1, Expr e2) {
        return new Mul(e1, e2);
    }

    public static Expr not(Expr e) {
        return new Not(e);
    }

    public static Expr equal(Expr e1, Expr e2) {
        return new Equal(e1, e2);
    }

    public static Expr gt(Expr e1, Expr e2) {
        return new Gt(e1, e2);
    }

    public static Expr lt(Expr e1, Expr e2) {
        return new Lt(e1, e2);
    }
}
