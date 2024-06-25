package sqlancer.common.usr.language;

import java.util.List;
import java.util.stream.Collectors;
import sqlancer.common.usr.language.SimpleTypes.BaseType;
import sqlancer.common.usr.language.SimpleTypes.InvalidTypeException;
import sqlancer.common.usr.language.SimpleTypes.Typed;
import sqlancer.common.usr.language.SimpleTypes.RelType;
import sqlancer.common.usr.language.SimpleTypes.Node;
import sqlancer.common.usr.language.SimpleTypes.Leaf;

public class SQL {

    public interface Relational extends Typed {
    }

    public interface Query extends Relational {
    }

    public static class LabelledRelational implements Relational {
        private final Label label;
        private final Relational relational;

        public LabelledRelational(Label label, Relational relational) {
            if (!label.getType().equals(relational.getType())) {
                throw new IllegalArgumentException("Label type must match relational type");
            }
            this.label = label;
            this.relational = relational;
        }

        @Override
        public RelType getType() {
            return relational.getType();
        }

        public Label getLabel() {
            return label;
        }

        public Relational getRelational() {
            return relational;
        }
    }

    public static class Select implements Query {
        private final List<Selector> selectors;
        private final List<LabelledRelational> from;
        private final Predicate where;

        public Select(List<Selector> selectors, List<LabelledRelational> from, Predicate where) {
            this.selectors = selectors;
            this.from = from;
            this.where = where;
        }

        @Override
        public RelType getType() {
            return selectors.stream().map(Selector::getType).reduce((a, b) -> new Node(a, b)).orElseThrow();
        }

        public List<Selector> getSelectors() {
            return selectors;
        }

        public List<LabelledRelational> getFrom() {
            return from;
        }

        public Predicate getWhere() {
            return where;
        }
    }

    public static class Union implements Query {
        private final Query left;
        private final Query right;

        public Union(Query left, Query right) {
            if (!left.getType().equals(right.getType())) {
                throw new IllegalArgumentException("Types must match");
            }
            this.left = left;
            this.right = right;
        }

        @Override
        public RelType getType() {
            return left.getType();
        }

        public Query getLeft() {
            return left;
        }

        public Query getRight() {
            return right;
        }
    }

    public static class Except implements Query {
        private final Query left;
        private final Query right;

        public Except(Query left, Query right) {
            if (!left.getType().equals(right.getType())) {
                throw new IllegalArgumentException("Types must match");
            }
            this.left = left;
            this.right = right;
        }

        @Override
        public RelType getType() {
            return left.getType();
        }

        public Query getLeft() {
            return left;
        }

        public Query getRight() {
            return right;
        }
    }

    public static class Join implements Query {
        private final Query left;
        private final Query right;
        private final Predicate on;

        public Join(Query left, Query right, Predicate on) {
            this.left = left;
            this.right = right;
            this.on = on;
        }

        @Override
        public RelType getType() {
            return new Node(left.getType(), right.getType());
        }

        public Query getLeft() {
            return left;
        }

        public Query getRight() {
            return right;
        }

        public Predicate getPredicate() {
            return on;
        }
    }

    public static class Distinct implements Query {
        private final Query inner;

        public Distinct(Query inner) {
            this.inner = inner;
        }

        @Override
        public RelType getType() {
            return inner.getType();
        }

        public Query getInner() {
            return inner;
        }
    }

    public interface Selector extends Typed {
    }

    public interface Projection extends Selector {
    }

    public static class LeftProjection implements Projection {
        private final Projection inner;

        public LeftProjection(Projection inner) {
            this.inner = inner;
        }

        @Override
        public RelType getType() throws InvalidTypeException {
            RelType tpe = inner.getType();
            if (tpe instanceof Node) {
                return ((Node) tpe).getLeft();
            } else {
                throw new InvalidTypeException();
            }
        }

        public Projection getInner() {
            return inner;
        }
    }

    public static class RightProjection implements Projection {
        private final Projection inner;

        public RightProjection(Projection inner) {
            this.inner = inner;
        }

        @Override
        public RelType getType() {
            RelType tpe = inner.getType();
            if (tpe instanceof Node) {
                return ((Node) tpe).getRight();
            } else {
                throw new InvalidTypeException();
            }
        }

        public Projection getInner() {
            return inner;
        }
    }

    public static class TableProjection implements Projection {
        private final Table table;

        public TableProjection(Table table) {
            this.table = table;
        }

        @Override
        public RelType getType() {
            return table.getType();
        }

        public Table getTable() {
            return table;
        }
    }

    public static class ConstantSelector implements Selector {
        private final BoxedConstant value;

        public ConstantSelector(BoxedConstant value) {
            this.value = value;
        }

        @Override
        public RelType getType() {
            return value.getType();
        }

        public BoxedConstant getValue() {
            return value;
        }
    }

    public static class FunctionalSelector implements Selector {
        private final Functional fun;
        private final List<Selector> args;

        public FunctionalSelector(Functional fun, List<Selector> args) {
            if (!fun.getInputTypes().equals(args.stream().map(Selector::getType).collect(Collectors.toList()))) {
                throw new IllegalArgumentException("Function input types must match argument types");
            }
            this.fun = fun;
            this.args = args;
        }

        @Override
        public RelType getType() {
            return fun.getOutputType();
        }

        public Functional getFun() {
            return fun;
        }

        public List<Selector> getArgs() {
            return args;
        }
    }

    public static class Table implements Relational {
        private final String name;
        private final RelType type;

        public Table(String name, RelType type) {
            this.name = name;
            this.type = type;
        }

        @Override
        public RelType getType() {
            return type;
        }

        public String getName() {
            return name;
        }
    }

    public static class BoxedConstant implements Typed {
        private final Object value;
        private final BaseType constType;

        public BoxedConstant(Object value, BaseType constType) {
            this.value = value;
            this.constType = constType;
        }

        @Override
        public RelType getType() {
            return new Leaf(constType);
        }
        
        public BaseType getConstType() {
            return constType;
        }

        public Object getValue() {
            return value;
        }
    }

    public static class Label implements Typed {
        private final String name;
        private final RelType type;

        public Label(String name, RelType type) {
            this.name = name;
            this.type = type;
        }

        @Override
        public RelType getType() {
            return type;
        }

        public String getName() {
            return name;
        }
    }

    public static class Functional {
        private final String name;
        private final List<RelType> inputTypes;
        private final RelType outputType;

        public Functional(String name, List<RelType> inputTypes, RelType outputType) {
            this.name = name;
            this.inputTypes = inputTypes;
            this.outputType = outputType;
        }

        public String getName() {
            return name;
        }

        public List<RelType> getInputTypes() {
            return inputTypes;
        }

        public RelType getOutputType() {
            return outputType;
        }
    }

    public interface Predicate {
    }

    public static class True implements Predicate {
    }

    public static class False implements Predicate {
    }

    public static class And implements Predicate {
        private final Predicate left;
        private final Predicate right;

        public And(Predicate left, Predicate right) {
            this.left = left;
            this.right = right;
        }

        public Predicate getLeft() {
            return left;
        }

        public Predicate getRight() {
            return right;
        }
    }

    public static class Or implements Predicate {
        private final Predicate left;
        private final Predicate right;

        public Or(Predicate left, Predicate right) {
            this.left = left;
            this.right = right;
        }

        public Predicate getLeft() {
            return left;
        }

        public Predicate getRight() {
            return right;
        }
    }

    public static class Not implements Predicate {
        private final Predicate inner;

        public Not(Predicate inner) {
            this.inner = inner;
        }

        public Predicate getInner() {
            return inner;
        }
    }

    public static class Eq implements Predicate {
        private final Selector left;
        private final Selector right;

        public Eq(Selector left, Selector right) {
            this.left = left;
            this.right = right;
        }

        public Selector getLeft() {
            return left;
        }

        public Selector getRight() {
            return right;
        }
    }

    public static class Gt implements Predicate {
        private final Selector left;
        private final Selector right;

        public Gt(Selector left, Selector right) {
            this.left = left;
            this.right = right;
        }

        public Selector getLeft() {
            return left;
        }

        public Selector getRight() {
            return right;
        }
    }

    public static class Lt implements Predicate {
        private final Selector left;
        private final Selector right;

        public Lt(Selector left, Selector right) {
            this.left = left;
            this.right = right;
        }

        public Selector getLeft() {
            return left;
        }

        public Selector getRight() {
            return right;
        }
    }

    public static class Uninterpreted implements Predicate {
        private final String name;
        private final List<Selector> args;

        public Uninterpreted(String name, List<Selector> args) {
            this.name = name;
            this.args = args;
        }

        public String getName() {
            return name;
        }

        public List<Selector> getArgs() {
            return args;
        }
    }

    public static String prettyPrint(Query query) {
        return prettyPrint(query, false);
    }

    public static String prettyPrint(Query query, boolean distinct) {
        if (query instanceof Select) {
            Select select = (Select) query;
            return String.format("SELECT %s%s FROM %s WHERE %s",
                    distinct ? "DISTINCT " : "",
                    select.selectors.stream().map(SQL::prettyPrintSelector).collect(Collectors.joining(", ")),
                    select.from.stream().map(SQL::prettyPrintLabelled).collect(Collectors.joining(", ")),
                    prettyPrintPred(select.where));
        } else if (query instanceof Union) {
            Union union = (Union) query;
            return String.format("%s UNION %s", prettyPrint(union.left), prettyPrint(union.right));
        } else if (query instanceof Except) {
            Except except = (Except) query;
            return String.format("%s EXCEPT %s", prettyPrint(except.left), prettyPrint(except.right));
        } else if (query instanceof Join) {
            Join join = (Join) query;
            return String.format("%s JOIN %s ON %s", prettyPrint(join.left), prettyPrint(join.right),
                    prettyPrintPred(join.on));
        } else if (query instanceof Distinct) {
            Distinct distinctQuery = (Distinct) query;
            return prettyPrint(distinctQuery.inner, true);
        } else {
            throw new IllegalArgumentException("Unknown query type");
        }
    }

    private static String prettyPrintSelector(Selector selector) {
        if (selector instanceof LeftProjection) {
            return prettyPrintSelector(((LeftProjection) selector).inner) + ".left";
        } else if (selector instanceof RightProjection) {
            return prettyPrintSelector(((RightProjection) selector).inner) + ".right";
        } else if (selector instanceof TableProjection) {
            return ((TableProjection) selector).table.getName();
        } else if (selector instanceof ConstantSelector) {
            return ((ConstantSelector) selector).value.toString();
        } else if (selector instanceof FunctionalSelector) {
            FunctionalSelector funcSel = (FunctionalSelector) selector;
            return String.format("%s(%s)", funcSel.fun.getName(),
                    funcSel.args.stream().map(SQL::prettyPrintSelector).collect(Collectors.joining(", ")));
        } else {
            throw new IllegalArgumentException("Unknown selector type");
        }
    }

    private static String prettyPrintLabelled(LabelledRelational labelled) {
        String rel;
        if (labelled.relational instanceof LabelledRelational) {
            throw new UnsupportedOperationException();
        } else if (labelled.relational instanceof Table) {
            rel = ((Table) labelled.relational).getName();
        } else if (labelled.relational instanceof Query) {
            rel = prettyPrint((Query) labelled.relational);
        } else {
            throw new IllegalArgumentException("Unknown relational type");
        }
        return String.format("(%s AS %s)", rel, labelled.label.getName());
    }

    private static String prettyPrintPred(Predicate pred) {
        if (pred instanceof True) {
            return "TRUE";
        } else if (pred instanceof False) {
            return "FALSE";
        } else if (pred instanceof And) {
            And and = (And) pred;
            return String.format("(%s AND %s)", prettyPrintPred(and.left), prettyPrintPred(and.right));
        } else if (pred instanceof Or) {
            Or or = (Or) pred;
            return String.format("(%s OR %s)", prettyPrintPred(or.left), prettyPrintPred(or.right));
        } else if (pred instanceof Not) {
            Not not = (Not) pred;
            return String.format("(NOT %s)", prettyPrintPred(not.inner));
        } else if (pred instanceof Eq) {
            Eq eq = (Eq) pred;
            return String.format("(%s = %s)", prettyPrintSelector(eq.left), prettyPrintSelector(eq.right));
        } else if (pred instanceof Gt) {
            Gt gt = (Gt) pred;
            return String.format("(%s > %s)", prettyPrintSelector(gt.left), prettyPrintSelector(gt.right));
        } else if (pred instanceof Lt) {
            Lt lt = (Lt) pred;
            return String.format("(%s < %s)", prettyPrintSelector(lt.left), prettyPrintSelector(lt.right));
        } else if (pred instanceof Uninterpreted) {
            Uninterpreted uninterpreted = (Uninterpreted) pred;
            return String.format("%s(%s)", uninterpreted.name,
                    uninterpreted.args.stream().map(SQL::prettyPrintSelector).collect(Collectors.joining(", ")));
        } else {
            throw new IllegalArgumentException("Unknown predicate type");
        }
    }
}
