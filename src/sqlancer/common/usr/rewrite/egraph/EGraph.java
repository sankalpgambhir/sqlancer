package sqlancer.common.usr.rewrite.egraph;

import sqlancer.common.usr.language.USR.*;
import sqlancer.common.usr.language.SimpleTypes.*;

import sqlancer.common.usr.interfaces.Equatable;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class EGraph {

    private static class Label implements Equatable {

        public static class TrueLabel extends Label {
            @Override
            public boolean extEquals(Object o) {
                return o instanceof TrueLabel;
            }
        }

        public static class FalseLabel extends Label {
            @Override
            public boolean extEquals(Object o) {
                return o instanceof FalseLabel;
            }
        }

        public static class VarLabel extends Label {
            private final String name;
            private final RelType type;

            public VarLabel(String name, RelType type) {
                this.name = name;
                this.type = type;
            }

            public String getName() {
                return name;
            }

            public RelType getType() {
                return type;
            }

            @Override
            public boolean extEquals(Object o) {
                if (!(o instanceof VarLabel)) return false;
                VarLabel other = (VarLabel) o;
                return name.equals(other.name) && type.equals(other.type);
            }
        } 

        public static class BoxedConstLabel extends Label {
            private final Object value;
            private final BaseType type;

            public BoxedConstLabel(Object value, BaseType type) {
                this.value = value;
                this.type = type;
            }

            public Object getValue() {
                return value;
            }

            public BaseType getType() {
                return type;
            }

            @Override
            public boolean extEquals(Object o) {
                if (!(o instanceof BoxedConstLabel)) return false;
                BoxedConstLabel other = (BoxedConstLabel) o;
                return value.equals(other.value) && type.equals(other.type);
            }
        }

        public static class LeftLabel extends Label {
            @Override
            public boolean extEquals(Object o) {
                return o instanceof LeftLabel;
            }
        }

        public static class RightLabel extends Label {
            @Override
            public boolean extEquals(Object o) {
                return o instanceof RightLabel;
            }
        }

        public static class MulLabel extends Label {
            @Override
            public boolean extEquals(Object o) {
                return o instanceof MulLabel;
            }
        }

        public static class AddLabel extends Label {
            @Override
            public boolean extEquals(Object o) {
                return o instanceof AddLabel;
            }
        }

        public static class NotLabel extends Label {
            @Override
            public boolean extEquals(Object o) {
                return o instanceof NotLabel;
            }
        }

        public static class SquashLabel extends Label {
            @Override
            public boolean extEquals(Object o) {
                return o instanceof SquashLabel;
            }
        }

        public static class USumLabel extends Label {
            @Override
            public boolean extEquals(Object o) {
                return o instanceof USumLabel;
            }
        }

        public static class LambdaLabel extends Label {
            @Override
            public boolean extEquals(Object o) {
                return o instanceof LambdaLabel;
            }
        }

        public static class AppLabel extends Label {
            @Override
            public boolean extEquals(Object o) {
                return o instanceof AppLabel;
            }
        }

        public static class RelationLabel extends Label {
            private final String name;
            private final RelType type;

            public RelationLabel(String name, RelType type) {
                this.name = name;
                this.type = type;
            }

            public String getName() {
                return name;
            }

            public RelType getType() {
                return type;
            }

            @Override
            public boolean extEquals(Object o) {
                if (!(o instanceof RelationLabel)) return false;
                RelationLabel other = (RelationLabel) o;
                return name.equals(other.name) && type.equals(other.type);
            }
        }

        public static class PredicateLabel extends Label {
            private final String name;

            public PredicateLabel(String name) {
                this.name = name;
            }

            public String getName() {
                return name;
            }

            @Override
            public boolean extEquals(Object o) {
                if (!(o instanceof PredicateLabel)) return false;
                PredicateLabel other = (PredicateLabel) o;
                return name.equals(other.name);
            }
        }

        public static class FunctionLabel extends Label {
            private final String name;
            private final RelType type;

            public FunctionLabel(String name, RelType type) {
                this.name = name;
                this.type = type;
            }

            public String getName() {
                return name;
            }

            public RelType getType() {
                return type;
            }

            @Override
            public boolean extEquals(Object o) {
                if (!(o instanceof FunctionLabel)) return false;
                FunctionLabel other = (FunctionLabel) o;
                return name.equals(other.name) && type.equals(other.type);
            }
        }

        public static Label fromExpr(Expr expr) {
            if (expr instanceof One) return new TrueLabel();
            if (expr instanceof Zero) return new FalseLabel();
            if (expr instanceof Left) return new LeftLabel();
            if (expr instanceof Right) return new RightLabel();
            if (expr instanceof Var) {
                Var var = (Var) expr;
                return new VarLabel(var.getName(), var.getType());
            }
            if (expr instanceof BoxedConst) {
                BoxedConst boxedConst = (BoxedConst) expr;
                return new BoxedConstLabel(boxedConst.getValue(), boxedConst.getConstType());
            }
            if (expr instanceof Mul) return new MulLabel();
            if (expr instanceof Add) return new AddLabel();
            if (expr instanceof Not) return new NotLabel();
            if (expr instanceof Squash) return new SquashLabel();
            if (expr instanceof USum) return new USumLabel();
            if (expr instanceof Lambda) return new LambdaLabel();
            if (expr instanceof App) return new AppLabel();
            if (expr instanceof Relation) {
                Relation relation = (Relation) expr;
                return new RelationLabel(relation.getName(), relation.getType());
            }
            if (expr instanceof Predicate) {
                Predicate predicate = (Predicate) expr;
                return new PredicateLabel(predicate.getName());
            }
            if (expr instanceof Function) {
                Function function = (Function) expr;
                return new FunctionLabel(function.getName(), function.getType());
            }
            throw new IllegalArgumentException("Unknown expression type");
        }

        public boolean extEquals(Object o) {
            throw new UnsupportedOperationException("Not implemented");
        }
    }



    private class ENode {
        private final Label label;
        private final List<Integer> children;

        public ENode(Label label, List<Integer> children) {
            this.label = label;
            this.children = children;
        }

        public Label getLabel() {
            return label;
        }

        public List<Integer> getChildren() {
            return children;
        }

        public Stream<Expr> generate() {
            if (label instanceof Label.TrueLabel) return Stream.of(new One());
            if (label instanceof Label.FalseLabel) return Stream.of(new Zero());
            if (label instanceof Label.VarLabel) {
                Label.VarLabel varLabel = (Label.VarLabel) label;
                return Stream.of(new Var(varLabel.getName(), varLabel.getType()));
            }
            if (label instanceof Label.BoxedConstLabel) {
                Label.BoxedConstLabel boxedConstLabel = (Label.BoxedConstLabel) label;
                return Stream.of(new BoxedConst(boxedConstLabel.getValue(), boxedConstLabel.getType()));
            }
            if (label instanceof Label.LeftLabel) {
                Integer child = children.get(0);
                return getEClass(child).generate();
            }
            if (label instanceof Label.RightLabel) {
                Integer child = children.get(0);
                return getEClass(child).generate();
            }
            if (label instanceof Label.MulLabel) {
                Integer left = children.get(0);
                Integer right = children.get(1);
                Stream<Expr> leftStream = getEClass(left).generate();
                Stream<Expr> rightStream = getEClass(right).generate();
                return leftStream.flatMap(l -> rightStream.map(r -> new Mul(l, r)));
            }
            if (label instanceof Label.AddLabel) {
                Integer left = children.get(0);
                Integer right = children.get(1);
                Stream<Expr> leftStream = getEClass(left).generate();
                Stream<Expr> rightStream = getEClass(right).generate();
                return leftStream.flatMap(l -> rightStream.map(r -> new Add(l, r)));
            }
            if (label instanceof Label.NotLabel) {
                Integer child = children.get(0);
                Stream<Expr> childStream = getEClass(child).generate();
                return childStream.map(Not::new);
            }
            if (label instanceof Label.SquashLabel) {
                Integer child = children.get(0);
                Stream<Expr> childStream = getEClass(child).generate();
                return childStream.map(Squash::new);
            }
            if (label instanceof Label.USumLabel) {
                Integer variable = children.get(0);
                Integer body = children.get(1);
                Stream<Expr> variableStream = getEClass(variable).generate();
                Stream<Expr> bodyStream = getEClass(body).generate();
                return variableStream.flatMap(v -> bodyStream.map(b -> new USum((Var) v, b)));
            }
            if (label instanceof Label.LambdaLabel) {
                Integer variable = children.get(0);
                Integer body = children.get(1);
                Stream<Expr> variableStream = getEClass(variable).generate();
                Stream<Expr> bodyStream = getEClass(body).generate();
                return variableStream.flatMap(v -> bodyStream.map(b -> new Lambda((Var) v, b)));
            }
            if (label instanceof Label.AppLabel) {
                Integer function = children.get(0);
                Integer argument = children.get(1);
                Stream<Expr> functionStream = getEClass(function).generate();
                Stream<Expr> argumentStream = getEClass(argument).generate();
                return functionStream.flatMap(f -> argumentStream.map(a -> new App((Lambda) f, a)));
            }
            if (label instanceof Label.RelationLabel) {
                Label.RelationLabel relationLabel = (Label.RelationLabel) label;
                Integer child = children.get(0);
                Stream<Expr> childStream = getEClass(child).generate();
                return childStream.map(c -> new Relation(relationLabel.getName(), relationLabel.getType(), (Var) c));
            }
            if (label instanceof Label.PredicateLabel) {
                Label.PredicateLabel predicateLabel = (Label.PredicateLabel) label;
                Stream<Stream<Expr>> childStream = children.stream().map(id -> getEClass(id)).map(EClass::generate);

                // now, we need the cross product of the elements of this stream (finite)
                // first we singularly box each element to a List(_) to uniformize types for upcoming reduce
                // finally, we compute the products with a reduce wrt a flatMap
                Stream<Stream<List<Expr>>> boxed = childStream.map(s -> s.map(List::of));
                Stream<List<Expr>> product = boxed.reduce(
                    (l, r) -> l.flatMap(
                        ll -> r.map(
                            rr -> {
                                List<Expr> newList = new ArrayList<>(ll);
                                newList.addAll(rr);
                                return newList;
                            }
                        )
                    )
                ).get();

                return product.map(l -> new Predicate(predicateLabel.getName(), l));
            }

            if (label instanceof Label.FunctionLabel) {
                Label.FunctionLabel functionLabel = (Label.FunctionLabel) label;
                Stream<Stream<Expr>> childStream = children.stream().map(id -> getEClass(id)).map(EClass::generate);

                // now, we need the cross product of the elements of this stream (finite)
                // first we singularly box each element to a List(_) to uniformize types for upcoming reduce
                // finally, we compute the products with a reduce wrt a flatMap
                Stream<Stream<List<Expr>>> boxed = childStream.map(s -> s.map(List::of));
                Stream<List<Expr>> product = boxed.reduce(
                    (l, r) -> l.flatMap(
                        ll -> r.map(
                            rr -> {
                                List<Expr> newList = new ArrayList<>(ll);
                                newList.addAll(rr);
                                return newList;
                            }
                        )
                    )
                ).get();

                return product.map(l -> new Function(functionLabel.getName(), l, functionLabel.getType()));
            }

            throw new IllegalArgumentException("Unknown label type");
        }
    }

    private class EClass {
        private final int id;
        private final Set<ENode> nodes;

        public EClass(int id, Set<ENode> nodes) {
            this.id = id;
            this.nodes = nodes;
        }

        public int getId() {
            return id;
        }

        public Set<ENode> getNodes() {
            return nodes;
        }

        public Stream<Expr> generate() {
            return nodes.stream().flatMap(node -> node.generate());
        }
    }

    private final Map<Integer, EClass> classes = new HashMap<>();
    private final Map<ENode, Integer> nodeLookup = new HashMap<>();
    private final Map<Integer, Set<Map.Entry<ENode, Integer>>> parents = new HashMap<>();
    private final UnionFind<Integer> uf = new UnionFind<>(id -> id);
    private final Rebuilder rebuilder = new Rebuilder();

    private final int root;

    public EGraph(Expr base) {
        root = insert(base);
        parents.put(root, new HashSet<>());
    }

    private EClass getEClass(int id) {
        return classes.get(uf.get(id));
    }

    private Set<Integer> visibleClassIDs() {
        return uf.visible();
    }

    private Set<EClass> visibleClasses() {
        return visibleClassIDs().stream().map(this::getEClass).collect(Collectors.toSet());
    }

    public Stream<Expr> generate() {
        return getEClass(root).generate();
    }

    private boolean contains(Expr expr, EClass eClass) {
        return eClass.getNodes().stream().anyMatch(node -> contains(expr, node));
    }

    private boolean contains(Expr expr, ENode node) {
        Label label = Label.fromExpr(expr);
        if (!label.equals(node.getLabel())) return false;
        List<EClass> children = node.getChildren().stream().map(this::getEClass).collect(Collectors.toList());
        return children.stream().map(EClass::getId).collect(Collectors.toList()).equals(expr.getChildren().stream().map(e -> find(e)).collect(Collectors.toList()));
    }

    private int find(Expr expr) {
        return visibleClassIDs().stream().filter(id -> contains(expr, getEClass(id))).findFirst().orElseGet(() -> insert(expr));
    }

    private int insert(Expr expr) {
        Label label = Label.fromExpr(expr);
        List<Integer> children = expr.getChildren().stream().map(this::find).collect(Collectors.toList());
        ENode en = new ENode(label, children);
        EClass newClass = new EClass(nextID(), new HashSet<>(Collections.singletonList(en)));
        classes.put(newClass.getId(), newClass);
        uf.add(newClass.getId());
        nodeLookup.put(en, newClass.getId());
        children.forEach(child -> parents.put(child, new HashSet<>(Collections.singletonList(new AbstractMap.SimpleEntry<>(en, newClass.getId())))));
        return newClass.getId();
    }

    private void merge(EClass cl1, EClass cl2) {
        int newParent = uf.union(cl1.getId(), cl2.getId());
        EClass parentClass = getEClass(newParent);
        parentClass.getNodes().addAll(cl1.getNodes());
        parentClass.getNodes().addAll(cl2.getNodes());
        rebuilder.add(newParent);
    }

    private void merge(int cl1, int cl2) {
        merge(getEClass(cl1), getEClass(cl2));
    }

    public Optional<Map<Var, Expr>> matchAtClass(EClass cl, Expr expr, Optional<Map<Var, Expr>> partialSubst) {
        return cl.getNodes().stream().map(node -> matchAtNode(node, expr, partialSubst)).filter(Optional::isPresent).findFirst().orElse(Optional.empty());
    }

    public Optional<Map<Var, Expr>> matchAtNode(ENode en, Expr expr, Optional<Map<Var, Expr>> partialSubst) {
        if (partialSubst.isEmpty()) return Optional.empty();
        Label label = Label.fromExpr(expr);
        if (!label.extEquals(en.getLabel())) return Optional.empty();

        Map<Var, Expr> subst = partialSubst.get();
        
        // is this a variable?
        if (label instanceof Label.VarLabel) {
            Label.VarLabel varLabel = (Label.VarLabel) label;
            if (subst.containsKey(new Var(varLabel.getName(), varLabel.getType()))) {
                if (subst.get(new Var(varLabel.getName(), varLabel.getType())).extEquals(expr)) return Optional.of(subst);
                return Optional.empty();
            }
            subst.put(new Var(varLabel.getName(), varLabel.getType()), expr);
            return Optional.of(subst);
        }
        
        // else, match children
        List<Expr> children = expr.getChildren();
        List<Integer> childClasses = en.getChildren();

        if (children.size() != childClasses.size()) return Optional.empty();

        for (int i = 0; i < children.size(); i++) {
            Expr child = children.get(i);
            int childClass = childClasses.get(i);
            Optional<Map<Var, Expr>> newSubst = matchAtClass(getEClass(childClass), child, Optional.of(subst));
            if (newSubst.isEmpty()) return Optional.empty();
            subst.putAll(newSubst.get());
        }

        return Optional.of(subst);

    }

    public void rewrite(Rule rule) {
        Expr lhs = rule.lhs();
        Expr rhs = rule.rhs();
        visibleClasses().forEach(cl -> {
            matchAtClass(cl, lhs, Optional.of(new HashMap<>())).ifPresent(subst -> {
                Expr newTerm = rhs.substituted(subst);
                int newClass = find(newTerm);
                merge(cl, getEClass(newClass));
            });
        });
        rebuilder.rebuild();
    }

    // counter
    private static int maxID = 0;
    private static int nextID() {
        return maxID++;
    }

    private class UnionFind<T> {
        private final Map<T, T> parent = new HashMap<>();
        private final Map<T, Set<T>> children = new HashMap<>();
        private final java.util.function.Function<T, Integer> ranking;

        public UnionFind(java.util.function.Function<T, Integer> ranking) {
            this.ranking = ranking;
        }

        public Set<T> visible() {
            return children.keySet();
        }

        public void add(T elem) {
            union(elem, elem);
        }

        public T union(T t, T u) {
            T pt = parent.computeIfAbsent(t, k -> {
                children.put(t, new HashSet<>());
                return t;
            });
            T pu = parent.computeIfAbsent(u, k -> {
                children.put(u, new HashSet<>());
                return u;
            });
            if (pt.equals(pu)) {
                return pt;
            } else {
                return merge(pt, pu);
            }
        }

        private T merge(T pt, T pu) {
            if (ranking.apply(pt) < ranking.apply(pu)) {
                return merge(pu, pt);
            }
            children.get(pu).forEach(child -> parent.put(child, pt));
            children.get(pt).addAll(children.get(pu));
            children.remove(pu);
            parent.put(pu, pt);
            return pt;
        }

        public T get(T t) {
            return parent.getOrDefault(t, t);
        }
    }

    private class Rebuilder {
        private Set<Integer> worklist = new HashSet<>();

        private int canonicalizeClass(int cl) {
            return uf.get(cl);
        }

        private ENode canonicalizeNode(ENode node) {
            Label label = node.getLabel();
            List<Integer> children = node.getChildren().stream().map(this::canonicalizeClass).collect(Collectors.toList());
            return new ENode(label, children);
        }

        public void add(int id) {
            worklist.add(id);
        }

        public void rebuild() {
            while (!worklist.isEmpty()) {
                int cl = worklist.iterator().next();
                worklist.remove(cl);
                repair(cl);
            }
        }

        private void repair(int cl) {
            HashSet<Map.Entry<ENode, Integer>> oldParents = new HashSet<Map.Entry<ENode, Integer>>(parents.get(cl));
            oldParents.forEach(
                entry -> {
                    ENode node = entry.getKey();
                    int par = entry.getValue();
                    nodeLookup.remove(node);
                    ENode newNode = canonicalizeNode(node);
                    int newClass = canonicalizeClass(par);
                    nodeLookup.put(newNode, newClass);
                }
            );

            HashMap<ENode, Integer> newParents = new HashMap<ENode, Integer>();

            for (Map.Entry<ENode, Integer> entry : oldParents) {
                ENode canonical = canonicalizeNode(entry.getKey());
                int newClass = canonicalizeClass(entry.getValue());
                if (newParents.containsKey(canonical)) {
                    merge(entry.getValue(), newClass);
                }
                newParents.put(canonical, newClass);
            }

            parents.put(cl, newParents.entrySet());
        }
    }

    public interface Rule {
        Expr lhs();
        Expr rhs();
    }
}
