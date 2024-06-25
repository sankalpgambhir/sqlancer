package sqlancer.common.usr.transformation;

import sqlancer.common.usr.language.SQL;
import sqlancer.common.usr.language.USR.Expr;
import sqlancer.common.usr.language.USR.*;
import sqlancer.common.usr.language.SimpleTypes.RelType;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Reconstruct SQL queries
 */
public class Reconstruct {

    public static SQL.Query apply(Expr expr) {
        return translate(expr);
    }

    private interface SelectorConstraint {
    }

    private static class Unassigned implements SelectorConstraint {
    }

    private static class Assigned implements SelectorConstraint {
        Expr expr;

        Assigned(Expr expr) {
            this.expr = expr;
        }
    }

    private static class Node implements SelectorConstraint {
        SelectorConstraint left;
        SelectorConstraint right;

        Node(SelectorConstraint left, SelectorConstraint right) {
            this.left = left;
            this.right = right;
        }
    }

    public static class ConstraintBuilder {
        private boolean inconsistent = false;
        private final Map<Expr, Expr> partialAssignment = new HashMap<>();
        private final Set<Expr> constraints = new HashSet<>();
        private SelectorConstraint selector = new Unassigned();
        private final Var topVar;

        public ConstraintBuilder(Var topVar) {
            this.topVar = topVar;
        }

        public boolean isInconsistent() {
            return inconsistent;
        }

        public Map<Expr, Expr> getAssignment() {
            return new HashMap<>(partialAssignment);
        }

        public Set<Expr> getConstraints() {
            return new HashSet<>(constraints);
        }

        private SelectorConstraint intersect(SelectorConstraint left, SelectorConstraint right) {
            if (left instanceof Unassigned)
                return right;
            if (right instanceof Unassigned)
                return left;
            if (left instanceof Node && right instanceof Node) {
                Node lNode = (Node) left;
                Node rNode = (Node) right;
                return new Node(intersect(lNode.left, rNode.left), intersect(lNode.right, rNode.right));
            }
            if (left instanceof Assigned && right instanceof Assigned) {
                if (!((Assigned) left).expr.equals(((Assigned) right).expr)) {
                    inconsistent = true;
                }
                return left;
            }
            return left;
        }

        public void assignSelector(Projection at, Expr expr) {
            selector = intersect(selector, construct(at, new Assigned(expr)));
        }

        private SelectorConstraint construct(Projection at, SelectorConstraint value) {
            if (at instanceof Var) {
                return value;
            } else if (at instanceof Left) {
                return new Node(value, new Unassigned());
            } else if (at instanceof Right) {
                return new Node(new Unassigned(), value);
            } else {
                throw new IllegalArgumentException("Unknown projection type: " + at.getClass());
            }
        }

        public List<Expr> collapsedSelectors() {
            return collapse(selector);
        }

        private List<Expr> collapse(SelectorConstraint node) {
            if (node instanceof Unassigned) {
                return Collections.emptyList();
            } else if (node instanceof Assigned) {
                return Collections.singletonList(((Assigned) node).expr);
            } else if (node instanceof Node) {
                Node n = (Node) node;
                List<Expr> left = collapse(n.left);
                List<Expr> right = collapse(n.right);
                left.addAll(right);
                return left;
            }
            return Collections.emptyList();
        }

        public ConstraintBuilder withFalse() {
            inconsistent = true;
            return this;
        }

        public ConstraintBuilder withConstraint(Expr expr) {
            constraints.add(expr);
            return this;
        }

        public void assignSource(Expr at, Expr expr, boolean squash) {
            partialAssignment.put(at, squash ? new Squash(expr) : expr);
        }

        public ConstraintBuilder copy() {
            ConstraintBuilder copy = new ConstraintBuilder(topVar);
            copy.inconsistent = inconsistent;
            copy.partialAssignment.putAll(partialAssignment);
            copy.constraints.addAll(constraints);
            copy.selector = selector;
            return copy;
        }
    }

    private static boolean projects(Expr expr, Var variable) {
        if (expr instanceof Var) {
            return ((Var) expr).getName().equals(variable.getName());
        } else if (expr instanceof Left) {
            return projects(((Left) expr).getInner(), variable);
        } else if (expr instanceof Right) {
            return projects(((Right) expr).getInner(), variable);
        }
        return false;
    }

    private interface Tree<A> {
        <B> Tree<B> map(java.util.function.Function<A, B> f);

        <B> Tree<B> flatMap(java.util.function.Function<A, Tree<B>> f);

        A reduceLeft(java.util.function.BiFunction<A, A, A> f);
    }

    private static class Leaf<A> implements Tree<A> {
        private final A value;

        Leaf(A value) {
            this.value = value;
        }

        @Override
        public <B> Tree<B> map(java.util.function.Function<A, B> f) {
            return new Leaf<>(f.apply(value));
        }

        @Override
        public <B> Tree<B> flatMap(java.util.function.Function<A, Tree<B>> f) {
            return f.apply(value);
        }

        @Override
        public A reduceLeft(java.util.function.BiFunction<A, A, A> f) {
            return value;
        }
    }

    private static class Branch<A> implements Tree<A> {
        private final Tree<A> left;
        private final Tree<A> right;

        Branch(Tree<A> left, Tree<A> right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public <B> Tree<B> map(java.util.function.Function<A, B> f) {
            return new Branch<>(left.map(f), right.map(f));
        }

        @Override
        public <B> Tree<B> flatMap(java.util.function.Function<A, Tree<B>> f) {
            return new Branch<>(left.flatMap(f), right.flatMap(f));
        }

        @Override
        public A reduceLeft(java.util.function.BiFunction<A, A, A> f) {
            return f.apply(left.reduceLeft(f), right.reduceLeft(f));
        }
    }

    private static class AccContext {
        boolean polarity;
        boolean squashed;

        AccContext(boolean polarity, boolean squashed) {
            this.polarity = polarity;
            this.squashed = squashed;
        }
    }

    private static Tree<ConstraintBuilder> accumulateConstraints(Expr expr, ConstraintBuilder context,
            AccContext opts) {
        if (expr instanceof One) {
            return new Leaf<>(context);
        } else if (expr instanceof Zero) {
            return new Leaf<>(context.withFalse());
        } else if (expr instanceof Mul) {
            Mul mul = (Mul) expr;
            if (opts.polarity) {
                return accumulateConstraints(mul.getLeft(), context, opts)
                        .flatMap(ctx -> accumulateConstraints(mul.getRight(), ctx, opts));
            } else {
                return new Branch<>(accumulateConstraints(mul.getLeft(), context, opts),
                        accumulateConstraints(mul.getRight(), context, opts));
            }
        } else if (expr instanceof Add) {
            Add add = (Add) expr;
            if (opts.polarity) {
                return new Branch<>(accumulateConstraints(add.getLeft(), context, opts),
                        accumulateConstraints(add.getRight(), context, opts));
            } else {
                return accumulateConstraints(add.getLeft(), context, opts)
                        .flatMap(ctx -> accumulateConstraints(add.getRight(), ctx, opts));
            }
        } else if (expr instanceof Not) {
            return accumulateConstraints(((Not) expr).getInner(), context,
                    new AccContext(!opts.polarity, opts.squashed));
        } else if (expr instanceof Squash) {
            return accumulateConstraints(((Squash) expr).getInner(), context, new AccContext(opts.polarity, true));
        } else if (expr instanceof USum) {
            return accumulateConstraints(((USum) expr).getInner(), context, opts);
        } else if (expr instanceof App) {
            App app = (App) expr;
            context.assignSource(app.getArg(), expr, opts.squashed);
            return new Leaf<>(context);
        } else if (expr instanceof Relation) {
            Relation relation = (Relation) expr;
            context.assignSource(relation.getArg(), expr, opts.squashed);
            return new Leaf<>(context);
        } else if (expr instanceof Equal) {
            Equal eq = (Equal) expr;
            if (projects(eq.getLeft(), context.topVar)) {
                context.assignSelector((Projection) eq.getLeft(), eq.getRight());
                return new Leaf<>(context);
            } else if (projects(eq.getRight(), context.topVar)) {
                context.assignSelector((Projection) eq.getRight(), eq.getLeft());
                return new Leaf<>(context);
            } else {
                return new Leaf<>(context.withConstraint(expr));
            }
        } else if (expr instanceof Predicate) {
            return new Leaf<>(context.withConstraint(expr));
        } else {
            throw new IllegalStateException(
                    "Unreachable pattern, reached raw value in constraint accumulation. Value: " + expr);
        }
    }

    public static SQL.Query translate(Expr expr) {
        if (expr instanceof App) {
            App app = (App) expr;
            if (app.getFun() instanceof Lambda) {
                Lambda lambda = (Lambda) app.getFun();
                ConstraintBuilder context = new ConstraintBuilder(lambda.getVariable());
                Tree<ConstraintBuilder> tree = accumulateConstraints(lambda.getInner(), context,
                        new AccContext(true, false));
                return tree.map(Reconstruct::toQuery).reduceLeft(SQL.Union::new);
            }
        }
        throw new IllegalStateException("Top level expression must be a lambda.");
    }

    private static SQL.Query toQuery(ConstraintBuilder context) {
        if (context.isInconsistent()) {
            throw new IllegalStateException("Inconsistent constraints.");
        } else {
            List<SQL.Selector> selectors = context.collapsedSelectors().stream()
                    .map(Reconstruct::toSQLSelector)
                    .collect(Collectors.toList());

            Map<Expr, Expr> assignment = context.getAssignment();

            boolean distinct = assignment.values().stream().allMatch(expr -> expr instanceof Squash);

            boolean wellFormed = assignment.values().stream().noneMatch(expr -> expr instanceof Squash) || distinct;

            if (!wellFormed) {
                throw new IllegalStateException("Invalid query, cannot have partially squashed sources.");
            }

            List<SQL.LabelledRelational> from = assignment.entrySet().stream()
                    .map(entry -> {
                        Var var = (Var) entry.getKey();
                        Expr value = entry.getValue();
                        if (value instanceof Squash) {
                            Squash squash = (Squash) value;
                            if (squash.getInner() instanceof Relation) {
                                Relation relation = (Relation) squash.getInner();
                                return new SQL.LabelledRelational(new SQL.Label(var.getName(), var.getType()),
                                        new SQL.Table(relation.getName(), relation.getType()));
                            } else {
                                return new SQL.LabelledRelational(new SQL.Label(var.getName(), var.getType()),
                                        translate(squash.getInner()));
                            }
                        } else if (value instanceof Relation) {
                            Relation relation = (Relation) value;
                            return new SQL.LabelledRelational(new SQL.Label(var.getName(), var.getType()),
                                    new SQL.Table(relation.getName(), relation.getType()));
                        } else {
                            return new SQL.LabelledRelational(new SQL.Label(var.getName(), var.getType()),
                                    translate(value));
                        }
                    })
                    .collect(Collectors.toList());

            SQL.Predicate where = context.getConstraints().stream()
                    .map(Reconstruct::toSQLPred)
                    .reduce(SQL.And::new)
                    .orElseThrow(() -> new IllegalStateException("No constraints found."));

            SQL.Select select = new SQL.Select(selectors, from, where);

            return distinct ? new SQL.Distinct(select) : select;
        }
    }

    private static SQL.Selector toSQLSelector(Expr expr) {
        if (expr instanceof Var) {
            Var var = (Var) expr;
            return new SQL.TableProjection(new SQL.Table(var.getName(), var.getType()));
        } else if (expr instanceof Left) {
            return new SQL.LeftProjection((SQL.Projection) toSQLSelector(((Left) expr).getInner()));
        } else if (expr instanceof Right) {
            return new SQL.RightProjection((SQL.Projection) toSQLSelector(((Right) expr).getInner()));
        } else if (expr instanceof BoxedConst) {
            BoxedConst boxedConst = (BoxedConst) expr;
            return new SQL.ConstantSelector(new SQL.BoxedConstant(boxedConst.getValue(), boxedConst.getConstType()));
        } else if (expr instanceof Function) {
            Function func = (Function) expr;
            List<SQL.Selector> args = func.getChildren().stream()
                    .map(Reconstruct::toSQLSelector)
                    .collect(Collectors.toList());
            List<RelType> itps = args.stream()
                    .map(SQL.Selector::getType)
                    .collect(Collectors.toList());
            return new SQL.FunctionalSelector(new SQL.Functional(func.getName(), itps, func.getType()), args);
        }
        throw new IllegalStateException("Invalid selector.");
    }

    private static SQL.Predicate toSQLPred(Expr expr) {
        if (expr instanceof Zero) {
            return new SQL.False();
        } else if (expr instanceof One) {
            return new SQL.True();
        } else if (expr instanceof Equal) {
            Equal eq = (Equal) expr;
            return new SQL.Eq(toSQLSelector(eq.getLeft()), toSQLSelector(eq.getRight()));
        } else if (expr instanceof Gt) {
            Gt gt = (Gt) expr;
            return new SQL.Gt(toSQLSelector(gt.getLeft()), toSQLSelector(gt.getRight()));
        } else if (expr instanceof Lt) {
            Lt lt = (Lt) expr;
            return new SQL.Lt(toSQLSelector(lt.getLeft()), toSQLSelector(lt.getRight()));
        } else if (expr instanceof Add) {
            Add add = (Add) expr;
            return new SQL.Or(toSQLPred(add.getLeft()), toSQLPred(add.getRight()));
        } else if (expr instanceof Mul) {
            Mul mul = (Mul) expr;
            return new SQL.And(toSQLPred(mul.getLeft()), toSQLPred(mul.getRight()));
        } else if (expr instanceof Not) {
            return new SQL.Not(toSQLPred(((Not) expr).getInner()));
        }
        throw new IllegalStateException("Invalid predicate.");
    }
}
