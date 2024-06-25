package sqlancer.common.usr.transformation;

import sqlancer.common.usr.language.*;
import sqlancer.common.usr.language.SimpleTypes.*;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Reduce SQL queries to USR Expressions
 */
public class Deconstruct {

    public static class Variable {
        private static int id = 0;

        private static synchronized int newID() {
            return ++id;
        }

        public static USR.Var fresh(RelType tpe) {
            return freshNamed("x", tpe);
        }

        public static USR.Var freshNamed(String name, RelType tpe) {
            int id = newID();
            return new USR.Var(name + "_" + id, tpe);
        }

        // Stable names to refer to tables, etc:
        public static USR.Var fromLabel(SQL.Label label) {
            return new USR.Var(label.getName(), label.getType());
        }

        public static USR.Var fromName(String name, RelType tpe) {
            return new USR.Var(name, tpe);
        }
    }

    public static USR.Expr apply(SQL.Query query) {
        return translate(query);
    }

    public static USR.Lambda translate(SQL.Query query) {
        if (query instanceof SQL.Select) {
            SQL.Select select = (SQL.Select) query;
            USR.Var topVar = Variable.fresh(query.getType());
            USR.Expr selectionConstraints = extractSelectors(select.getSelectors(), topVar);
            USR.Expr fromConstraints = extractFroms(select.getFrom());
            USR.Expr whereConstraints = extractPredicates(select.getWhere());
            return new USR.Lambda(topVar, new USR.Mul(new USR.Mul(selectionConstraints, fromConstraints), whereConstraints));
        } else if (query instanceof SQL.Union) {
            SQL.Union union = (SQL.Union) query;
            USR.Var topVar = Variable.fresh(query.getType());
            return new USR.Lambda(topVar, new USR.Add(translate(union.getLeft()).eval(topVar), translate(union.getRight()).eval(topVar)));
        } else if (query instanceof SQL.Join) {
            SQL.Join join = (SQL.Join) query;
            USR.Var topVar = Variable.fresh(query.getType());
            return new USR.Lambda(topVar, new USR.Mul(translate(join.getLeft()).eval(topVar), translate(join.getRight()).eval(topVar)));
        } else if (query instanceof SQL.Distinct) {
            SQL.Distinct distinct = (SQL.Distinct) query;
            USR.Var topVar = Variable.fresh(query.getType());
            return new USR.Lambda(topVar, new USR.Squash(translate(distinct.getInner()).eval(topVar)));
        } else if (query instanceof SQL.Except) {
            throw new UnsupportedOperationException("Except is not supported.");
        }
        throw new IllegalArgumentException("Unknown query type: " + query.getClass());
    }

    private static USR.Expr selectorToExpr(SQL.Selector selector) {
        if (selector instanceof SQL.LeftProjection) {
            return new USR.Left((USR.Projection) selectorToExpr(((SQL.LeftProjection) selector).getInner()));
        } else if (selector instanceof SQL.RightProjection) {
            return new USR.Right((USR.Projection) selectorToExpr(((SQL.RightProjection) selector).getInner()));
        } else if (selector instanceof SQL.TableProjection) {
            SQL.TableProjection tableProj = (SQL.TableProjection) selector;
            return Variable.fromName(tableProj.getTable().getName(), tableProj.getTable().getType());
        } else if (selector instanceof SQL.ConstantSelector) {
            SQL.ConstantSelector constSelector = (SQL.ConstantSelector) selector;
            return new USR.BoxedConst(constSelector.getValue(), constSelector.getValue().getConstType());
        } else if (selector instanceof SQL.FunctionalSelector) {
            SQL.FunctionalSelector funcSelector = (SQL.FunctionalSelector) selector;
            List<USR.Expr> args = funcSelector.getArgs().stream().map(Deconstruct::selectorToExpr).collect(Collectors.toList());
            return new USR.Function(funcSelector.getFun().getName(), args, funcSelector.getFun().getOutputType());
        }
        throw new IllegalArgumentException("Unknown selector type: " + selector.getClass());
    }

    private static USR.Expr extractSelectors(List<SQL.Selector> selectors, USR.Projection currentProj) {
        if (selectors.isEmpty()) {
            return new USR.One();
        } else if (selectors.size() == 1) {
            return new USR.Equal(currentProj, selectorToExpr(selectors.get(0)));
        } else {
            USR.Expr headExpr = new USR.Equal(new USR.Left(currentProj), selectorToExpr(selectors.get(0)));
            USR.Expr tailExpr = extractSelectors(selectors.subList(1, selectors.size()), new USR.Right(currentProj));
            return new USR.Mul(headExpr, tailExpr);
        }
    }

    private static USR.Expr extractFroms(List<SQL.LabelledRelational> from) {
        return from.stream().map(f -> {
            if (f.getRelational() instanceof SQL.Query) {
                USR.Lambda innerLambda = translate((SQL.Query) f.getRelational());
                USR.Var lbl = Variable.fromLabel(f.getLabel());
                return innerLambda.eval(lbl);
            } else if (f.getRelational() instanceof SQL.Table) {
                SQL.Table table = (SQL.Table) f.getRelational();
                USR.Var lbl = Variable.fromLabel(f.getLabel());
                return new USR.Relation(table.getName(), table.getType(), lbl);
            }
            throw new IllegalArgumentException("Nested labelled relationals are not supported.");
        }).reduce((l, r) -> new USR.Mul(l, r)).orElse(new USR.One());
    }

    private static USR.Expr extractPredicates(SQL.Predicate where) {
        if (where instanceof SQL.True) {
            return new USR.One();
        } else if (where instanceof SQL.False) {
            return new USR.Zero();
        } else if (where instanceof SQL.And) {
            SQL.And and = (SQL.And) where;
            return new USR.Mul(extractPredicates(and.getLeft()), extractPredicates(and.getRight()));
        } else if (where instanceof SQL.Or) {
            SQL.Or or = (SQL.Or) where;
            return new USR.Add(extractPredicates(or.getLeft()), extractPredicates(or.getRight()));
        } else if (where instanceof SQL.Not) {
            return new USR.Not(extractPredicates(((SQL.Not) where).getInner()));
        } else if (where instanceof SQL.Eq) {
            SQL.Eq eq = (SQL.Eq) where;
            return new USR.Equal(selectorToExpr(eq.getLeft()), selectorToExpr(eq.getRight()));
        } else if (where instanceof SQL.Gt) {
            SQL.Gt gt = (SQL.Gt) where;
            return new USR.Gt(selectorToExpr(gt.getLeft()), selectorToExpr(gt.getRight()));
        } else if (where instanceof SQL.Lt) {
            SQL.Lt lt = (SQL.Lt) where;
            return new USR.Lt(selectorToExpr(lt.getLeft()), selectorToExpr(lt.getRight()));
        } else if (where instanceof SQL.Uninterpreted) {
            SQL.Uninterpreted uninterpreted = (SQL.Uninterpreted) where;
            List<USR.Expr> args = uninterpreted.getArgs().stream().map(Deconstruct::selectorToExpr).collect(Collectors.toList());
            return new USR.Function(uninterpreted.getName(), args, new Leaf(BaseType.BOOL_TYPE));
        }
        throw new IllegalArgumentException("Unknown predicate type: " + where.getClass());
    }
}
