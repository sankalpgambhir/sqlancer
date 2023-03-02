package sqlancer.mariadb;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.visitor.ToStringVisitor;
import sqlancer.mariadb.ast.MariaDBBetweenOperation;
import sqlancer.mariadb.ast.MariaDBBinaryComparisonOperation;
import sqlancer.mariadb.ast.MariaDBBinaryLogicalOperation;
import sqlancer.mariadb.ast.MariaDBBinaryOperation;
import sqlancer.mariadb.ast.MariaDBCastOperation;
import sqlancer.mariadb.ast.MariaDBCollate;
import sqlancer.mariadb.ast.MariaDBColumnReference;
import sqlancer.mariadb.ast.MariaDBComputableFunction;
import sqlancer.mariadb.ast.MariaDBConstant;
import sqlancer.mariadb.ast.MariaDBExists;
import sqlancer.mariadb.ast.MariaDBExpression;
import sqlancer.mariadb.ast.MariaDBInOperation;
import sqlancer.mariadb.ast.MariaDBOrderByTerm;
import sqlancer.mariadb.ast.MariaDBOrderByTerm.MariaDBOrder;
import sqlancer.mariadb.ast.MariaDBSelect;
import sqlancer.mariadb.ast.MariaDBStringExpression;
import sqlancer.mariadb.ast.MariaDBTableReference;
import sqlancer.mariadb.ast.MariaDBUnaryPostfixOperation;

public class MariaDBToStringVisitor extends ToStringVisitor<MariaDBExpression> implements MariaDBVisitor {

    int ref;

    @Override
    public void visitSpecific(MariaDBExpression expr) {
        MariaDBVisitor.super.visit(expr);
    }

    @Override
    public void visit(MariaDBSelect s) {
        sb.append("SELECT ");
        switch (s.getFromOptions()) {
        case DISTINCT:
            sb.append("DISTINCT ");
            break;
        case ALL:
            sb.append(Randomly.fromOptions("ALL ", ""));
            break;
        case DISTINCTROW:
            sb.append("DISTINCTROW ");
            break;
        default:
            throw new AssertionError();
        }
        sb.append(s.getModifiers().stream().collect(Collectors.joining(" ")));
        if (s.getModifiers().size() > 0) {
            sb.append(" ");
        }
        if (s.getFetchColumns() == null) {
            sb.append("*");
        } else {
            for (int i = 0; i < s.getFetchColumns().size(); i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                visit(s.getFetchColumns().get(i));
                // MariaDB does not allow duplicate column names
                sb.append(" AS ");
                sb.append("ref");
                sb.append(ref++);
            }
        }
        sb.append(" FROM ");
        for (int i = 0; i < s.getFromList().size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            visit(s.getFromList().get(i));
        }
        for (MariaDBExpression j : s.getJoinList()) {
            visit(j);
        }

        if (s.getWhereClause() != null) {
            MariaDBExpression whereClause = s.getWhereClause();
            sb.append(" WHERE ");
            visit(whereClause);
        }
        if (s.getGroupByExpressions() != null && s.getGroupByExpressions().size() > 0) {
            sb.append(" ");
            sb.append("GROUP BY ");
            List<MariaDBExpression> groupBys = s.getGroupByExpressions();
            for (int i = 0; i < groupBys.size(); i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                visit(groupBys.get(i));
            }
        }
        if (!s.getOrderByExpressions().isEmpty()) {
            sb.append(" ORDER BY ");
            List<MariaDBExpression> orderBys = s.getOrderByExpressions();
            for (int i = 0; i < orderBys.size(); i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                visit(s.getOrderByExpressions().get(i));
            }
        }
        if (s.getLimitClause() != null) {
            sb.append(" LIMIT ");
            visit(s.getLimitClause());
        }

        if (s.getOffsetClause() != null) {
            sb.append(" OFFSET ");
            visit(s.getOffsetClause());
        }
    }

    @Override
    public void visit(MariaDBConstant constant) {
        sb.append(constant.getTextRepresentation());
    }

    @Override
    public String get() {
        return sb.toString();
    }

    @Override
    public void visit(MariaDBColumnReference column) {
        sb.append(column.getColumn().getFullQualifiedName());
    }

    @Override
    public void visit(MariaDBUnaryPostfixOperation op) {
        sb.append("(");
        visit(op.getExpression());
        sb.append(")");
        sb.append(" IS ");
        if (op.isNegated()) {
            sb.append("NOT ");
        }
        switch (op.getOperator()) {
        case IS_FALSE:
            sb.append("FALSE");
            break;
        case IS_NULL:
            if (Randomly.getBoolean()) {
                sb.append("UNKNOWN");
            } else {
                sb.append("NULL");
            }
            break;
        case IS_TRUE:
            sb.append("TRUE");
            break;
        default:
            throw new AssertionError(op);
        }
    }

    @Override
    public void visit(MariaDBComputableFunction f) {
        sb.append(f.getFunction().getName());
        sb.append("(");
        for (int i = 0; i < f.getArguments().length; i++) {
            if (i != 0) {
                sb.append(", ");
            }
            visit(f.getArguments()[i]);
        }
        sb.append(")");
    }

    @Override
    public void visit(MariaDBBinaryLogicalOperation op) {
        sb.append("(");
        visit(op.getLeft());
        sb.append(")");
        sb.append(" ");
        sb.append(op.getTextRepresentation());
        sb.append(" ");
        sb.append("(");
        visit(op.getRight());
        sb.append(")");
    }

    @Override
    public void visit(MariaDBBinaryComparisonOperation op) {
        sb.append("(");
        visit(op.getLeft());
        sb.append(") ");
        sb.append(op.getOp().getTextRepresentation());
        sb.append(" (");
        visit(op.getRight());
        sb.append(")");
    }

    @Override
    public void visit(MariaDBCastOperation op) {
        sb.append("CAST(");
        visit(op.getExpr());
        sb.append(" AS ");
        sb.append(op.getType());
        sb.append(")");
    }

    @Override
    public void visit(MariaDBInOperation op) {
        sb.append("(");
        visit(op.getExpr());
        sb.append(")");
        if (!op.isTrue()) {
            sb.append(" NOT");
        }
        sb.append(" IN ");
        sb.append("(");
        for (int i = 0; i < op.getListElements().size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            visit(op.getListElements().get(i));
        }
        sb.append(")");
    }

    @Override
    public void visit(MariaDBBinaryOperation op) {
        sb.append("(");
        visit(op.getLeft());
        sb.append(") ");
        sb.append(op.getOp().getTextRepresentation());
        sb.append(" (");
        visit(op.getRight());
        sb.append(")");
    }

    @Override
    public void visit(MariaDBOrderByTerm op) {
        visit(op.getExpr());
        sb.append(" ");
        sb.append(op.getOrder() == MariaDBOrder.ASC ? "ASC" : "DESC");
    }

    @Override
    public void visit(MariaDBExists op) {
        sb.append(" EXISTS (");
        visit(op.getExpr());
        sb.append(")");
    }

    @Override
    public void visit(MariaDBStringExpression op) {
        sb.append(op.getStr());
    }

    @Override
    public void visit(MariaDBBetweenOperation op) {
        sb.append("(");
        visit(op.getExpr());
        sb.append(") BETWEEN (");
        visit(op.getLeft());
        sb.append(") AND (");
        visit(op.getRight());
        sb.append(")");
    }

    @Override
    public void visit(MariaDBTableReference ref) {
        sb.append(ref.getTable().getName());
    }

    @Override
    public void visit(MariaDBCollate collate) {
        sb.append("(");
        visit(collate.getExpression());
        sb.append(" ");
        sb.append(collate.getOperatorRepresentation());
        sb.append(")");
    }

}
