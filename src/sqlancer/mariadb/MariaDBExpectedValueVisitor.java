package sqlancer.mariadb;

import sqlancer.IgnoreMeException;
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
import sqlancer.mariadb.ast.MariaDBSelect;
import sqlancer.mariadb.ast.MariaDBStringExpression;
import sqlancer.mariadb.ast.MariaDBTableReference;
import sqlancer.mariadb.ast.MariaDBUnaryPostfixOperation;

public class MariaDBExpectedValueVisitor implements MariaDBVisitor {

    private final StringBuilder sb = new StringBuilder();
    private int nrTabs;

    private void print(MariaDBExpression expr) {
        MariaDBToStringVisitor v = new MariaDBToStringVisitor();
        v.visit(expr);
        for (int i = 0; i < nrTabs; i++) {
            sb.append("\t");
        }
        sb.append(v.get());
        sb.append(" -- ");
        sb.append(expr.getExpectedValue());
        sb.append("\n");
    }

    @Override
    public void visit(MariaDBExpression expr) {
        nrTabs++;
        try {
            MariaDBVisitor.super.visit(expr);
        } catch (IgnoreMeException e) {

        }
        nrTabs--;
    }

    @Override
    public void visit(MariaDBConstant constant) {
        print(constant);
    }

    @Override
    public void visit(MariaDBColumnReference column) {
        print(column);
    }

    @Override
    public void visit(MariaDBUnaryPostfixOperation op) {
        print(op);
        visit(op.getExpression());
    }

    @Override
    public void visit(MariaDBComputableFunction f) {
        print(f);
        for (MariaDBExpression expr : f.getArguments()) {
            visit(expr);
        }
    }

    @Override
    public void visit(MariaDBBinaryLogicalOperation op) {
        print(op);
        visit(op.getLeft());
        visit(op.getRight());
    }

    public String get() {
        return sb.toString();
    }

    @Override
    public void visit(MariaDBSelect select) {
        for (MariaDBExpression j : select.getJoinList()) {
            visit(j);
        }
        if (select.getWhereClause() != null) {
            visit(select.getWhereClause());
        }
    }

    @Override
    public void visit(MariaDBBinaryComparisonOperation op) {
        print(op);
        visit(op.getLeft());
        visit(op.getRight());
    }

    @Override
    public void visit(MariaDBCastOperation op) {
        print(op);
        visit(op.getExpr());
    }

    @Override
    public void visit(MariaDBInOperation op) {
        print(op);
        for (MariaDBExpression right : op.getListElements()) {
            visit(right);
        }
    }

    @Override
    public void visit(MariaDBBinaryOperation op) {
        print(op);
        visit(op.getLeft());
        visit(op.getRight());
    }

    @Override
    public void visit(MariaDBOrderByTerm op) {
    }

    @Override
    public void visit(MariaDBExists op) {
        print(op);
        visit(op.getExpr());
    }

    @Override
    public void visit(MariaDBStringExpression op) {
        print(op);
    }

    @Override
    public void visit(MariaDBBetweenOperation op) {
        print(op);
        visit(op.getExpr());
        visit(op.getLeft());
        visit(op.getRight());
    }

    @Override
    public void visit(MariaDBTableReference ref) {
    }

    @Override
    public void visit(MariaDBCollate collate) {
        print(collate);
        visit(collate.getExpectedValue());
    }

}
