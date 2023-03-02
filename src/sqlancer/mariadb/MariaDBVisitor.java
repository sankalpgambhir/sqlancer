package sqlancer.mariadb;

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

public interface MariaDBVisitor {

    void visit(MariaDBTableReference ref);

    void visit(MariaDBConstant constant);

    void visit(MariaDBColumnReference column);

    void visit(MariaDBUnaryPostfixOperation column);

    void visit(MariaDBComputableFunction f);

    void visit(MariaDBBinaryLogicalOperation op);

    void visit(MariaDBSelect select);

    void visit(MariaDBBinaryComparisonOperation op);

    void visit(MariaDBCastOperation op);

    void visit(MariaDBInOperation op);

    void visit(MariaDBBinaryOperation op);

    void visit(MariaDBOrderByTerm op);

    void visit(MariaDBExists op);

    void visit(MariaDBStringExpression op);

    void visit(MariaDBBetweenOperation op);

    void visit(MariaDBCollate collate);

    default void visit(MariaDBExpression expr) {
        if (expr instanceof MariaDBConstant) {
            visit((MariaDBConstant) expr);
        } else if (expr instanceof MariaDBColumnReference) {
            visit((MariaDBColumnReference) expr);
        } else if (expr instanceof MariaDBUnaryPostfixOperation) {
            visit((MariaDBUnaryPostfixOperation) expr);
        } else if (expr instanceof MariaDBComputableFunction) {
            visit((MariaDBComputableFunction) expr);
        } else if (expr instanceof MariaDBBinaryLogicalOperation) {
            visit((MariaDBBinaryLogicalOperation) expr);
        } else if (expr instanceof MariaDBSelect) {
            visit((MariaDBSelect) expr);
        } else if (expr instanceof MariaDBBinaryComparisonOperation) {
            visit((MariaDBBinaryComparisonOperation) expr);
        } else if (expr instanceof MariaDBCastOperation) {
            visit((MariaDBCastOperation) expr);
        } else if (expr instanceof MariaDBInOperation) {
            visit((MariaDBInOperation) expr);
        } else if (expr instanceof MariaDBBinaryOperation) {
            visit((MariaDBBinaryOperation) expr);
        } else if (expr instanceof MariaDBOrderByTerm) {
            visit((MariaDBOrderByTerm) expr);
        } else if (expr instanceof MariaDBExists) {
            visit((MariaDBExists) expr);
        } else if (expr instanceof MariaDBStringExpression) {
            visit((MariaDBStringExpression) expr);
        } else if (expr instanceof MariaDBBetweenOperation) {
            visit((MariaDBBetweenOperation) expr);
        } else if (expr instanceof MariaDBTableReference) {
            visit((MariaDBTableReference) expr);
        } else if (expr instanceof MariaDBCollate) {
            visit((MariaDBCollate) expr);
        } else {
            throw new AssertionError(expr);
        }
    }

    static String asString(MariaDBExpression expr) {
        MariaDBToStringVisitor visitor = new MariaDBToStringVisitor();
        visitor.visit(expr);
        return visitor.get();
    }

    static String asExpectedValues(MariaDBExpression expr) {
        MariaDBExpectedValueVisitor visitor = new MariaDBExpectedValueVisitor();
        visitor.visit(expr);
        return visitor.get();
    }

}
