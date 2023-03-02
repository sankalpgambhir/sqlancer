package sqlancer.mariadb.ast;

import sqlancer.common.ast.UnaryNode;

public class MariaDBCollate extends UnaryNode<MariaDBExpression> implements MariaDBExpression {

    private final String collate;

    public MariaDBCollate(MariaDBExpression expr, String text) {
        super(expr);
        this.collate = text;
    }

    @Override
    public String getOperatorRepresentation() {
        return String.format("COLLATE '%s'", collate);
    }

    @Override
    public OperatorKind getOperatorKind() {
        return OperatorKind.POSTFIX;
    }

}
