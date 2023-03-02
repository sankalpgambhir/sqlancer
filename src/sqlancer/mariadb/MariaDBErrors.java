package sqlancer.mariadb;

import sqlancer.common.query.ExpectedErrors;

public final class MariaDBErrors {

    private MariaDBErrors() {
    }

    public static void addExpressionErrors(ExpectedErrors errors) {
        errors.add("BIGINT value is out of range"); // e.g., CAST(-('-1e500') AS SIGNED)
        errors.add("is not valid for CHARACTER SET");
    }

}
