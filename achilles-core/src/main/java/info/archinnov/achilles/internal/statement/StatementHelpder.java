package info.archinnov.achilles.internal.statement;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Statement;

public class StatementHelpder {

    public static String maybeGetQueryString(Statement statement) {
        if (statement instanceof RegularStatement) {
            return ((RegularStatement)statement).getQueryString();
        } else if (statement instanceof BoundStatement) {
            return ((BoundStatement) statement).preparedStatement().getQueryString();
        } else {
            return "<batch statement>";
        }
    }

    public static String maybeGetNormalizedQueryString(Statement statement) {
        return maybeGetQueryString(statement).toLowerCase().trim();
    }
}
