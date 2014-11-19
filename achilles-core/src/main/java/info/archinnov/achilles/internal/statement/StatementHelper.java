package info.archinnov.achilles.internal.statement;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Statement;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;

import java.util.List;

public class StatementHelper {

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

    public static boolean hasOnlyStaticColumns(List<PropertyMeta> pms) {
        boolean onlyStaticColumns = !pms.isEmpty();
        for (PropertyMeta propertyMeta : pms) {
            if (!propertyMeta.structure().isStaticColumn()) {
                onlyStaticColumns = false;
                break;
            }
        }
        return onlyStaticColumns;
    }
}
