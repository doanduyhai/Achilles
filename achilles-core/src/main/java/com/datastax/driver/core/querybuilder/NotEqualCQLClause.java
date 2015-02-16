package com.datastax.driver.core.querybuilder;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;

public class NotEqualCQLClause {

    public static Clause build(String columnName) {
        return new Clause.SimpleClause(columnName, "!=", bindMarker(columnName));
    }

    public static Clause build(String columnName, Object value) {
        return new Clause.SimpleClause(columnName, "!=", value);
    }
}
