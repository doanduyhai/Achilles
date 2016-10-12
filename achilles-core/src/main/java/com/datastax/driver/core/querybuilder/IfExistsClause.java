package com.datastax.driver.core.querybuilder;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.querybuilder.Clause.SimpleClause;

import java.util.List;

public class IfExistsClause {

    public static Clause build() {
        return new SimpleClause("IF EXISTS","",null) {
            @Override
            void appendTo(StringBuilder sb, List<Object> variables, CodecRegistry codecRegistry) {
                sb.append("EXISTS");
            }
        };
    }
}
