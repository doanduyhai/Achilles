/*
 * Copyright (C) 2012-2021 DuyHai DOAN
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datastax.driver.core.querybuilder;

import java.util.List;

import com.datastax.driver.core.CodecRegistry;

public class MapEntryClause extends Clause {


    private final String quotedColumnName;
    private final Object key;
    private final Object value;

    private MapEntryClause(String quotedColumnName, Object key, Object value) {
        this.quotedColumnName = quotedColumnName;
        this.key = key;
        this.value = value;
    }

    public static MapEntryClause of(String quotedColumnName, Object key, Object value) {
        return new MapEntryClause(quotedColumnName, key, value);
    }

    @Override
    void appendTo(StringBuilder sb, List<Object> variables, CodecRegistry codecRegistry) {
        sb.append(quotedColumnName).append("[").append(key.toString()).append("]")
                .append(" = ")
                .append(value.toString());
        if (!Utils.containsBindMarker(key)) {
            variables.add(key);
        }

        if (!Utils.containsBindMarker(value)) {
            variables.add(value);
        }
    }

    @Override
    String name() {
        return quotedColumnName;
    }

    @Override
    Object firstValue() {
        return containsBindMarker() ? null : key;
    }

    @Override
    boolean containsBindMarker() {
        return Utils.containsBindMarker(key) && Utils.containsBindMarker(value);
    }
}
