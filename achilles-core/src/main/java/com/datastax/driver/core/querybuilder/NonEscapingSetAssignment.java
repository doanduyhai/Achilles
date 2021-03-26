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

import static com.datastax.driver.core.querybuilder.Utils.appendValue;

import java.util.List;

import com.datastax.driver.core.CodecRegistry;

public class NonEscapingSetAssignment extends Assignment.SetAssignment {

    private final Object value;

    public static final NonEscapingSetAssignment of(String name, Object value) {
        return new NonEscapingSetAssignment(name, value);
    }

    private NonEscapingSetAssignment(String name, Object value) {
        super(name, value);
        this.value = value;
    }

    @Override
    boolean isIdempotent() {
        return Utils.isIdempotent(value);
    }

    @Override
    void appendTo(StringBuilder sb, List<Object> variables, CodecRegistry codecRegistry) {
        sb.append(name).append('=');
        appendValue(value, codecRegistry, sb, variables);
    }

    @Override
    boolean containsBindMarker() {
        return Utils.containsBindMarker(value);
    }
}
