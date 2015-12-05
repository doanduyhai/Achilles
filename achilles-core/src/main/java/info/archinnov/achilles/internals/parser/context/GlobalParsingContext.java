/*
 * Copyright (C) 2012-2015 DuyHai DOAN
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

package info.archinnov.achilles.internals.parser.context;

import static info.archinnov.achilles.internals.strategy.field_filtering.FieldFilter.EXPLICIT_ENTITY_FIELD_FILTER;
import static info.archinnov.achilles.internals.strategy.field_filtering.FieldFilter.EXPLICIT_UDT_FIELD_FILTER;

import java.util.HashMap;
import java.util.Map;

import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

import info.archinnov.achilles.internals.strategy.field_filtering.FieldFilter;
import info.archinnov.achilles.internals.strategy.naming.CaseSensitiveNaming;
import info.archinnov.achilles.internals.strategy.naming.InternalNamingStrategy;
import info.archinnov.achilles.internals.strategy.naming.LowerCaseNaming;
import info.archinnov.achilles.internals.strategy.naming.SnakeCaseNaming;
import info.archinnov.achilles.internals.strategy.types_nesting.FrozenNestedTypeStrategy;
import info.archinnov.achilles.internals.strategy.types_nesting.NestedTypesStrategy;
import info.archinnov.achilles.type.strategy.InsertStrategy;
import info.archinnov.achilles.type.strategy.NamingStrategy;

public class GlobalParsingContext {

    public InsertStrategy insertStrategy = InsertStrategy.ALL_FIELDS;
    public InternalNamingStrategy namingStrategy = new LowerCaseNaming();
    public FieldFilter fieldFilter = EXPLICIT_ENTITY_FIELD_FILTER;
    public FieldFilter udtFieldFilter = EXPLICIT_UDT_FIELD_FILTER;
    public Map<TypeName, TypeSpec> udtTypes = new HashMap<>();
    public NestedTypesStrategy nestedTypesStrategy = new FrozenNestedTypeStrategy();

    public GlobalParsingContext(InsertStrategy insertStrategy, NamingStrategy namingStrategy,
                                FieldFilter fieldFilter, FieldFilter udtFieldFilter, NestedTypesStrategy nestedTypesStrategy) {
        this.insertStrategy = insertStrategy;
        this.fieldFilter = fieldFilter;
        this.udtFieldFilter = udtFieldFilter;
        this.nestedTypesStrategy = nestedTypesStrategy;
        this.namingStrategy = mapNamingStrategy(namingStrategy);
    }

    public GlobalParsingContext() {
    }

    private InternalNamingStrategy mapNamingStrategy(NamingStrategy namingStrategy) {
        switch (namingStrategy) {
            case SNAKE_CASE:
                return new SnakeCaseNaming();
            case CASE_SENSITIVE:
                return new CaseSensitiveNaming();
            default:
                return new LowerCaseNaming();
        }
    }
}
