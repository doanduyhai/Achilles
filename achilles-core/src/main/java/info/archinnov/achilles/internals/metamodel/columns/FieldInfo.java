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

package info.archinnov.achilles.internals.metamodel.columns;

import info.archinnov.achilles.internals.metamodel.index.IndexInfo;
import info.archinnov.achilles.internals.metamodel.index.IndexType;
import info.archinnov.achilles.internals.parser.accessors.Getter;
import info.archinnov.achilles.internals.parser.accessors.Setter;
import info.archinnov.achilles.internals.utils.NamingHelper;

public class FieldInfo<ENTITY, VALUEFROM> {
    public final String fieldName;
    public final String cqlColumn;
    public final String quotedCqlColumn;
    public final ColumnType columnType;
    public final ColumnInfo columnInfo;
    public final IndexInfo indexInfo;
    public final Getter<ENTITY, VALUEFROM> getter;
    public final Setter<ENTITY, VALUEFROM> setter;

    public FieldInfo(Getter<ENTITY, VALUEFROM> getter, Setter<ENTITY, VALUEFROM> setter,
                     String fieldName, String cqlColumn,
                     ColumnType columnType, ColumnInfo columnInfo, IndexInfo indexInfo) {
        this.fieldName = fieldName;
        this.cqlColumn = cqlColumn;
        this.quotedCqlColumn = NamingHelper.maybeQuote(cqlColumn);
        this.getter = getter;
        this.setter = setter;
        this.columnType = columnType;
        this.columnInfo = columnInfo;
        this.indexInfo = indexInfo;
    }

    private FieldInfo(String cqlColumn, String fieldName) {
        this.cqlColumn = cqlColumn;
        this.quotedCqlColumn = NamingHelper.maybeQuote(cqlColumn);
        this.fieldName = fieldName;
        this.getter = null;
        this.setter = null;
        this.columnType = null;
        this.columnInfo = null;
        this.indexInfo = null;
    }

    private FieldInfo(String cqlColumn, String fieldName, boolean frozen) {
        this.cqlColumn = cqlColumn;
        this.quotedCqlColumn = NamingHelper.maybeQuote(cqlColumn);
        this.fieldName = fieldName;
        this.getter = null;
        this.setter = null;
        this.columnType = null;
        this.columnInfo = new ColumnInfo(frozen);
        this.indexInfo = null;
    }

    public static <A, B> FieldInfo of(String cqlColumn, String fieldName) {
        return new FieldInfo<A, B>(cqlColumn, fieldName);
    }

    public static <A, B> FieldInfo of(String cqlColumn, String fieldName, boolean frozen) {
        return new FieldInfo<A, B>(cqlColumn, fieldName, frozen);
    }

    public boolean hasIndex() {
        return indexInfo.type != IndexType.NONE;
    }
}
