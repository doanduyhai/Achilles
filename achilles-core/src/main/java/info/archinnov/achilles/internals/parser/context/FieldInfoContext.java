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

package info.archinnov.achilles.internals.parser.context;

import com.squareup.javapoet.CodeBlock;

import info.archinnov.achilles.internals.metamodel.columns.ColumnInfo;
import info.archinnov.achilles.internals.metamodel.columns.ColumnType;
import info.archinnov.achilles.internals.metamodel.index.IndexInfo;
import info.archinnov.achilles.internals.utils.NamingHelper;

public class FieldInfoContext {

    public final CodeBlock codeBlock;
    public final String fieldName;
    public final String cqlColumn;
    public final String quotedCqlColumn;
    public final ColumnType columnType;
    public final ColumnInfo columnInfo;
    public final IndexInfo indexInfo;

    public FieldInfoContext(CodeBlock codeBlock, String fieldName, String cqlColumn, ColumnType columnType, ColumnInfo columnInfo, IndexInfo indexInfo) {
        this.codeBlock = codeBlock;
        this.fieldName = fieldName;
        this.cqlColumn = cqlColumn;
        this.indexInfo = indexInfo;
        this.quotedCqlColumn = NamingHelper.maybeQuote(cqlColumn);
        this.columnType = columnType;
        this.columnInfo = columnInfo;
    }
}
