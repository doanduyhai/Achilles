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

import static info.archinnov.achilles.internals.parser.TypeUtils.FIELD_INFO;

import java.util.Objects;

import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

import info.archinnov.achilles.internals.metamodel.columns.ColumnInfo;
import info.archinnov.achilles.internals.metamodel.columns.ColumnType;

public class FieldParsingContext {
    public final String fieldName;
    public final String className;
    public final String cqlColumn;
    public final EntityParsingContext entityContext;
    public final CodeBlock fieldInfoCode;
    public final TypeName entityRawType;
    public final ColumnType columnType;
    public final ColumnInfo columnInfo;
    public boolean buildExtractor;

    public FieldParsingContext(EntityParsingContext entityContext, TypeName entityRawType, FieldInfoContext fieldInfoContext) {
        this.entityRawType = entityRawType;
        this.fieldInfoCode = fieldInfoContext.codeBlock;
        this.fieldName = fieldInfoContext.fieldName;
        this.entityContext = entityContext;
        this.columnType = fieldInfoContext.columnType;
        this.columnInfo = fieldInfoContext.columnInfo;
        this.className = entityContext.className;
        this.cqlColumn = fieldInfoContext.cqlColumn;
        this.buildExtractor = true;
    }

    public FieldParsingContext(EntityParsingContext entityContext, TypeName entityRawType, FieldInfoContext fieldInfoContext, boolean buildExtractor) {
        this(entityContext, entityRawType, fieldInfoContext);
        this.buildExtractor = buildExtractor;
    }

    public FieldParsingContext noLambda(TypeName entityType, TypeName sourceType) {
        return new FieldParsingContext(entityContext, entityRawType, new FieldInfoContext(
                CodeBlock.builder().add("$T.<$T, $T> of($S, $S)", FIELD_INFO, entityType, sourceType,
                        fieldName, cqlColumn).build(),
                fieldName, cqlColumn, columnType, columnInfo), false);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldParsingContext that = (FieldParsingContext) o;
        return Objects.equals(fieldName, that.fieldName) &&
                Objects.equals(className, that.className);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldName, className);
    }

    public boolean hasProcessedUDT(TypeName rawUdtType) {
        return entityContext.globalContext.udtTypes.containsKey(rawUdtType);
    }

    public void addUDTMeta(TypeName rawUdtType, TypeSpec typeSpec) {
        entityContext.globalContext.udtTypes.put(rawUdtType, typeSpec);
    }
}
