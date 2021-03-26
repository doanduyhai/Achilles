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

import static info.archinnov.achilles.internals.parser.TypeUtils.FIELD_INFO;

import java.util.Collections;
import java.util.Objects;
import javax.lang.model.element.TypeElement;

import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

import info.archinnov.achilles.internals.metamodel.columns.ColumnInfo;
import info.archinnov.achilles.internals.metamodel.columns.ColumnType;
import info.archinnov.achilles.internals.metamodel.index.IndexInfo;
import info.archinnov.achilles.internals.parser.CodecFactory;
import info.archinnov.achilles.internals.parser.FieldParser.UDTMetaSignature;
import info.archinnov.achilles.internals.parser.validator.FieldValidator;
import info.archinnov.achilles.internals.parser.validator.TypeValidator;
import info.archinnov.achilles.internals.utils.NamingHelper;

public class FieldParsingContext {
    public final String fieldName;
    public final String className;
    public final String simpleClassName;
    public final String cqlColumn;
    public final String quotedCqlColumn;
    public final EntityParsingContext entityContext;
    public final CodeBlock fieldInfoCode;
    public final TypeName entityRawType;
    public final ColumnType columnType;
    public final ColumnInfo columnInfo;
    public final IndexInfo indexInfo;
    public boolean buildExtractor;

    public static FieldParsingContext forConfig(GlobalParsingContext parsingContext, TypeElement typeElement, TypeName typeName, String className, String fieldName) {
        return new FieldParsingContext(parsingContext, typeElement, typeName, className, fieldName);
    }

    private FieldParsingContext(GlobalParsingContext parsingContext, TypeElement typeElement, TypeName typeName, String className, String fieldName) {
        this.className = className;
        this.simpleClassName = className.replaceAll("([^.]+\\.)" ,"");
        this.fieldName = fieldName;
        this.entityContext = new EntityParsingContext(typeElement, typeName, parsingContext.namingStrategy, Collections.emptyList(), parsingContext);
        this.columnType = null;
        this.columnInfo = null;
        this.indexInfo = null;
        this.cqlColumn = null;
        this.quotedCqlColumn = null;
        this.entityRawType = null;
        this.fieldInfoCode = null;
        this.buildExtractor = false;
    }

    public FieldParsingContext(EntityParsingContext entityContext, TypeName entityRawType, FieldInfoContext fieldInfoContext) {
        this.entityRawType = entityRawType;
        this.fieldInfoCode = fieldInfoContext.codeBlock;
        this.fieldName = fieldInfoContext.fieldName;
        this.entityContext = entityContext;
        this.columnType = fieldInfoContext.columnType;
        this.columnInfo = fieldInfoContext.columnInfo;
        this.indexInfo = fieldInfoContext.indexInfo;
        this.className = entityContext.className;
        this.simpleClassName = className.replaceAll("([^.]+\\.)" ,"");
        this.cqlColumn = fieldInfoContext.cqlColumn;
        this.quotedCqlColumn = fieldInfoContext.quotedCqlColumn;
        this.buildExtractor = true;
    }

    public FieldParsingContext(EntityParsingContext entityContext, TypeName entityRawType, FieldInfoContext fieldInfoContext, boolean buildExtractor) {
        this(entityContext, entityRawType, fieldInfoContext);
        this.buildExtractor = buildExtractor;
    }

    public FieldParsingContext noLambda(TypeName entityType, TypeName sourceType) {
        return new FieldParsingContext(entityContext, entityRawType, new FieldInfoContext(
                CodeBlock.builder().add("$T.<$T, $T> of($S, $S, true)", FIELD_INFO, entityType, sourceType,
                        fieldName, cqlColumn).build(),
                fieldName, cqlColumn, columnType, columnInfo, indexInfo), false);
    }

    public FieldParsingContext forOptionalType(TypeName entityType, TypeName nestedType, boolean frozen) {
        return new FieldParsingContext(entityContext, entityRawType, new FieldInfoContext(
                CodeBlock.builder().add("$T.<$T, $T> of($S, $S, $L)", FIELD_INFO, entityType, nestedType,
                    cqlColumn, fieldName, frozen).build(),
                fieldName, cqlColumn, columnType, columnInfo, indexInfo), true);
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
        return Objects.hash(fieldName, className, cqlColumn, columnInfo);
    }

    public boolean equalsTo(FieldParsingContext o) {
        if (this == o) return true;
        return Objects.equals(this.fieldName, o.fieldName) &&
                Objects.equals(this.cqlColumn, o.cqlColumn) &&
                Objects.equals(this.columnInfo.frozen, o.columnInfo.frozen);
    }

    public String toStringForViewCheck() {
        final StringBuilder sb = new StringBuilder();
        sb.append("fieldName='").append(fieldName).append('\'');
        sb.append(", cqlColumn='").append(cqlColumn).append('\'');
        return sb.toString();
    }

    public boolean hasProcessedUDT(TypeName rawUdtType) {
        return entityContext.globalContext.udtTypes.containsKey(rawUdtType);
    }

    public void addUDTMeta(TypeName rawUdtType, TypeSpec typeSpec) {
        entityContext.globalContext.udtTypes.put(rawUdtType, typeSpec);
    }

    public void addUDTMetaSignature(TypeName rawUdtType, UDTMetaSignature udtMetaSignature) {
        entityContext.globalContext.udtMetaSignatures.put(rawUdtType, udtMetaSignature);
    }

    public UDTMetaSignature getUDTMetaSignature(TypeName rawUdtType) {
        return entityContext.globalContext.udtMetaSignatures.get(rawUdtType);
    }

    public String udtClassName() {
        return NamingHelper.upperCaseFirst(this.fieldName) + "_UDT";
    }

    public TypeValidator typeValidator() {
        return entityContext.globalContext.typeValidator();
    }

    public FieldValidator fieldValidator() {
        return entityContext.globalContext.fieldValidator();
    }

    public CodecFactory.CodecInfo getCodecFor(TypeName typeName) {
        return entityContext.getCodecFor(typeName);
    }
}
