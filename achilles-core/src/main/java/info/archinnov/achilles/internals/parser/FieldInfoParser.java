/*
 * Copyright (C) 2012-2016 DuyHai DOAN
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

package info.archinnov.achilles.internals.parser;

import static com.datastax.driver.core.ClusteringOrder.ASC;
import static com.datastax.driver.core.ClusteringOrder.DESC;
import static info.archinnov.achilles.internals.apt.AptUtils.*;
import static info.archinnov.achilles.internals.metamodel.columns.ColumnType.*;
import static info.archinnov.achilles.internals.parser.TypeUtils.*;
import static java.util.Arrays.asList;
import static java.util.Optional.ofNullable;
import static org.apache.commons.lang3.StringUtils.isBlank;

import java.util.*;
import javax.lang.model.element.*;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;

import com.datastax.driver.core.ClusteringOrder;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.TypeName;

import info.archinnov.achilles.annotations.*;
import info.archinnov.achilles.internals.apt.AptUtils;
import info.archinnov.achilles.internals.metamodel.columns.*;
import info.archinnov.achilles.internals.metamodel.index.IndexInfo;
import info.archinnov.achilles.internals.metamodel.index.IndexType;
import info.archinnov.achilles.internals.parser.context.EntityParsingContext;
import info.archinnov.achilles.internals.parser.context.FieldInfoContext;
import info.archinnov.achilles.internals.parser.context.GlobalParsingContext;
import info.archinnov.achilles.internals.parser.context.IndexInfoContext;
import info.archinnov.achilles.type.TypedMap;
import info.archinnov.achilles.type.tuples.Tuple2;

public class FieldInfoParser {

    private final AptUtils aptUtils;

    public FieldInfoParser(AptUtils aptUtils) {
        this.aptUtils = aptUtils;
    }

    public FieldInfoContext buildFieldInfo(VariableElement elm, AnnotationTree annotationTree, EntityParsingContext context) {
        final TypeElement classElm = context.entityTypeElement;
        final TypeName rawEntityClass = TypeName.get(aptUtils.erasure(classElm));
        final TypeName currentType = TypeName.get(elm.asType()).box();
        final String fieldName = elm.getSimpleName().toString();
        final String cqlColumn = ofNullable(elm.getAnnotation(Column.class))
                .map(x -> x.value().isEmpty() ? null : x.value())
                .orElse(context.namingStrategy.apply(fieldName));

        final ExecutableElement getter = aptUtils.findGetter(classElm, elm, deriveGetterName(elm));
        final ExecutableElement setter = aptUtils.findSetter(classElm, elm, deriveSetterName(elm));

        final Tuple2<CodeBlock, ColumnType> columnTypeCode = buildColumnType(context.globalContext, elm, fieldName, rawEntityClass);
        final Tuple2<CodeBlock, ColumnInfo> columnInfoCode = buildColumnInfo(context.globalContext, annotationTree, elm, fieldName, rawEntityClass);
        final Tuple2<CodeBlock, IndexInfo> indexInfoCode = buildIndexInfo(annotationTree, elm, context);

        CodeBlock getterLambda = CodeBlock.builder()
                .add("($T entity$$) -> entity$$.$L()", rawEntityClass, getter.getSimpleName().toString())
                .build();

        CodeBlock setterLambda = CodeBlock.builder()
                .add("($T entity$$, $T value$$) -> entity$$.$L(value$$)", rawEntityClass, currentType, setter.getSimpleName().toString())
                .build();

        return new FieldInfoContext(CodeBlock.builder()
                .add("new $T<>($L, $L, $S, $S, $L, $L, $L)", FIELD_INFO, getterLambda, setterLambda,
                        fieldName, cqlColumn, columnTypeCode._1(), columnInfoCode._1(), indexInfoCode)
                .build(), fieldName, cqlColumn, columnTypeCode._2(), columnInfoCode._2(), indexInfoCode._2());
    }

    protected List<String> deriveGetterName(VariableElement elm) {
        final String fieldName = elm.getSimpleName().toString();
        final TypeMirror typeMirror = elm.asType();
        String camelCase = fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
        if (typeMirror.getKind() == TypeKind.BOOLEAN) {
            return asList("is" + camelCase, "get" + camelCase);
        } else {
            return asList("get" + camelCase);
        }
    }

    protected String deriveSetterName(VariableElement elm) {
        final String fieldName = elm.getSimpleName().toString();
        String setter = "set" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
        return setter;
    }

    protected Tuple2<CodeBlock, ColumnType> buildColumnType(GlobalParsingContext context, VariableElement elm, String fieldName, TypeName rawEntityClass) {
        final CodeBlock.Builder builder = CodeBlock.builder();
        final Optional<PartitionKey> partitionKey = Optional.ofNullable(elm.getAnnotation(PartitionKey.class));
        final Optional<ClusteringColumn> clusteringColumn = Optional.ofNullable(elm.getAnnotation(ClusteringColumn.class));
        final Optional<Static> staticColumn = Optional.ofNullable(elm.getAnnotation(Static.class));
        final Optional<Computed> computed = Optional.ofNullable(elm.getAnnotation(Computed.class));
        final Optional<Counter> counter = Optional.ofNullable(elm.getAnnotation(Counter.class));

        context.fieldValidator().validateCompatibleColumnAnnotationsOnField(aptUtils, fieldName, rawEntityClass, partitionKey, clusteringColumn, staticColumn, computed, counter);

        if (partitionKey.isPresent()) {
            builder.add("$T.$L", COLUMN_TYPE, PARTITION.name());
            return Tuple2.of(builder.build(), PARTITION);
        } else if (clusteringColumn.isPresent()) {
            builder.add("$T.$L", COLUMN_TYPE, CLUSTERING.name());
            return Tuple2.of(builder.build(), CLUSTERING);
        } else if (staticColumn.isPresent() && counter.isPresent()) {
            builder.add("$T.$L", COLUMN_TYPE, STATIC_COUNTER.name());
            return Tuple2.of(builder.build(), STATIC_COUNTER);
        } else if (staticColumn.isPresent()) {
            builder.add("$T.$L", COLUMN_TYPE, STATIC.name());
            return Tuple2.of(builder.build(), STATIC);
        } else if (computed.isPresent()) {
            builder.add("$T.$L", COLUMN_TYPE, COMPUTED.name());
            return Tuple2.of(builder.build(), COMPUTED);
        } else if (counter.isPresent()) {
            builder.add("$T.$L", COLUMN_TYPE, COUNTER.name());
            return Tuple2.of(builder.build(), COUNTER);
        } else {
            builder.add("$T.$L", COLUMN_TYPE, NORMAL.name());
            return Tuple2.of(builder.build(), NORMAL);
        }
    }

    protected Tuple2<CodeBlock, ColumnInfo> buildColumnInfo(GlobalParsingContext context, AnnotationTree annotationTree, VariableElement elm, String fieldName, TypeName rawEntityClass) {
        final CodeBlock.Builder builder = CodeBlock.builder();
        final boolean isFrozen = containsAnnotation(annotationTree, Frozen.class);
        final Optional<TypedMap> partitionKey = extractTypedMap(annotationTree, PartitionKey.class);
        final Optional<TypedMap> clusteringColumn = extractTypedMap(annotationTree, ClusteringColumn.class);
        final Optional<TypedMap> computed = extractTypedMap(annotationTree, Computed.class);

        context.fieldValidator().validateAllowedFrozen(isFrozen, aptUtils, elm, fieldName, rawEntityClass);

        if (partitionKey.isPresent()) {
            final int order = partitionKey.get().getTyped("order");
            aptUtils.validateTrue(order > 0, "@PartitionKey order on field '%s' of class '%s' should be > 0, the ordering starts at 1", fieldName, rawEntityClass);
            builder.add("new $T($L, $L)", PARTITION_KEY_INFO, order, isFrozen);
            return Tuple2.of(builder.build(), new PartitionKeyInfo(order, isFrozen));

        } else if (clusteringColumn.isPresent()) {
            final int order = clusteringColumn.get().getTyped("order");
            final ClusteringOrder clusteringOrder = clusteringColumn.get().<Boolean>getTyped("asc") ? ASC : DESC;
            aptUtils.validateTrue(order > 0, "@ClusteringColumn order on field '%s' of class '%s' should be > 0, the ordering starts at 1", fieldName, rawEntityClass);
            builder.add("new $T($L, $L, $T.$L)", CLUSTERING_COLUMN_INFO, order, isFrozen, CLUSTERING_ORDER, clusteringOrder.name());
            return Tuple2.of(builder.build(), new ClusteringColumnInfo(order, isFrozen, clusteringOrder));

        } else if (computed.isPresent()) {
            final TypedMap typedMap = computed.get();
            final String function = typedMap.getTyped("function");
            final String alias = typedMap.getTyped("alias");
            final List<String> targetColumns = typedMap.getTyped("targetColumns");
            final Class<?> cqlClass = typedMap.getTyped("cqlClass");
            final ClassName className = ClassName.get(cqlClass);
            final StringJoiner joiner = new StringJoiner(",");

            for (String x : targetColumns) {
                joiner.add("\"" + x + "\"");
            }

            builder.add("new $T($S, $S, $T.asList(new String[]{$L}), $T.class)", COMPUTED_COLUMN_INFO, function, alias, ARRAYS, joiner.toString(), className);
            return Tuple2.of(builder.build(), new ComputedColumnInfo(function, alias, targetColumns, cqlClass));


        } else {
            builder.add("new $T($L)", COLUMN_INFO, isFrozen);
            return Tuple2.of(builder.build(), new ColumnInfo(isFrozen));
        }
    }

    protected Tuple2<CodeBlock, IndexInfo> buildIndexInfo(AnnotationTree annotationTree, VariableElement elm, EntityParsingContext context) {
        final CodeBlock.Builder builder = CodeBlock.builder();
        final TypeMirror currentType = aptUtils.erasure(annotationTree.getCurrentType());
        final Optional<TypedMap> indexAnnot = extractTypedMap(annotationTree, Index.class);

        final IndexInfo indexInfo;

        final Name fieldName = elm.getSimpleName();
        final Name className = enclosingClass(elm).getQualifiedName();
        final boolean isFrozen = containsAnnotation(annotationTree, Frozen.class);
        final boolean hasJSON = containsAnnotation(annotationTree, JSON.class);
        if (currentType.getKind().isPrimitive()) {
            if (indexAnnot.isPresent()) {
                final IndexInfoContext indexInfoContext = indexAnnot.get()
                        .<IndexInfoContext>getTyped("indexInfoContext")
                        .computeIndexName(elm, context);
                builder.add("new $T($T.$L, $S, $S, $S)", INDEX_INFO, INDEX_TYPE, IndexType.NORMAL, indexInfoContext.indexName,
                        indexInfoContext.indexClassName, indexInfoContext.indexOptions);

                indexInfo = new IndexInfo(IndexType.NORMAL, indexInfoContext.indexName, indexInfoContext.indexClassName, indexInfoContext.indexOptions);

            } else {
                noIndex(builder);
                indexInfo = IndexInfo.noIndex();
            }

            return Tuple2.of(builder.build(), indexInfo);
        }

        if (aptUtils.isAssignableFrom(List.class, currentType) || aptUtils.isAssignableFrom(Set.class, currentType)) {
            final AnnotationTree next = hasJSON ? annotationTree : annotationTree.next();
            if (indexAnnot.isPresent()) {
                final IndexInfoContext indexInfoContext = indexAnnot.get()
                        .<IndexInfoContext>getTyped("indexInfoContext")
                        .computeIndexName(elm, context);
                indexInfo = buildIndexForListOrSet(builder, isFrozen, indexInfoContext);
            } else if (containsAnnotation(next, Index.class)) {
                final TypedMap typedMap = extractTypedMap(next, Index.class).get();
                indexInfo = buildIndexForListOrSet(builder, isFrozen, typedMap.<IndexInfoContext>getTyped("indexInfoContext").computeIndexName(elm, context));
            } else {
                noIndex(builder);
                indexInfo = IndexInfo.noIndex();
            }
        } else if (aptUtils.isAssignableFrom(Map.class, currentType)) {
            final AnnotationTree keyAnnotationTree = hasJSON ? annotationTree : annotationTree.next();
            final AnnotationTree valueAnnotationTree = hasJSON ? annotationTree : annotationTree.next().next();
            if (indexAnnot.isPresent()) {
                final IndexInfoContext indexInfoContext = indexAnnot.get().<IndexInfoContext>getTyped("indexInfoContext").computeIndexName(elm, context);

                if (isFrozen) {
                    IndexType indexType = isBlank(indexInfoContext.indexClassName) ? IndexType.FULL : IndexType.CUSTOM;
                    builder.add("new $T($T.$L, $S, $S, $S)", INDEX_INFO, INDEX_TYPE, indexType, indexInfoContext.indexName,
                            indexInfoContext.indexClassName, indexInfoContext.indexOptions);

                    indexInfo = new IndexInfo(indexType, indexInfoContext.indexName, indexInfoContext.indexClassName, indexInfoContext.indexOptions);
                } else {
                    IndexType indexType = isBlank(indexInfoContext.indexClassName) ? IndexType.MAP_ENTRY : IndexType.CUSTOM;
                    builder.add("new $T($T.$L, $S, $S, $S)", INDEX_INFO, INDEX_TYPE, indexType, indexInfoContext.indexName,
                            indexInfoContext.indexClassName, indexInfoContext.indexOptions);
                    indexInfo = new IndexInfo(indexType, indexInfoContext.indexName, indexInfoContext.indexClassName, indexInfoContext.indexOptions);
                }
                aptUtils.validateFalse(containsAnnotation(keyAnnotationTree, Index.class),
                        "Cannot have @Index on Map AND key type in field '%s' of class '%s'", fieldName, className);
                aptUtils.validateFalse(containsAnnotation(valueAnnotationTree, Index.class),
                        "Cannot have @Index on Map AND value type in field '%s' of class '%s'", fieldName, className);
            } else if (containsAnnotation(keyAnnotationTree, Index.class)) {
                aptUtils.validateFalse(containsAnnotation(valueAnnotationTree, Index.class),
                        "Cannot have @Index on Map key AND value type in field '%s' of class '%s'", fieldName, className);
                IndexInfoContext indexInfoContext = extractTypedMap(keyAnnotationTree, Index.class)
                    .get()
                    .<IndexInfoContext>getTyped("indexInfoContext")
                    .computeIndexName(elm, context);
                IndexType indexType = isBlank(indexInfoContext.indexClassName) ? IndexType.MAP_KEY : IndexType.CUSTOM;
                builder.add("new $T($T.$L, $S, $S, $S)", INDEX_INFO, INDEX_TYPE, indexType, indexInfoContext.indexName,
                        indexInfoContext.indexClassName, indexInfoContext.indexOptions);
                indexInfo = new IndexInfo(indexType, indexInfoContext.indexName, indexInfoContext.indexClassName, indexInfoContext.indexOptions);

            } else if (containsAnnotation(valueAnnotationTree, Index.class)) {
                aptUtils.validateFalse(containsAnnotation(keyAnnotationTree, Index.class),
                        "Cannot have @Index on Map key AND value type in field '%s' of class '%s'", fieldName, className);
                IndexInfoContext indexInfoContext = extractTypedMap(valueAnnotationTree, Index.class)
                    .get()
                    .<IndexInfoContext>getTyped("indexInfoContext")
                    .computeIndexName(elm, context);
                IndexType indexType = isBlank(indexInfoContext.indexClassName) ? IndexType.COLLECTION : IndexType.CUSTOM;
                builder.add("new $T($T.$L, $S, $S, $S)", INDEX_INFO, INDEX_TYPE, indexType, indexInfoContext.indexName,
                        indexInfoContext.indexClassName, indexInfoContext.indexOptions);

                indexInfo = new IndexInfo(indexType, indexInfoContext.indexName, indexInfoContext.indexClassName, indexInfoContext.indexOptions);
            } else {
                noIndex(builder);
                indexInfo = IndexInfo.noIndex();
            }
        } else if (indexAnnot.isPresent()) {
            final IndexInfoContext indexInfoContext = indexAnnot.get()
                    .<IndexInfoContext>getTyped("indexInfoContext")
                    .computeIndexName(elm, context);
            IndexType indexType = isBlank(indexInfoContext.indexClassName) ? IndexType.NORMAL : IndexType.CUSTOM;
            builder.add("new $T($T.$L, $S, $S, $S)", INDEX_INFO, INDEX_TYPE, indexType, indexInfoContext.indexName,
                    indexInfoContext.indexClassName, indexInfoContext.indexOptions);
            indexInfo = new IndexInfo(indexType, indexInfoContext.indexName, indexInfoContext.indexClassName, indexInfoContext.indexOptions);
        } else {
            noIndex(builder);
            indexInfo = IndexInfo.noIndex();
        }
        return Tuple2.of(builder.build(), indexInfo);
    }

    private IndexInfo buildIndexForListOrSet(CodeBlock.Builder builder, boolean isFrozen, IndexInfoContext indexInfo) {
        if (isFrozen) {
            IndexType indexType = isBlank(indexInfo.indexClassName) ? IndexType.FULL : IndexType.CUSTOM;
            builder.add("new $T($T.$L, $S, $S, $S)", INDEX_INFO, INDEX_TYPE, indexType, indexInfo.indexName,
                    indexInfo.indexClassName, indexInfo.indexOptions);
            return new IndexInfo(indexType, indexInfo.indexName, indexInfo.indexClassName, indexInfo.indexOptions);
        } else {
            IndexType indexType = isBlank(indexInfo.indexClassName) ? IndexType.COLLECTION : IndexType.CUSTOM;
            builder.add("new $T($T.$L, $S, $S, $S)", INDEX_INFO, INDEX_TYPE, indexType, indexInfo.indexName,
                    indexInfo.indexClassName, indexInfo.indexOptions);
            return new IndexInfo(indexType, indexInfo.indexName, indexInfo.indexClassName, indexInfo.indexOptions);
        }
    }

    private void noIndex(CodeBlock.Builder builder) {
        builder.add("$T.noIndex()", INDEX_INFO);
    }
}
