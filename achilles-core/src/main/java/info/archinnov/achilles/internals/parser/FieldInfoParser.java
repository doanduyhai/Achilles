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

package info.archinnov.achilles.internals.parser;

import static com.datastax.driver.core.TableMetadata.Order.ASC;
import static com.datastax.driver.core.TableMetadata.Order.DESC;
import static info.archinnov.achilles.internals.apt.AptUtils.*;
import static info.archinnov.achilles.internals.metamodel.columns.ColumnType.*;
import static info.archinnov.achilles.internals.parser.TypeUtils.*;
import static info.archinnov.achilles.internals.parser.validator.FieldValidator.validateAllowedFrozen;
import static info.archinnov.achilles.internals.parser.validator.FieldValidator.validateCompatibleColumnAnnotationsOnField;
import static java.util.Arrays.asList;
import static java.util.Optional.ofNullable;
import static org.apache.commons.lang3.StringUtils.isBlank;

import java.util.*;
import javax.lang.model.element.*;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;

import com.datastax.driver.core.TableMetadata;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.TypeName;

import info.archinnov.achilles.annotations.*;
import info.archinnov.achilles.internals.apt.AptUtils;
import info.archinnov.achilles.internals.metamodel.columns.*;
import info.archinnov.achilles.internals.metamodel.index.IndexType;
import info.archinnov.achilles.internals.parser.context.EntityParsingContext;
import info.archinnov.achilles.type.tuples.Tuple2;
import info.archinnov.achilles.type.tuples.Tuple3;
import info.archinnov.achilles.type.tuples.Tuple5;

public class FieldInfoParser {

    private final AptUtils aptUtils;

    public FieldInfoParser(AptUtils aptUtils) {
        this.aptUtils = aptUtils;
    }

    public Tuple5<CodeBlock, String, String, ColumnType, ColumnInfo> buildFieldInfo(VariableElement elm, AnnotationTree annotationTree, EntityParsingContext context) {
        final TypeElement classElm = context.entityTypeElement;
        final TypeName rawEntityClass = TypeName.get(aptUtils.erasure(classElm));
        final TypeName currentType = TypeName.get(elm.asType()).box();
        final String fieldName = elm.getSimpleName().toString();
        final String cqlColumn = ofNullable(elm.getAnnotation(Column.class))
                .map(x -> x.value().isEmpty() ? null : x.value())
                .orElse(context.namingStrategy.apply(fieldName));

        final ExecutableElement getter = aptUtils.findGetter(classElm, elm, deriveGetterName(elm));
        final ExecutableElement setter = aptUtils.findSetter(classElm, elm, deriveSetterName(elm));

        final Tuple2<CodeBlock, ColumnType> columnTypeCode = buildColumnType(elm, fieldName, rawEntityClass);
        final Tuple2<CodeBlock, ColumnInfo> columnInfoCode = buildColumnInfo(elm, fieldName, rawEntityClass);
        final CodeBlock indexInfoCode = buildIndexInfo(annotationTree, elm, context);

        CodeBlock getterLambda = CodeBlock.builder()
                .add("($T entity$$) -> entity$$.$L()", rawEntityClass, getter.getSimpleName().toString())
                .build();

        CodeBlock setterLambda = CodeBlock.builder()
                .add("($T entity$$, $T value$$) -> entity$$.$L(value$$)", rawEntityClass, currentType, setter.getSimpleName().toString())
                .build();

        return Tuple5.of(CodeBlock.builder()
                .add("new $T<>($L, $L, $S, $S, $L, $L, $L)", FIELD_INFO, getterLambda, setterLambda,
                        fieldName, cqlColumn, columnTypeCode._1(), columnInfoCode._1(), indexInfoCode)
                .build(), fieldName, cqlColumn, columnTypeCode._2(), columnInfoCode._2());
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

    protected Tuple2<CodeBlock, ColumnType> buildColumnType(VariableElement elm, String fieldName, TypeName rawEntityClass) {
        final CodeBlock.Builder builder = CodeBlock.builder();
        final Optional<PartitionKey> partitionKey = Optional.ofNullable(elm.getAnnotation(PartitionKey.class));
        final Optional<ClusteringColumn> clusteringColumn = Optional.ofNullable(elm.getAnnotation(ClusteringColumn.class));
        final Optional<Static> staticColumn = Optional.ofNullable(elm.getAnnotation(Static.class));
        final Optional<Computed> computed = Optional.ofNullable(elm.getAnnotation(Computed.class));
        final Optional<Counter> counter = Optional.ofNullable(elm.getAnnotation(Counter.class));

        validateCompatibleColumnAnnotationsOnField(aptUtils, fieldName, rawEntityClass, partitionKey, clusteringColumn, staticColumn, computed, counter);

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

    protected Tuple2<CodeBlock, ColumnInfo> buildColumnInfo(VariableElement elm, String fieldName, TypeName rawEntityClass) {
        final CodeBlock.Builder builder = CodeBlock.builder();
        final Optional<Frozen> frozen = Optional.ofNullable(elm.getAnnotation(Frozen.class));
        final Optional<PartitionKey> partitionKey = Optional.ofNullable(elm.getAnnotation(PartitionKey.class));
        final Optional<ClusteringColumn> clusteringColumn = Optional.ofNullable(elm.getAnnotation(ClusteringColumn.class));
        final Optional<? extends AnnotationMirror> computed = extractAnnotation(elm.getAnnotationMirrors(), Computed.class);

        validateAllowedFrozen(frozen, aptUtils, elm, fieldName, rawEntityClass);

        if (partitionKey.isPresent()) {
            final int order = partitionKey.get().value();
            aptUtils.validateTrue(order > 0, "@PartitionKey order on field '%s' of class '%s' should be > 0, the ordering starts at 1", fieldName, rawEntityClass);
            builder.add("new $T($L, $L)", PARTITION_KEY_INFO, order, frozen.isPresent());
            return Tuple2.of(builder.build(), new PartitionKeyInfo(order, frozen.isPresent()));

        } else if (clusteringColumn.isPresent()) {
            final int order = clusteringColumn.get().value();
            final TableMetadata.Order clusteringOrder = clusteringColumn.get().asc() ? ASC : DESC;
            aptUtils.validateTrue(order > 0, "@ClusteringColumn order on field '%s' of class '%s' should be > 0, the ordering starts at 1", fieldName, rawEntityClass);
            builder.add("new $T($L, $L, $T.$L)", CLUSTERING_COLUMN_INFO, order, frozen.isPresent(), CLUSTERING_ORDER, clusteringOrder.name());
            return Tuple2.of(builder.build(), new ClusteringColumnInfo(order, frozen.isPresent(), clusteringOrder));

        } else if (computed.isPresent()) {
            final AnnotationMirror annotationMirror = computed.get();
            final String function = getElementValue(annotationMirror, "function", String.class, false);
            final String alias = getElementValue(annotationMirror, "alias", String.class, false);
            final List<String> targetFields = getElementValueArray(annotationMirror, "targetColumns", String.class, false);
            final Optional<Class<Object>> cqlClassO = getElementValueClass(annotationMirror, "cqlClass", false);
            if (cqlClassO.isPresent()) {
                final Class<?> cqlClass = cqlClassO.get();
                final ClassName className = ClassName.get(cqlClass);
                final StringJoiner joiner = new StringJoiner(",");
                targetFields
                        .stream()
                        .forEach(x -> joiner.add("\"" + x + "\""));
                builder.add("new $T($S, $S, $T.asList(new String[]{$L}), $T.class)", COMPUTED_COLUMN_INFO, function, alias, ARRAYS, joiner.toString(), className);
                return Tuple2.of(builder.build(), new ComputedColumnInfo(function, alias, targetFields, cqlClass));
            } else {
                aptUtils.printError("Cannot find CQL class for annotation %s on field %s in class %s",
                        Computed.class.getCanonicalName(), elm.getSimpleName(), enclosingClass(elm).getQualifiedName());
                return null;
            }


        } else {
            builder.add("new $T($L)", COLUMN_INFO, frozen.isPresent() ? true : false);
            return Tuple2.of(builder.build(), new ColumnInfo(frozen.isPresent()));
        }
    }

    protected CodeBlock buildIndexInfo(AnnotationTree annotationTree, VariableElement elm, EntityParsingContext context) {
        final CodeBlock.Builder builder = CodeBlock.builder();
        final TypeMirror currentType = aptUtils.erasure(annotationTree.getCurrentType());
        final Optional<? extends AnnotationMirror> indexAnnot = aptUtils.extractAnnotation(elm.getAnnotationMirrors(), Index.class);
        final Name fieldName = elm.getSimpleName();
        final Name className = enclosingClass(elm).getQualifiedName();
        final boolean isFrozen = elm.getAnnotation(Frozen.class) != null;
        final boolean hasJSON = elm.getAnnotation(JSON.class) != null;
        if (currentType.getKind().isPrimitive()) {
            if (indexAnnot.isPresent()) {
                Tuple3<String, String, String> indexInfo = extractIndexInfo(indexAnnot.get(), elm, context);
                builder.add("new $T($T.$L, $S, $L, $S)", INDEX_INFO, INDEX_TYPE, IndexType.NORMAL, indexInfo._1(),
                        indexInfo._2(), indexInfo._3());
            } else {
                noIndex(builder);
            }

            return builder.build();
        }

        if (aptUtils.isAssignableFrom(List.class, currentType) || aptUtils.isAssignableFrom(Set.class, currentType)) {
            final AnnotationTree next = hasJSON ? annotationTree : annotationTree.next();
            if (indexAnnot.isPresent()) {
                Tuple3<String, String, String> indexInfo = extractIndexInfo(indexAnnot.get(), elm, context);
                buildIndexForListOrSet(builder, isFrozen, indexInfo);
            } else if (containsAnnotation(next.getAnnotations(), Index.class)) {
                Tuple3<String, String, String> indexInfo = extractIndexInfo(next.getAnnotations(), elm, context);
                buildIndexForListOrSet(builder, isFrozen, indexInfo);
            } else {
                noIndex(builder);
            }
        } else if (aptUtils.isAssignableFrom(Map.class, currentType)) {
            final List<AnnotationMirror> keyAnnotations = hasJSON ? annotationTree.getAnnotations() : annotationTree.next().getAnnotations();
            final List<AnnotationMirror> valueAnnotations = hasJSON ? annotationTree.getAnnotations() : annotationTree.next().next().getAnnotations();
            if (indexAnnot.isPresent()) {
                final String indexName = getIndexName(indexAnnot.get(), elm, context);
                final String indexClassName = getIndexClass(indexAnnot.get());
                final String indexOptions = getIndexOptions(indexAnnot.get());
                if (isFrozen) {
                    IndexType indexType = indexClassName.equals("null") ? IndexType.FULL : IndexType.CUSTOM;
                    builder.add("new $T($T.$L, $S, $L, $S)", INDEX_INFO, INDEX_TYPE, indexType, indexName,
                            indexClassName, indexOptions);
                } else {
                    IndexType indexType = indexClassName.equals("null") ? IndexType.MAP_ENTRY : IndexType.CUSTOM;
                    builder.add("new $T($T.$L, $S, $L, $S)", INDEX_INFO, INDEX_TYPE, indexType, indexName,
                            indexClassName, indexOptions);
                }
                aptUtils.validateFalse(containsAnnotation(keyAnnotations, Index.class),
                        "Cannot have @Index on Map AND key type in field '%s' of class '%s'", fieldName, className);
                aptUtils.validateFalse(containsAnnotation(valueAnnotations, Index.class),
                        "Cannot have @Index on Map AND value type in field '%s' of class '%s'", fieldName, className);
            } else if (containsAnnotation(keyAnnotations, Index.class)) {
                aptUtils.validateFalse(containsAnnotation(valueAnnotations, Index.class),
                        "Cannot have @Index on Map key AND value type in field '%s' of class '%s'", fieldName, className);
                Tuple3<String, String, String> indexInfo = extractIndexInfo(keyAnnotations, elm, context);
                IndexType indexType = indexInfo._2().equals("null") ? IndexType.MAP_KEY : IndexType.CUSTOM;
                builder.add("new $T($T.$L, $S, $L, $S)", INDEX_INFO, INDEX_TYPE, indexType, indexInfo._1(),
                        indexInfo._2(), indexInfo._3());
            } else if (containsAnnotation(valueAnnotations, Index.class)) {
                aptUtils.validateFalse(containsAnnotation(keyAnnotations, Index.class),
                        "Cannot have @Index on Map key AND value type in field '%s' of class '%s'", fieldName, className);
                Tuple3<String, String, String> indexInfo = extractIndexInfo(valueAnnotations, elm, context);
                IndexType indexType = indexInfo._2().equals("null") ? IndexType.COLLECTION : IndexType.CUSTOM;
                builder.add("new $T($T.$L, $S, $L, $S)", INDEX_INFO, INDEX_TYPE, indexType, indexInfo._1(),
                        indexInfo._2(), indexInfo._3());
            } else {
                noIndex(builder);
            }
        } else if (indexAnnot.isPresent()) {
            Tuple3<String, String, String> indexInfo = extractIndexInfo(indexAnnot.get(), elm, context);
            IndexType indexType = indexInfo._2().equals("null") ? IndexType.NORMAL : IndexType.CUSTOM;
            builder.add("new $T($T.$L, $S, $L, $S)", INDEX_INFO, INDEX_TYPE, indexType, indexInfo._1(),
                    indexInfo._2(), indexInfo._3());
        } else {
            noIndex(builder);
        }
        return builder.build();
    }

    private Tuple3<String, String, String> extractIndexInfo(AnnotationMirror annotationMirror, VariableElement elm, EntityParsingContext context) {
        final String indexName = getIndexName(annotationMirror, elm, context);
        final String indexClassName = getIndexClass(annotationMirror);
        return Tuple3.of(indexName, indexClassName, getIndexOptions(annotationMirror));
    }

    private Tuple3<String, String, String> extractIndexInfo(List<AnnotationMirror> annotationMirrors, VariableElement elm, EntityParsingContext context) {
        final AnnotationMirror annotationMirror = extractAnnotation(annotationMirrors, Index.class).get();
        final String indexName = getIndexName(annotationMirror, elm, context);
        final String indexClassName = getIndexClass(annotationMirror);
        return Tuple3.of(indexName, indexClassName, getIndexOptions(annotationMirror));
    }

    private void buildIndexForListOrSet(CodeBlock.Builder builder, boolean isFrozen, Tuple3<String, String, String> indexInfo) {
        if (isFrozen) {
            IndexType indexType = indexInfo._2().equals("null") ? IndexType.FULL : IndexType.CUSTOM;
            builder.add("new $T($T.$L, $S, $L, $S)", INDEX_INFO, INDEX_TYPE, indexType, indexInfo._1(),
                    indexInfo._2(), indexInfo._3());
        } else {
            IndexType indexType = indexInfo._2().equals("null") ? IndexType.COLLECTION : IndexType.CUSTOM;
            builder.add("new $T($T.$L, $S, $L, $S)", INDEX_INFO, INDEX_TYPE, indexType, indexInfo._1(),
                    indexInfo._2(), indexInfo._3());
        }
    }

    private void noIndex(CodeBlock.Builder builder) {
        builder.add("$T.noIndex()", INDEX_INFO);
    }

    private String getIndexName(AnnotationMirror annotationMirror, VariableElement elm, EntityParsingContext context) {
        final String indexName = getElementValue(annotationMirror, "name", String.class, true);
        return isBlank(indexName)
                ? context.namingStrategy.apply(elm.getSimpleName().toString() + "_index")
                : indexName;

    }

    private String getIndexOptions(AnnotationMirror annotationMirror) {
        return getElementValue(annotationMirror, "indexOptions", String.class, true);
    }

    private String getIndexClass(AnnotationMirror annotationMirror) {
        final Optional<Class<Object>> indexClass = getElementValueClass(annotationMirror, "indexClass", true);
        return indexClass
                .map(clazz -> clazz.equals(Object.class) ? "null" : clazz.getCanonicalName() + ".class")
                .orElseGet(() -> {
                    final Name name = getElementValueClassName(annotationMirror, "indexClass", false);
                    return name == null ? "null" : name.toString() + ".class";
                });

    }
}
