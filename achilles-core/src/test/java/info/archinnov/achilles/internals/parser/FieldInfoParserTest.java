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

package info.archinnov.achilles.internals.parser;

import static info.archinnov.achilles.internals.apt.AptUtils.findFieldInType;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;

import org.junit.Before;
import org.junit.Test;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.TypeName;

import info.archinnov.achilles.internals.apt_utils.AbstractTestProcessor;
import info.archinnov.achilles.internals.metamodel.columns.ColumnInfo;
import info.archinnov.achilles.internals.metamodel.columns.ColumnType;
import info.archinnov.achilles.internals.parser.context.AccessorsExclusionContext;
import info.archinnov.achilles.internals.parser.context.EntityParsingContext;
import info.archinnov.achilles.internals.parser.context.FieldInfoContext;
import info.archinnov.achilles.internals.parser.context.GlobalParsingContext;
import info.archinnov.achilles.internals.sample_classes.parser.field_info.TestEntityForFieldInfo;
import info.archinnov.achilles.internals.strategy.naming.InternalNamingStrategy;
import info.archinnov.achilles.internals.strategy.naming.SnakeCaseNaming;
import info.archinnov.achilles.type.tuples.Tuple2;

public class FieldInfoParserTest extends AbstractTestProcessor {

    private final InternalNamingStrategy strategy = new SnakeCaseNaming();
    private GlobalParsingContext globalParsingContext = GlobalParsingContext.defaultContext();

    @Before
    public void setUp() {
        super.testEntityClass = TestEntityForFieldInfo.class;
    }

    @Test
    public void should_build_partition_column_type() throws Exception {
        setExec(aptUtils -> {
            final FieldInfoParser parser = new FieldInfoParser(aptUtils);
            final String className = TestEntityForFieldInfo.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final TypeName typeName = ClassName.get(TestEntityForFieldInfo.class);

            // @PartitionKey(1) private Long id;
            VariableElement elm = findFieldInType(typeElement, "id");

            final Tuple2<CodeBlock, ColumnType> codeBlock = parser.buildColumnType(globalParsingContext, elm, "id", typeName);
            assertThat(codeBlock._1().toString()).isEqualTo("info.archinnov.achilles.internals.metamodel.columns.ColumnType.PARTITION");
        });
        launchTest();
    }

    @Test
    public void should_build_clustering_column_type() throws Exception {
        setExec(aptUtils -> {
            final FieldInfoParser parser = new FieldInfoParser(aptUtils);
            final String className = TestEntityForFieldInfo.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final TypeName typeName = ClassName.get(TestEntityForFieldInfo.class);

            // @ClusteringColumn(1) private String clust1;
            VariableElement elm = findFieldInType(typeElement, "clust1");

            final Tuple2<CodeBlock, ColumnType> codeBlock = parser.buildColumnType(globalParsingContext, elm, "clust1", typeName);
            assertThat(codeBlock._1().toString()).isEqualTo("info.archinnov.achilles.internals.metamodel.columns.ColumnType.CLUSTERING");
        });
        launchTest();
    }

    @Test
    public void should_build_static_column_type() throws Exception {
        setExec(aptUtils -> {
            final FieldInfoParser parser = new FieldInfoParser(aptUtils);
            final String className = TestEntityForFieldInfo.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final TypeName typeName = ClassName.get(TestEntityForFieldInfo.class);

            // @Static private int staticCol;
            VariableElement elm = findFieldInType(typeElement, "staticCol");

            final Tuple2<CodeBlock, ColumnType> codeBlock = parser.buildColumnType(globalParsingContext, elm, "staticCol", typeName);
            assertThat(codeBlock._1().toString()).isEqualTo("info.archinnov.achilles.internals.metamodel.columns.ColumnType.STATIC");
        });
        launchTest();
    }

    @Test
    public void should_build_computed_column_type() throws Exception {
        setExec(aptUtils -> {
            final FieldInfoParser parser = new FieldInfoParser(aptUtils);
            final String className = TestEntityForFieldInfo.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final TypeName typeName = ClassName.get(TestEntityForFieldInfo.class);

            // @Computed(function = "writetime", targettargetColumnsaticCol") private Long computed;
            VariableElement elm = findFieldInType(typeElement, "computed");

            final Tuple2<CodeBlock, ColumnType> codeBlock = parser.buildColumnType(globalParsingContext, elm, "computed", typeName);
            assertThat(codeBlock._1().toString()).isEqualTo("info.archinnov.achilles.internals.metamodel.columns.ColumnType.COMPUTED");
        });
        launchTest();
    }

    @Test
    public void should_build_normal_column_type() throws Exception {
        setExec(aptUtils -> {
            final FieldInfoParser parser = new FieldInfoParser(aptUtils);
            final String className = TestEntityForFieldInfo.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final TypeName typeName = ClassName.get(TestEntityForFieldInfo.class);

            // @Column private String normal;
            VariableElement elm = findFieldInType(typeElement, "normal");

            final Tuple2<CodeBlock, ColumnType> codeBlock = parser.buildColumnType(globalParsingContext, elm, "normal", typeName);
            assertThat(codeBlock._1().toString()).isEqualTo("info.archinnov.achilles.internals.metamodel.columns.ColumnType.NORMAL");
        });
        launchTest();
    }

    @Test
    public void should_build_counter_column_type() throws Exception {
        setExec(aptUtils -> {
            final FieldInfoParser parser = new FieldInfoParser(aptUtils);
            final String className = TestEntityForFieldInfo.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final TypeName typeName = ClassName.get(TestEntityForFieldInfo.class);

            // @Counter private Long counter;
            VariableElement elm = findFieldInType(typeElement, "counter");

            final Tuple2<CodeBlock, ColumnType> codeBlock = parser.buildColumnType(globalParsingContext, elm, "counter", typeName);
            assertThat(codeBlock._1().toString().trim().replaceAll("\n", ""))
                    .isEqualTo("info.archinnov.achilles.internals.metamodel.columns.ColumnType.COUNTER");
        });
        launchTest();
    }

    @Test
    public void should_build_static_counter_column_type() throws Exception {
        setExec(aptUtils -> {
            final FieldInfoParser parser = new FieldInfoParser(aptUtils);
            final String className = TestEntityForFieldInfo.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final TypeName typeName = ClassName.get(TestEntityForFieldInfo.class);

            // @Static @Counter private Long staticCounter;
            VariableElement elm = findFieldInType(typeElement, "staticCounter");

            final Tuple2<CodeBlock, ColumnType> codeBlock = parser.buildColumnType(globalParsingContext, elm, "staticCounter", typeName);
            assertThat(codeBlock._1().toString().trim().replaceAll("\n", ""))
                    .isEqualTo("info.archinnov.achilles.internals.metamodel.columns.ColumnType.STATIC_COUNTER");
        });
        launchTest();
    }

    @Test
    public void should_fail_if_both_partition_and_clustering_column() throws Exception {
        setExec(aptUtils -> {
            final FieldInfoParser parser = new FieldInfoParser(aptUtils);
            final String className = TestEntityForFieldInfo.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final TypeName typeName = ClassName.get(TestEntityForFieldInfo.class);

            // @PartitionKey(1) @ClusteringColumn(1) private Long partitionAndClustering;
            VariableElement elm = findFieldInType(typeElement, "partitionAndClustering");

            parser.buildColumnType(globalParsingContext, elm, "partitionAndClustering", typeName);
        });
        failTestWithMessage("Field 'partitionAndClustering' in class " +
                "'info.archinnov.achilles.internals.sample_classes.parser.field_info.TestEntityForFieldInfo' " +
                "cannot have both @PartitionKey AND @ClusteringColumn annotations");
    }

    @Test
    public void should_fail_if_both_partition_and_static_column() throws Exception {
        setExec(aptUtils -> {
            final FieldInfoParser parser = new FieldInfoParser(aptUtils);
            final String className = TestEntityForFieldInfo.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final TypeName typeName = ClassName.get(TestEntityForFieldInfo.class);

            //  @PartitionKey(1) @Static private Long partitionAndStatic;
            VariableElement elm = findFieldInType(typeElement, "partitionAndStatic");

            parser.buildColumnType(globalParsingContext, elm, "partitionAndStatic", typeName);
        });
        failTestWithMessage("Field 'partitionAndStatic' in class " +
                "'info.archinnov.achilles.internals.sample_classes.parser.field_info.TestEntityForFieldInfo' " +
                "cannot have both @Static AND @PartitionKey annotations");
    }

    @Test
    public void should_fail_if_both_partition_and_computed_column() throws Exception {
        setExec(aptUtils -> {
            final FieldInfoParser parser = new FieldInfoParser(aptUtils);
            final String className = TestEntityForFieldInfo.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final TypeName typeName = ClassName.get(TestEntityForFieldInfo.class);

            // @PartitionKey(1) @Computed(function = "xx", targettargetColumnsx"}) private Long partitionAndComputed;
            VariableElement elm = findFieldInType(typeElement, "partitionAndComputed");

            parser.buildColumnType(globalParsingContext, elm, "partitionAndComputed", typeName);
        });
        failTestWithMessage("Field 'partitionAndComputed' in class " +
                "'info.archinnov.achilles.internals.sample_classes.parser.field_info.TestEntityForFieldInfo' " +
                "cannot have both @Computed AND @PartitionKey annotations");
    }

    @Test
    public void should_fail_if_both_clustering_and_static_column() throws Exception {
        setExec(aptUtils -> {
            final FieldInfoParser parser = new FieldInfoParser(aptUtils);
            final String className = TestEntityForFieldInfo.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final TypeName typeName = ClassName.get(TestEntityForFieldInfo.class);

            // @ClusteringColumn(0) @Static private String clusteringAndStatic;
            VariableElement elm = findFieldInType(typeElement, "clusteringAndStatic");

            parser.buildColumnType(globalParsingContext, elm, "clusteringAndStatic", typeName);
        });
        failTestWithMessage("Field 'clusteringAndStatic' in class " +
                "'info.archinnov.achilles.internals.sample_classes.parser.field_info.TestEntityForFieldInfo' " +
                "cannot have both @Static AND @ClusteringColumn annotations");
    }

    @Test
    public void should_fail_if_both_clustering_and_computed_column() throws Exception {
        setExec(aptUtils -> {
            final FieldInfoParser parser = new FieldInfoParser(aptUtils);
            final String className = TestEntityForFieldInfo.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final TypeName typeName = ClassName.get(TestEntityForFieldInfo.class);

            // @ClusteringColumn(0) @Computed(function = "xx", targettargetColumnsx"}) private String clusteringAndComputed;
            VariableElement elm = findFieldInType(typeElement, "clusteringAndComputed");

            parser.buildColumnType(globalParsingContext, elm, "clusteringAndComputed", typeName);
        });
        failTestWithMessage("Field 'clusteringAndComputed' in class " +
                "'info.archinnov.achilles.internals.sample_classes.parser.field_info.TestEntityForFieldInfo' " +
                "cannot have both @Computed AND @ClusteringColumn annotations");
    }

    @Test
    public void should_fail_if_both_static_and_computed_column() throws Exception {
        setExec(aptUtils -> {
            final FieldInfoParser parser = new FieldInfoParser(aptUtils);
            final String className = TestEntityForFieldInfo.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final TypeName typeName = ClassName.get(TestEntityForFieldInfo.class);

            // @Static @Computed(function = "writetime", targettargetColumnsaticCol") private int staticAndComputed;
            VariableElement elm = findFieldInType(typeElement, "staticAndComputed");

            parser.buildColumnType(globalParsingContext, elm, "staticAndComputed", typeName);
        });
        failTestWithMessage("Field 'staticAndComputed' in class " +
                "'info.archinnov.achilles.internals.sample_classes.parser.field_info.TestEntityForFieldInfo' " +
                "cannot have both @Computed AND @Static annotations");
    }

    @Test
    public void should_fail_if_partition_key_order_zero() throws Exception {
        setExec(aptUtils -> {
            final FieldInfoParser parser = new FieldInfoParser(aptUtils);
            final String className = TestEntityForFieldInfo.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final TypeName typeName = ClassName.get(TestEntityForFieldInfo.class);

            // @PartitionKey(0) private Long wrongPartitionOrder;
            VariableElement elm = findFieldInType(typeElement, "wrongPartitionOrder");
            AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils, globalParsingContext, elm);
            parser.buildColumnInfo(globalParsingContext, annotationTree, elm, "wrongPartitionOrder", typeName);
        });
        failTestWithMessage("@PartitionKey order on field 'wrongPartitionOrder' of class " +
                "'info.archinnov.achilles.internals.sample_classes.parser.field_info.TestEntityForFieldInfo' " +
                "should be > 0, the ordering starts at 1");
    }

    @Test
    public void should_fail_if_clustering_column_order_zero() throws Exception {
        setExec(aptUtils -> {
            final FieldInfoParser parser = new FieldInfoParser(aptUtils);
            final String className = TestEntityForFieldInfo.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final TypeName typeName = ClassName.get(TestEntityForFieldInfo.class);

            // @ClusteringColumn(0) private String wrongClusteringOrder;
            VariableElement elm = findFieldInType(typeElement, "wrongClusteringOrder");
            AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils,  globalParsingContext, elm);
            parser.buildColumnInfo(globalParsingContext, annotationTree,  elm, "wrongClusteringOrder", typeName);
        });
        failTestWithMessage("@ClusteringColumn order on field 'wrongClusteringOrder' of class " +
                "'info.archinnov.achilles.internals.sample_classes.parser.field_info.TestEntityForFieldInfo' " +
                "should be > 0, the ordering starts at 1");
    }

    @Test
    public void should_build_partition_column_info() throws Exception {
        setExec(aptUtils -> {
            final FieldInfoParser parser = new FieldInfoParser(aptUtils);
            final String className = TestEntityForFieldInfo.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final TypeName typeName = ClassName.get(TestEntityForFieldInfo.class);

            // @PartitionKey(1) private Long id;
            VariableElement elm = findFieldInType(typeElement, "id");
            AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils,  globalParsingContext, elm);
            final Tuple2<CodeBlock, ColumnInfo> codeBlock = parser.buildColumnInfo(globalParsingContext, annotationTree,  elm, "id", typeName);
            assertThat(codeBlock._1().toString().trim().replaceAll("\n", ""))
                    .isEqualTo("new info.archinnov.achilles.internals.metamodel.columns.PartitionKeyInfo(1, false)");
        });
        launchTest();
    }

    @Test
    public void should_build_clustering_column_info() throws Exception {
        setExec(aptUtils -> {
            final FieldInfoParser parser = new FieldInfoParser(aptUtils);
            final String className = TestEntityForFieldInfo.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final TypeName typeName = ClassName.get(TestEntityForFieldInfo.class);

            // @ClusteringColumn(value = 2, asc = false) private int clust2;
            VariableElement elm = findFieldInType(typeElement, "clust2");
            AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils,  globalParsingContext, elm);
            final Tuple2<CodeBlock, ColumnInfo> codeBlock = parser.buildColumnInfo(globalParsingContext, annotationTree,  elm, "clust2", typeName);
            assertThat(codeBlock._1().toString().trim().replaceAll("\n", ""))
                    .isEqualTo("new info.archinnov.achilles.internals.metamodel.columns.ClusteringColumnInfo(2, false, com.datastax.driver.core.ClusteringOrder.DESC)");
        });
        launchTest();
    }

    @Test
    public void should_build_static_column_info() throws Exception {
        setExec(aptUtils -> {
            final FieldInfoParser parser = new FieldInfoParser(aptUtils);
            final String className = TestEntityForFieldInfo.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final TypeName typeName = ClassName.get(TestEntityForFieldInfo.class);

            // @Static private int staticCol;
            VariableElement elm = findFieldInType(typeElement, "staticCol");
            AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils,  globalParsingContext, elm);
            final Tuple2<CodeBlock, ColumnInfo> codeBlock = parser.buildColumnInfo(globalParsingContext, annotationTree,  elm, "staticCol", typeName);
            assertThat(codeBlock._1().toString().trim().replaceAll("\n", ""))
                    .isEqualTo("new info.archinnov.achilles.internals.metamodel.columns.ColumnInfo(false)");
        });
        launchTest();
    }

    @Test
    public void should_build_computed_column_info() throws Exception {
        setExec(aptUtils -> {
            final FieldInfoParser parser = new FieldInfoParser(aptUtils);
            final String className = TestEntityForFieldInfo.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final TypeName typeName = ClassName.get(TestEntityForFieldInfo.class);

            // @Computed(function = "writetime", alias = "writetime", targettargetColumnstaticCol","normal"}, cqlClass = Long.class)
            // private Long computed;
            VariableElement elm = findFieldInType(typeElement, "computed");
            AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils,  globalParsingContext, elm);
            final Tuple2<CodeBlock, ColumnInfo> codeBlock = parser.buildColumnInfo(globalParsingContext, annotationTree,  elm, "computed", typeName);
            assertThat(codeBlock._1().toString().trim().replaceAll("\n", ""))
                    .isEqualTo("new info.archinnov.achilles.internals.metamodel.columns.ComputedColumnInfo(\"writetime\", \"writetime\", java.util.Arrays.asList(new String[]{\"staticCol\",\"normal\"}), java.lang.Long.class)");
        });
        launchTest();

    }

    @Test
    public void should_build_normal_column_info() throws Exception {
        setExec(aptUtils -> {
            final FieldInfoParser parser = new FieldInfoParser(aptUtils);
            final String className = TestEntityForFieldInfo.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final TypeName typeName = ClassName.get(TestEntityForFieldInfo.class);

            // @Column private String normal;
            VariableElement elm = findFieldInType(typeElement, "normal");
            AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils,  globalParsingContext, elm);
            final Tuple2<CodeBlock, ColumnInfo> codeBlock = parser.buildColumnInfo(globalParsingContext, annotationTree,  elm, "normal", typeName);
            assertThat(codeBlock._1().toString().trim().replaceAll("\n", ""))
                    .isEqualTo("new info.archinnov.achilles.internals.metamodel.columns.ColumnInfo(false)");
        });
        launchTest();
    }

    @Test
    public void should_build_frozen_column_info() throws Exception {
        setExec(aptUtils -> {
            final FieldInfoParser parser = new FieldInfoParser(aptUtils);
            final String className = TestEntityForFieldInfo.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final TypeName typeName = ClassName.get(TestEntityForFieldInfo.class);

            // @Column @Frozen private TestUDT udt;
            VariableElement elm = findFieldInType(typeElement, "udt");
            AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils,  globalParsingContext, elm);
            final Tuple2<CodeBlock, ColumnInfo> codeBlock = parser.buildColumnInfo(globalParsingContext, annotationTree,  elm, "udt", typeName);
            assertThat(codeBlock._1().toString().trim().replaceAll("\n", ""))
                    .isEqualTo("new info.archinnov.achilles.internals.metamodel.columns.ColumnInfo(true)");
        });
        launchTest();
    }

    @Test
    public void should_build_list_index_info() throws Exception {
        setExec(aptUtils -> {
            final FieldInfoParser parser = new FieldInfoParser(aptUtils);
            final String className = TestEntityForFieldInfo.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext context = new EntityParsingContext(typeElement, ClassName.get(TestEntityForFieldInfo.class), strategy, globalParsingContext);
            // @Index private List<String> indexedList;
            VariableElement elm = findFieldInType(typeElement, "indexedList");
            final AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils,  globalParsingContext, elm);

            final CodeBlock codeBlock = parser.buildNativeIndexInfo(annotationTree, elm, context)._1();
            assertThat(codeBlock.toString().trim().replaceAll("\n", ""))
                    .isEqualTo("info.archinnov.achilles.internals.metamodel.index.IndexInfo.forNative(info.archinnov.achilles.internals.metamodel.index.IndexType.COLLECTION, \"indexed_list_index\", \"\", \"\")");
        });
        launchTest();
    }

    @Test
    public void should_build_list_nested_index_info() throws Exception {
        setExec(aptUtils -> {
            final FieldInfoParser parser = new FieldInfoParser(aptUtils);
            final String className = TestEntityForFieldInfo.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext context = new EntityParsingContext(typeElement, ClassName.get(TestEntityForFieldInfo.class), strategy, globalParsingContext);

            // private List<@Index(name = "list_index") String> nestedIndexList;
            VariableElement elm = findFieldInType(typeElement, "nestedIndexList");
            final AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils,  globalParsingContext, elm);

            final CodeBlock codeBlock = parser.buildNativeIndexInfo(annotationTree, elm, context)._1();
            assertThat(codeBlock.toString().trim().replaceAll("\n", ""))
                    .isEqualTo("info.archinnov.achilles.internals.metamodel.index.IndexInfo.forNative(info.archinnov.achilles.internals.metamodel.index.IndexType.COLLECTION, \"list_index\", \"\", \"\")");
        });
        launchTest();
    }

    @Test
    public void should_build_list_not_index_info() throws Exception {
        setExec(aptUtils -> {
            final FieldInfoParser parser = new FieldInfoParser(aptUtils);
            final String className = TestEntityForFieldInfo.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext context = new EntityParsingContext(typeElement, ClassName.get(TestEntityForFieldInfo.class), strategy, globalParsingContext);

            // private List<String> notIndexedList;
            VariableElement elm = findFieldInType(typeElement, "notIndexedList");
            final AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils,  globalParsingContext, elm);

            final CodeBlock codeBlock = parser.buildNativeIndexInfo(annotationTree, elm, context)._1();
            assertThat(codeBlock.toString().trim().replaceAll("\n", ""))
                    .isEqualTo("info.archinnov.achilles.internals.metamodel.index.IndexInfo.noIndex()");
        });
        launchTest();
    }

    @Test
    public void should_build_map_entry_index_info() throws Exception {
        setExec(aptUtils -> {
            final FieldInfoParser parser = new FieldInfoParser(aptUtils);
            final String className = TestEntityForFieldInfo.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext context = new EntityParsingContext(typeElement, ClassName.get(TestEntityForFieldInfo.class), strategy, globalParsingContext);

            // @Index(indexClass = TestEntityForFieldInfo.class) private Map<Integer, String> indexedEntryMap;
            VariableElement elm = findFieldInType(typeElement, "indexedEntryMap");
            final AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils,  globalParsingContext, elm);

            final CodeBlock codeBlock = parser.buildNativeIndexInfo(annotationTree, elm, context)._1();
            assertThat(codeBlock.toString().trim().replaceAll("\n", ""))
                    .isEqualTo(
                            "info.archinnov.achilles.internals.metamodel.index.IndexInfo.forNative(info.archinnov.achilles.internals.metamodel.index.IndexType.CUSTOM, \"indexed_entry_map_index\", \"java.lang.Long\", \"\")");
        });
        launchTest();
    }

    @Test
    public void should_build_map_key_index_info() throws Exception {
        setExec(aptUtils -> {
            final FieldInfoParser parser = new FieldInfoParser(aptUtils);
            final String className = TestEntityForFieldInfo.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext context = new EntityParsingContext(typeElement, ClassName.get(TestEntityForFieldInfo.class), strategy, globalParsingContext);

            // private Map<@Index Integer, String> indexedKeyMap;
            VariableElement elm = findFieldInType(typeElement, "indexedKeyMap");
            final AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils,  globalParsingContext, elm);

            final CodeBlock codeBlock = parser.buildNativeIndexInfo(annotationTree, elm, context)._1();
            assertThat(codeBlock.toString().trim().replaceAll("\n", ""))
                    .isEqualTo(
                            "info.archinnov.achilles.internals.metamodel.index.IndexInfo.forNative(info.archinnov.achilles.internals.metamodel.index.IndexType.MAP_KEY, \"indexed_key_map_index\", \"\", \"\")");
        });
        launchTest();
    }

    @Test
    public void should_build_map_value_index_info() throws Exception {
        setExec(aptUtils -> {
            final FieldInfoParser parser = new FieldInfoParser(aptUtils);
            final String className = TestEntityForFieldInfo.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext context = new EntityParsingContext(typeElement, ClassName.get(TestEntityForFieldInfo.class), strategy, globalParsingContext);

            // private Map<Integer, @Index String> indexedValueMap;
            VariableElement elm = findFieldInType(typeElement, "indexedValueMap");
            final AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils,  globalParsingContext, elm);

            final CodeBlock codeBlock = parser.buildNativeIndexInfo(annotationTree, elm, context)._1();
            assertThat(codeBlock.toString().trim().replaceAll("\n", ""))
                    .isEqualTo(
                            "info.archinnov.achilles.internals.metamodel.index.IndexInfo.forNative(info.archinnov.achilles.internals.metamodel.index.IndexType.MAP_VALUE, \"indexed_value_map_index\", \"\", \"\")");
        });
        launchTest();
    }

    @Test
    public void should_fail_building_map_with_multiple_indices() throws Exception {
        setExec(aptUtils -> {
            final FieldInfoParser parser = new FieldInfoParser(aptUtils);
            final String className = TestEntityForFieldInfo.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext context = new EntityParsingContext(typeElement, ClassName.get(TestEntityForFieldInfo.class), strategy, globalParsingContext);

            // private Map<@Index Integer, @Index String> duplicatedIndicesForMap;
            VariableElement elm = findFieldInType(typeElement, "duplicatedIndicesForMap");
            final AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils,  globalParsingContext, elm);

            parser.buildNativeIndexInfo(annotationTree, elm, context);

        });
        failTestWithMessage("Cannot have @Index on Map key AND value type in field 'duplicatedIndicesForMap' of class 'info.archinnov.achilles.internals.sample_classes.parser.field_info.TestEntityForFieldInfo'",
                TestEntityForFieldInfo.class);
    }

    @Test
    public void should_build_not_indexed_map_index_info() throws Exception {
        setExec(aptUtils -> {
            final FieldInfoParser parser = new FieldInfoParser(aptUtils);
            final String className = TestEntityForFieldInfo.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext context = new EntityParsingContext(typeElement, ClassName.get(TestEntityForFieldInfo.class), strategy, globalParsingContext);

            // private Map<Integer, String> notIndexedMap;
            VariableElement elm = findFieldInType(typeElement, "notIndexedMap");
            final AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils,  globalParsingContext, elm);

            final CodeBlock codeBlock = parser.buildNativeIndexInfo(annotationTree, elm, context)._1();
            assertThat(codeBlock.toString().trim().replaceAll("\n", ""))
                    .isEqualTo("info.archinnov.achilles.internals.metamodel.index.IndexInfo.noIndex()");
        });
        launchTest();
    }

    @Test
    public void should_build_udt_index_info() throws Exception {
        setExec(aptUtils -> {
            final FieldInfoParser parser = new FieldInfoParser(aptUtils);
            final String className = TestEntityForFieldInfo.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext context = new EntityParsingContext(typeElement, ClassName.get(TestEntityForFieldInfo.class), strategy, globalParsingContext);
            // @Index private TestUDT indexedUdt;
            VariableElement elm = findFieldInType(typeElement, "indexedUdt");
            final AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils,  globalParsingContext, elm);

            final CodeBlock codeBlock = parser.buildNativeIndexInfo(annotationTree, elm, context)._1();
            assertThat(codeBlock.toString().trim().replaceAll("\n", ""))
                    .isEqualTo(
                            "info.archinnov.achilles.internals.metamodel.index.IndexInfo.forNative(info.archinnov.achilles.internals.metamodel.index.IndexType.NORMAL, \"indexed_udt_index\", \"\", \"\")");
        });
        launchTest();
    }

    @Test
    public void should_build_frozen_list_index_info() throws Exception {
        setExec(aptUtils -> {
            final FieldInfoParser parser = new FieldInfoParser(aptUtils);
            final String className = TestEntityForFieldInfo.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext context = new EntityParsingContext(typeElement, ClassName.get(TestEntityForFieldInfo.class), strategy, globalParsingContext);

            // @Index @Frozen private List<String> indexedFrozenList;
            VariableElement elm = findFieldInType(typeElement, "indexedFrozenList");
            final AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils,  globalParsingContext, elm);

            final CodeBlock codeBlock = parser.buildNativeIndexInfo(annotationTree, elm, context)._1();
            assertThat(codeBlock.toString().trim().replaceAll("\n", ""))
                    .isEqualTo(
                            "info.archinnov.achilles.internals.metamodel.index.IndexInfo.forNative(info.archinnov.achilles.internals.metamodel.index.IndexType.FULL, \"indexed_frozen_list_index\", \"\", \"\")");
        });
        launchTest();
    }

    @Test
    public void should_build_not_indexed_column_index_info() throws Exception {
        setExec(aptUtils -> {
            final FieldInfoParser parser = new FieldInfoParser(aptUtils);
            final String className = TestEntityForFieldInfo.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext context = new EntityParsingContext(typeElement, ClassName.get(TestEntityForFieldInfo.class), strategy, globalParsingContext);
            // @Column private String normal;
            VariableElement elm = findFieldInType(typeElement, "normal");
            final AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils,  globalParsingContext, elm);

            final CodeBlock codeBlock = parser.buildNativeIndexInfo(annotationTree, elm, context)._1();
            assertThat(codeBlock.toString().trim().replaceAll("\n", ""))
                    .isEqualTo("info.archinnov.achilles.internals.metamodel.index.IndexInfo.noIndex()");
        });
        launchTest();
    }

    @Test
    public void should_generate_field_info_for_consistencyLevel() throws Exception {
        setExec(aptUtils -> {
            FieldInfoParser parser = new FieldInfoParser(aptUtils);
            final String className = TestEntityForFieldInfo.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext context = new EntityParsingContext(typeElement, ClassName.get(TestEntityForFieldInfo.class), strategy, globalParsingContext);

            // @Enumerated(value = NAME) private ConsistencyLevel consistencyLevel
            VariableElement elm = findFieldInType(typeElement, "consistencyLevel");
            final AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils,  globalParsingContext, elm);

            FieldInfoContext fieldInfo = parser.buildFieldInfo(elm, annotationTree, context);

            assertThat(fieldInfo.codeBlock.toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/method_parser/should_generate_field_info_for_consistencyLevel.txt"));
        });
        launchTest();
    }

    @Test
    public void should_generate_field_info_for_primitiveBoolean() throws Exception {
        setExec(aptUtils -> {
            FieldInfoParser parser = new FieldInfoParser(aptUtils);
            final String className = TestEntityForFieldInfo.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext context = new EntityParsingContext(typeElement, ClassName.get(TestEntityForFieldInfo.class), strategy, globalParsingContext);

            // private boolean primitiveBoolean;
            VariableElement elm = findFieldInType(typeElement, "primitiveBoolean");
            final AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils,  globalParsingContext, elm);

            FieldInfoContext fieldInfo = parser.buildFieldInfo(elm, annotationTree, context);

            assertThat(fieldInfo.codeBlock.toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/method_parser/should_generate_field_info_for_primitiveBoolean.txt"));
        });
        launchTest();
    }

    @Test
    public void should_generate_field_info_for_objectBoolean() throws Exception {
        setExec(aptUtils -> {
            FieldInfoParser parser = new FieldInfoParser(aptUtils);
            final String className = TestEntityForFieldInfo.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext context = new EntityParsingContext(typeElement, ClassName.get(TestEntityForFieldInfo.class), strategy, globalParsingContext);

            // private Boolean objectBoolean;
            VariableElement elm = findFieldInType(typeElement, "objectBoolean");
            final AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils,  globalParsingContext, elm);

            FieldInfoContext fieldInfo = parser.buildFieldInfo(elm, annotationTree, context);

            assertThat(fieldInfo.codeBlock.toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/method_parser/should_generate_field_info_for_objectBoolean.txt"));
        });
        launchTest();
    }

    @Test
    public void should_generate_field_info_for_UpperCase() throws Exception {
        setExec(aptUtils -> {
            FieldInfoParser parser = new FieldInfoParser(aptUtils);
            final String className = TestEntityForFieldInfo.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext context = new EntityParsingContext(typeElement, ClassName.get(TestEntityForFieldInfo.class), strategy, globalParsingContext);

            // @Column("UpperCase")
            // private String upperCase;
            VariableElement elm = findFieldInType(typeElement, "upperCase");
            final AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils,  globalParsingContext, elm);

            FieldInfoContext fieldInfo = parser.buildFieldInfo(elm, annotationTree, context);

            assertThat(fieldInfo.codeBlock.toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/method_parser/should_generate_field_info_for_UpperCase.txt"));
        });
        launchTest();
    }

    @Test
    public void should_generate_field_info_for_map() throws Exception {
        setExec(aptUtils -> {
            FieldInfoParser parser = new FieldInfoParser(aptUtils);
            final String className = TestEntityForFieldInfo.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext context = new EntityParsingContext(typeElement, ClassName.get(TestEntityForFieldInfo.class), strategy, globalParsingContext);

            /*
                @EmptyCollectionIfNull
                private Map<@JSON TestUDT,
                        @EmptyCollectionIfNull @Frozen Map<Integer,
                                                    Tuple3<@Codec(value = IntToStringCodec.class) Integer,
                                                            Integer,
                                                            @Enumerated(value = ORDINAL) ConsistencyLevel>>> map;
             */
            VariableElement elm = findFieldInType(typeElement, "map");
            final AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils,  globalParsingContext, elm);

            FieldInfoContext fieldInfo = parser.buildFieldInfo(elm, annotationTree, context);

            assertThat(fieldInfo.codeBlock.toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/method_parser/should_generate_field_info_for_map.txt"));
        });
        launchTest();
    }

    @Test
    public void should_generate_field_info_for_column_with_no_setter() throws Exception {
        setExec(aptUtils -> {
            final FieldInfoParser parser = new FieldInfoParser(aptUtils);
            final String className = TestEntityForFieldInfo.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final List<AccessorsExclusionContext> exclusionContexts = Arrays.asList(new AccessorsExclusionContext("columnWithNoSetter", false, true));
            final EntityParsingContext context = new EntityParsingContext(typeElement,
                    ClassName.get(TestEntityForFieldInfo.class), strategy, exclusionContexts,
                    globalParsingContext);

            VariableElement elm = findFieldInType(typeElement, "columnWithNoSetter");

            final AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils,  globalParsingContext, elm);

            FieldInfoContext fieldInfo = parser.buildFieldInfo(elm, annotationTree, context);

            assertThat(fieldInfo.codeBlock.toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/method_parser/should_generate_field_info_for_column_with_no_setter.txt"));
        });
        launchTest();
    }

    @Test
    public void should_generate_field_info_for_public_final_columns() throws Exception {
        setExec(aptUtils -> {
            final FieldInfoParser parser = new FieldInfoParser(aptUtils);
            final String className = TestEntityForFieldInfo.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final List<AccessorsExclusionContext> exclusionContexts = Arrays.asList(new AccessorsExclusionContext("immutableColumn", true, true));
            final EntityParsingContext context = new EntityParsingContext(typeElement,
                    ClassName.get(TestEntityForFieldInfo.class), strategy, exclusionContexts,
                    globalParsingContext);

            VariableElement elm = findFieldInType(typeElement, "immutableColumn");

            final AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils,  globalParsingContext, elm);

            FieldInfoContext fieldInfo = parser.buildFieldInfo(elm, annotationTree, context);

            assertThat(fieldInfo.codeBlock.toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/method_parser/should_generate_field_info_for_public_final_columns.txt"));
        });
        launchTest();
    }

    @Test
    public void should_fail_compilation_when_no_getter() throws Exception {
        setExec(aptUtils -> {
            FieldInfoParser parser = new FieldInfoParser(aptUtils);
            final String className = TestEntityForFieldInfo.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext context = new EntityParsingContext(typeElement, ClassName.get(TestEntityForFieldInfo.class), strategy, globalParsingContext);

            // @Codec(value = IntToStringCodec.class) private Integer integer;
            VariableElement elm = findFieldInType(typeElement, "integer");
            final AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils,  globalParsingContext, elm);

            parser.buildFieldInfo(elm, annotationTree, context);
        });
        failTestWithMessage("Cannot find getter of names '[getInteger]' for field 'integer' in class 'info.archinnov.achilles.internals.sample_classes.parser.field_info.TestEntityForFieldInfo'");
    }

    @Test
    public void should_fail_compilation_when_no_setter() throws Exception {
        setExec(aptUtils -> {
            FieldInfoParser parser = new FieldInfoParser(aptUtils);
            final String className = TestEntityForFieldInfo.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext context = new EntityParsingContext(typeElement, ClassName.get(TestEntityForFieldInfo.class), strategy, globalParsingContext);

            // private TestUdt testUdt;
            VariableElement elm = findFieldInType(typeElement, "testUdt");
            final AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils, globalParsingContext, elm);

            parser.buildFieldInfo(elm, annotationTree, context);
        });
        failTestWithMessage("Cannot find setter 'void setTestUdt(info.archinnov.achilles.internals.sample_classes.parser.field.TestUDT value)' for field 'testUdt' in class 'info.archinnov.achilles.internals.sample_classes.parser.field_info.TestEntityForFieldInfo'");
    }

    @Test
    public void should_fail_compilation_when_no_suitable_setter() throws Exception {
        setExec(aptUtils -> {
            FieldInfoParser parser = new FieldInfoParser(aptUtils);
            final String className = TestEntityForFieldInfo.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext context = new EntityParsingContext(typeElement, ClassName.get(TestEntityForFieldInfo.class), strategy, globalParsingContext);

            // private Set<@Enumerated(value = ORDINAL) ConsistencyLevel> set;
            VariableElement elm = findFieldInType(typeElement, "set");
            final AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils,  globalParsingContext, elm);

            parser.buildFieldInfo(elm, annotationTree, context);
        });
        failTestWithMessage("Cannot find setter 'void setSet(java.util.Set<com.datastax.driver.core.ConsistencyLevel> value)' for field 'set' in class 'info.archinnov.achilles.internals.sample_classes.parser.field_info.TestEntityForFieldInfo'");
    }
}