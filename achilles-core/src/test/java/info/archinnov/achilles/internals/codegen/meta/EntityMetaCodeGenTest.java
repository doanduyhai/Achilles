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

package info.archinnov.achilles.internals.codegen.meta;

import static info.archinnov.achilles.internals.codegen.TypeParsingResultConsumer.getTypeParsingResults;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import javax.lang.model.element.TypeElement;

import org.junit.Test;

import com.squareup.javapoet.TypeSpec;

import info.archinnov.achilles.internals.apt_utils.AbstractTestProcessor;
import info.archinnov.achilles.internals.apt_utils.AptAssertOK;
import info.archinnov.achilles.internals.cassandra_version.V3_0;
import info.archinnov.achilles.internals.codegen.TypeParsingResultConsumer;
import info.archinnov.achilles.internals.metamodel.AbstractEntityProperty.EntityType;
import info.archinnov.achilles.internals.parser.FieldParser;
import info.archinnov.achilles.internals.parser.context.AccessorsExclusionContext;
import info.archinnov.achilles.internals.parser.context.GlobalParsingContext;
import info.archinnov.achilles.internals.sample_classes.parser.entity.*;
import info.archinnov.achilles.internals.sample_classes.parser.view.TestViewCounter;
import info.archinnov.achilles.internals.sample_classes.parser.view.TestViewSensorByType;
import info.archinnov.achilles.internals.sample_classes.parser.view.TestViewStatic;
import info.archinnov.achilles.internals.sample_classes.parser.view.TestViewWithEntityAnnotation;
import info.archinnov.achilles.internals.strategy.field_filtering.FieldFilter;
import info.archinnov.achilles.internals.strategy.naming.SnakeCaseNaming;
import info.archinnov.achilles.type.strategy.InsertStrategy;

public class EntityMetaCodeGenTest extends AbstractTestProcessor
        implements TypeParsingResultConsumer {

    private static final GlobalParsingContext context = GlobalParsingContext.defaultContext();

    @Test
    public void should_build_entity_with_simple_partition_key() throws Exception {
        setExec(aptUtils -> {
            final String className = TestEntityWithSimplePartitionKey.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            final EntityMetaCodeGen builder = new EntityMetaCodeGen(aptUtils);
            final List<FieldParser.FieldMetaSignature> parsingResults = getTypeParsingResults(aptUtils, typeElement, context);
            final TypeSpec typeSpec = builder.buildEntityMeta(EntityType.TABLE, typeElement, context, parsingResults, emptyList()).sourceCode;

            assertThat(buildSource(typeSpec)).isEqualTo(
                    readCodeBlockFromFile("expected_code/entity_meta_builder/should_build_entity_with_simple_partition_key.txt"));
        });
        launchTest(TestEntityWithSimplePartitionKey.class);
    }

    @Test
    public void should_build_entity_with_composite_partition_key() throws Exception {
        setExec(aptUtils -> {
            final String className = TestEntityWithCompositePartitionKey.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            final EntityMetaCodeGen builder = new EntityMetaCodeGen(aptUtils);
            final List<FieldParser.FieldMetaSignature> parsingResults = getTypeParsingResults(aptUtils, typeElement, context);
            final TypeSpec typeSpec = builder.buildEntityMeta(EntityType.TABLE, typeElement, context, parsingResults, emptyList()).sourceCode;

            assertThat(buildSource(typeSpec)).isEqualTo(
                    readCodeBlockFromFile("expected_code/entity_meta_builder/should_build_entity_with_composite_partition_key.txt"));
        });
        launchTest(TestEntityWithCompositePartitionKey.class);
    }

    @Test
    public void should_build_entity_with_clustering_column() throws Exception {
        setExec(aptUtils -> {
            final String className = TestEntityWithClusteringColumns.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            final EntityMetaCodeGen builder = new EntityMetaCodeGen(aptUtils);
            final List<FieldParser.FieldMetaSignature> parsingResults = getTypeParsingResults(aptUtils, typeElement, context);
            final TypeSpec typeSpec = builder.buildEntityMeta(EntityType.TABLE, typeElement, context, parsingResults, emptyList()).sourceCode;

            assertThat(buildSource(typeSpec)).isEqualTo(
                    readCodeBlockFromFile("expected_code/entity_meta_builder/should_build_entity_with_clustering_column.txt"));
        });
        launchTest(TestEntityWithClusteringColumns.class);
    }

    @Test
    public void should_build_entity_with_counter_column() throws Exception {
        setExec(aptUtils -> {
            final String className = TestEntityWithCounterColumn.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            final EntityMetaCodeGen builder = new EntityMetaCodeGen(aptUtils);
            final List<FieldParser.FieldMetaSignature> parsingResults = getTypeParsingResults(aptUtils, typeElement, context);
            final TypeSpec typeSpec = builder.buildEntityMeta(EntityType.TABLE, typeElement, context, parsingResults, emptyList()).sourceCode;

            assertThat(buildSource(typeSpec)).isEqualTo(
                    readCodeBlockFromFile("expected_code/entity_meta_builder/should_build_entity_with_counter_column.txt"));
        });
        launchTest(TestEntityWithCounterColumn.class);
    }

    @Test
    public void should_build_entity_with_static_counter_column() throws Exception {
        setExec(aptUtils -> {
            final String className = TestEntityWithStaticCounterColumn.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            final EntityMetaCodeGen builder = new EntityMetaCodeGen(aptUtils);
            final List<FieldParser.FieldMetaSignature> parsingResults = getTypeParsingResults(aptUtils, typeElement, context);
            final TypeSpec typeSpec = builder.buildEntityMeta(EntityType.TABLE, typeElement, context, parsingResults, emptyList()).sourceCode;

            assertThat(buildSource(typeSpec)).isEqualTo(
                    readCodeBlockFromFile("expected_code/entity_meta_builder/should_build_entity_with_static_counter_column.txt"));
        });
        launchTest(TestEntityWithStaticCounterColumn.class);
    }

    @Test
    public void should_build_entity_with_static_column() throws Exception {
        setExec(aptUtils -> {
            final String className = TestEntityWithStaticColumn.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            final EntityMetaCodeGen builder = new EntityMetaCodeGen(aptUtils);
            final List<FieldParser.FieldMetaSignature> parsingResults = getTypeParsingResults(aptUtils, typeElement, context);
            final TypeSpec typeSpec = builder.buildEntityMeta(EntityType.TABLE, typeElement, context, parsingResults, emptyList()).sourceCode;

            assertThat(buildSource(typeSpec)).isEqualTo(
                    readCodeBlockFromFile("expected_code/entity_meta_builder/should_build_entity_with_static_column.txt"));
        });
        launchTest(TestEntityWithStaticColumn.class);
    }

    @Test
    public void should_build_entity_with_computed_column() throws Exception {
        setExec(aptUtils -> {
            final String className = TestEntityWithComputedColumn.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            final EntityMetaCodeGen builder = new EntityMetaCodeGen(aptUtils);
            final List<FieldParser.FieldMetaSignature> parsingResults = getTypeParsingResults(aptUtils, typeElement, context);
            final TypeSpec typeSpec = builder.buildEntityMeta(EntityType.TABLE, typeElement, context, parsingResults, emptyList()).sourceCode;

            assertThat(buildSource(typeSpec)).isEqualTo(
                    readCodeBlockFromFile("expected_code/entity_meta_builder/should_build_entity_with_computed_column.txt"));
        });
        launchTest(TestEntityWithComputedColumn.class);
    }

    @Test
    public void should_build_inherited_entity() throws Exception {
        setExec(aptUtils -> {
            final String className = TestEntityAsChild.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            final EntityMetaCodeGen builder = new EntityMetaCodeGen(aptUtils);
            final List<FieldParser.FieldMetaSignature> parsingResults = getTypeParsingResults(aptUtils, typeElement, context);
            final TypeSpec typeSpec = builder.buildEntityMeta(EntityType.TABLE, typeElement, context, parsingResults, emptyList()).sourceCode;

            assertThat(buildSource(typeSpec)).isEqualTo(
                    readCodeBlockFromFile("expected_code/entity_meta_builder/should_build_inherited_entity.txt"));
        });
        launchTest(TestEntityAsChild.class);
    }

    @Test
    public void should_build_entity_with_static_annotations() throws Exception {
        setExec(aptUtils -> {
            final String className = TestEntityWithStaticAnnotations.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            final EntityMetaCodeGen builder = new EntityMetaCodeGen(aptUtils);
            final List<FieldParser.FieldMetaSignature> parsingResults = getTypeParsingResults(aptUtils, typeElement, context);
            final TypeSpec typeSpec = builder.buildEntityMeta(EntityType.TABLE, typeElement, context, parsingResults, emptyList()).sourceCode;

            assertThat(buildSource(typeSpec)).isEqualTo(
                    readCodeBlockFromFile("expected_code/entity_meta_builder/should_build_entity_with_static_annotations.txt"));
        });
        launchTest(TestEntityWithStaticAnnotations.class);
    }

    @Test
    public void should_build_entity_with_complex_types() throws Exception {
        setExec(aptUtils -> {
            final String className = TestEntityWithComplexTypes.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            final EntityMetaCodeGen builder = new EntityMetaCodeGen(aptUtils);
            final List<FieldParser.FieldMetaSignature> parsingResults = getTypeParsingResults(aptUtils, typeElement, context);
            final TypeSpec typeSpec = builder.buildEntityMeta(EntityType.TABLE, typeElement, context, parsingResults, emptyList()).sourceCode;

            assertThat(buildSource(typeSpec)).isEqualTo(
                    readCodeBlockFromFile("expected_code/entity_meta_builder/should_build_entity_with_complex_types.txt"));
        });
        launchTest(TestEntityWithComplexTypes.class);
    }

    @Test
    public void should_build_entity_with_complex_counter_types() throws Exception {
        setExec(aptUtils -> {
            final String className = TestEntityWithComplexCounters.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            final EntityMetaCodeGen builder = new EntityMetaCodeGen(aptUtils);
            final List<FieldParser.FieldMetaSignature> parsingResults = getTypeParsingResults(aptUtils, typeElement, context);
            final TypeSpec typeSpec = builder.buildEntityMeta(EntityType.TABLE, typeElement, context, parsingResults, emptyList()).sourceCode;

            assertThat(buildSource(typeSpec)).isEqualTo(
                    readCodeBlockFromFile("expected_code/entity_meta_builder/should_build_entity_with_complex_counter_types.txt"));
        });
        launchTest(TestEntityWithComplexCounters.class);
    }

    @Test
    public void should_build_entity_with_complex_indices() throws Exception {
        setExec(aptUtils -> {
            final String className = TestEntityWithComplexIndices.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            final EntityMetaCodeGen builder = new EntityMetaCodeGen(aptUtils);
            final List<FieldParser.FieldMetaSignature> parsingResults = getTypeParsingResults(aptUtils, typeElement, context);
            final TypeSpec typeSpec = builder.buildEntityMeta(EntityType.TABLE, typeElement, context, parsingResults, emptyList()).sourceCode;

            assertThat(buildSource(typeSpec)).isEqualTo(
                    readCodeBlockFromFile("expected_code/entity_meta_builder/should_build_entity_with_complex_indices.txt"));
        });
        launchTest(TestEntityWithComplexIndices.class);
    }

    @Test
    public void should_build_entity_with_implicit_field_parsing() throws Exception {
        setExec(aptUtils -> {
            final String className = TestEntityWithImplicitFieldParsing.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            GlobalParsingContext globalParsingContext = new GlobalParsingContext(V3_0.INSTANCE,
                    InsertStrategy.ALL_FIELDS, new SnakeCaseNaming(), FieldFilter.IMPLICIT_ENTITY_FIELD_FILTER,
                    FieldFilter.IMPLICIT_UDT_FIELD_FILTER, Optional.empty());

            final EntityMetaCodeGen builder = new EntityMetaCodeGen(aptUtils);
            final List<FieldParser.FieldMetaSignature> parsingResults = getTypeParsingResults(aptUtils, typeElement, globalParsingContext);
            final TypeSpec typeSpec = builder.buildEntityMeta(EntityType.TABLE, typeElement, globalParsingContext, parsingResults, emptyList()).sourceCode;

            assertThat(buildSource(typeSpec)).isEqualTo(
                    readCodeBlockFromFile("expected_code/entity_meta_builder/should_build_entity_with_implicit_field_parsing.txt"));
        });
        launchTest(TestEntityWithImplicitFieldParsing.class);
    }

    @Test
    public void should_build_entity_with_custom_constructor() throws Exception {
        final AptAssertOK aptAssertOK = aptUtils -> {
            final String className = TestEntityWithCustomConstructor.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            final EntityMetaCodeGen builder = new EntityMetaCodeGen(aptUtils);
            final List<AccessorsExclusionContext> exclusionContexts = Arrays.asList(
                    new AccessorsExclusionContext("id", false, true),
                    new AccessorsExclusionContext("date", false, true),
                    new AccessorsExclusionContext("value", false, true));
            final List<FieldParser.FieldMetaSignature> fieldMetaSignatures = getTypeParsingResults(aptUtils, typeElement, exclusionContexts, context);
            final TypeSpec typeSpec = builder.buildEntityMeta(EntityType.TABLE, typeElement, context, fieldMetaSignatures, fieldMetaSignatures).sourceCode;

            assertThat(buildSource(typeSpec)).isEqualTo(
                    readCodeBlockFromFile("expected_code/entity_meta_builder/should_build_entity_with_custom_constructor.txt"));
        }; setExec(aptAssertOK);
        launchTest(TestEntityWithCustomConstructor.class);
    }

    @Test
    public void should_build_entity_with_custom_constructor_with_declared_fields() throws Exception {
        final AptAssertOK aptAssertOK = aptUtils -> {
            final String className = TestEntityWithCustomConstructorAndDeclaredFields.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            final EntityMetaCodeGen builder = new EntityMetaCodeGen(aptUtils);
            final List<AccessorsExclusionContext> exclusionContexts = Arrays.asList(
                    new AccessorsExclusionContext("id", false, true),
                    new AccessorsExclusionContext("date", false, true),
                    new AccessorsExclusionContext("value", false, true));
            final List<FieldParser.FieldMetaSignature> fieldMetaSignatures = getTypeParsingResults(aptUtils, typeElement, exclusionContexts, context);
            final TypeSpec typeSpec = builder.buildEntityMeta(EntityType.TABLE, typeElement, context, fieldMetaSignatures, fieldMetaSignatures).sourceCode;

            assertThat(buildSource(typeSpec)).isEqualTo(
                    readCodeBlockFromFile("expected_code/entity_meta_builder/should_build_entity_with_custom_constructor_with_declared_fields.txt"));
        }; setExec(aptAssertOK);
        launchTest(TestEntityWithCustomConstructorAndDeclaredFields.class);
    }


    @Test
    public void should_build_view_meta() throws Exception {
        setExec(aptUtils -> {
            final String className = TestViewSensorByType.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            final EntityMetaCodeGen builder = new EntityMetaCodeGen(aptUtils);
            final List<FieldParser.FieldMetaSignature> parsingResults = getTypeParsingResults(aptUtils, typeElement, context);
            final TypeSpec typeSpec = builder.buildEntityMeta(EntityType.VIEW, typeElement, context, parsingResults, emptyList()).sourceCode;

            assertThat(buildSource(typeSpec)).isEqualTo(
                    readCodeBlockFromFile("expected_code/entity_meta_builder/should_build_view_meta.txt"));
        });
        launchTest(TestViewSensorByType.class);
    }

    @Test
    public void should_fail_building_abstract_class() throws Exception {
        setExec(aptUtils -> {
            final String className = TestEntityWithAbstractClass.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            final EntityMetaCodeGen builder = new EntityMetaCodeGen(aptUtils);
            final List<FieldParser.FieldMetaSignature> parsingResults = getTypeParsingResults(aptUtils, typeElement, context);
            builder.buildEntityMeta(EntityType.TABLE, typeElement, context, parsingResults, emptyList());

        });
        failTestWithMessage(
                "Bean type 'info.archinnov.achilles.internals.sample_classes.parser.entity.TestEntityWithAbstractClass' should not be abstract",
                TestEntityWithAbstractClass.class);
    }

    @Test
    public void should_fail_building_class_with_no_public_constructor() throws Exception {
        setExec(aptUtils -> {
            final String className = TestEntityWithNoPublicConstructor.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            final EntityMetaCodeGen builder = new EntityMetaCodeGen(aptUtils);
            final List<FieldParser.FieldMetaSignature> parsingResults = getTypeParsingResults(aptUtils, typeElement, context);
            builder.buildEntityMeta(EntityType.TABLE, typeElement, context, parsingResults, emptyList());
        });
        failTestWithMessage(
                "Entity type 'info.archinnov.achilles.internals.sample_classes.parser.entity.TestEntityWithNoPublicConstructor' should",
                TestEntityWithNoPublicConstructor.class);
    }

    @Test
    public void should_fail_building_class_with_duplicated_cql_name() throws Exception {
        setExec(aptUtils -> {
            final String className = TestEntityWithDuplicateCQLColumn.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            final EntityMetaCodeGen builder = new EntityMetaCodeGen(aptUtils);
            final List<FieldParser.FieldMetaSignature> parsingResults = getTypeParsingResults(aptUtils, typeElement, context);
            builder.buildEntityMeta(EntityType.TABLE, typeElement, context, parsingResults, emptyList());
        });
        failTestWithMessage(
                "The class 'info.archinnov.achilles.internals.sample_classes.parser.entity.TestEntityWithDuplicateCQLColumn' already contains a cql column with name 'value'",
                TestEntityWithDuplicateCQLColumn.class);
    }

    @Test
    public void should_fail_building_class_with_no_partition_key() throws Exception {
        setExec(aptUtils -> {
            final String className = TestEntityWithNoPartitionKey.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            final EntityMetaCodeGen builder = new EntityMetaCodeGen(aptUtils);
            final List<FieldParser.FieldMetaSignature> parsingResults = getTypeParsingResults(aptUtils, typeElement, context);
            builder.buildEntityMeta(EntityType.TABLE, typeElement, context, parsingResults, emptyList());
        });
        failTestWithMessage(
                "The class 'info.archinnov.achilles.internals.sample_classes.parser.entity.TestEntityWithNoPartitionKey' should have at least 1 partition key (@PartitionKey)",
                TestEntityWithNoPartitionKey.class);
    }

    @Test
    public void should_fail_building_class_with_static_without_clustering() throws Exception {
        setExec(aptUtils -> {
            final String className = TestEntityWithStaticWithoutClustering.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            final EntityMetaCodeGen builder = new EntityMetaCodeGen(aptUtils);
            final List<FieldParser.FieldMetaSignature> parsingResults = getTypeParsingResults(aptUtils, typeElement, context);
            builder.buildEntityMeta(EntityType.TABLE, typeElement, context, parsingResults, emptyList());
        });
        failTestWithMessage(
                "The class 'info.archinnov.achilles.internals.sample_classes.parser.entity.TestEntityWithStaticWithoutClustering' cannot have static columns without at least 1 clustering column",
                TestEntityWithStaticWithoutClustering.class);
    }

    @Test
    public void should_fail_building_class_with_wrong_computed_column() throws Exception {
        setExec(aptUtils -> {
            final String className = TestEntityWithWrongComputed.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            final EntityMetaCodeGen builder = new EntityMetaCodeGen(aptUtils);
            final List<FieldParser.FieldMetaSignature> parsingResults = getTypeParsingResults(aptUtils, typeElement, context);
            builder.buildEntityMeta(EntityType.TABLE, typeElement, context, parsingResults, emptyList());
        });
        failTestWithMessage(
                "Target field 'one' in @Computed annotation of field 'value' of class 'info.archinnov.achilles.internals.sample_classes.parser.entity.TestEntityWithWrongComputed' does not exist",
                TestEntityWithWrongComputed.class);
    }

    @Test
    public void should_fail_building_class_with_wrong_composite_partition_key_order() throws Exception {
        setExec(aptUtils -> {
            final String className = TestEntityWithWrongCompositePartitionKey.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            final EntityMetaCodeGen builder = new EntityMetaCodeGen(aptUtils);
            final List<FieldParser.FieldMetaSignature> parsingResults = getTypeParsingResults(aptUtils, typeElement, context);
            builder.buildEntityMeta(EntityType.TABLE, typeElement, context, parsingResults, emptyList());
        });
        failTestWithMessage(
                "The @PartitionKey ordering is wrong in class 'info.archinnov.achilles.internals.sample_classes.parser.entity.TestEntityWithWrongCompositePartitionKey'",
                TestEntityWithWrongCompositePartitionKey.class);
    }

    @Test
    public void should_fail_building_class_with_wrong_clustering_column_order() throws Exception {
        setExec(aptUtils -> {
            final String className = TestEntityWithWrongClusteringColumns.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            final EntityMetaCodeGen builder = new EntityMetaCodeGen(aptUtils);
            final List<FieldParser.FieldMetaSignature> parsingResults = getTypeParsingResults(aptUtils, typeElement, context);
            builder.buildEntityMeta(EntityType.TABLE, typeElement, context, parsingResults, emptyList());
        });
        failTestWithMessage(
                "The @ClusteringColumn ordering is wrong in class 'info.archinnov.achilles.internals.sample_classes.parser.entity.TestEntityWithWrongClusteringColumns'",
                TestEntityWithWrongClusteringColumns.class);
    }

    @Test
    public void should_fail_building_child_class_with_shadowed_field() throws Exception {
        setExec(aptUtils -> {
            final String className = TestEntityAsChildShadowingVariable.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            final EntityMetaCodeGen builder = new EntityMetaCodeGen(aptUtils);
            final List<FieldParser.FieldMetaSignature> parsingResults = getTypeParsingResults(aptUtils, typeElement, context);
            builder.buildEntityMeta(EntityType.TABLE, typeElement, context, parsingResults, emptyList());
        });
        failTestWithMessage(
                "The class 'info.archinnov.achilles.internals.sample_classes.parser.entity.TestEntityAsChildShadowingVariable' already contains a field with name 'value'",
                TestEntityAsChildShadowingVariable.class);
    }

    @Test
    public void should_fail_building_view_meta_with_entity_annotation() throws Exception {
        setExec(aptUtils -> {
            final String className = TestViewWithEntityAnnotation.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            final EntityMetaCodeGen builder = new EntityMetaCodeGen(aptUtils);
            final List<FieldParser.FieldMetaSignature> parsingResults = getTypeParsingResults(aptUtils, typeElement, context);
            builder.buildEntityMeta(EntityType.VIEW, typeElement, context, parsingResults, emptyList());

        });
        failTestWithMessage(
                "Cannot have both @Table and @MaterializedView on the class 'info.archinnov.achilles.internals.sample_classes.parser.view.TestViewWithEntityAnnotation'",
                TestViewWithEntityAnnotation.class);
    }

    @Test
    public void should_fail_building_view_meta_with_counter_column() throws Exception {
        setExec(aptUtils -> {
            final String className = TestViewCounter.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            final EntityMetaCodeGen builder = new EntityMetaCodeGen(aptUtils);
            final List<FieldParser.FieldMetaSignature> parsingResults = getTypeParsingResults(aptUtils, typeElement, context);
            builder.buildEntityMeta(EntityType.VIEW, typeElement, context, parsingResults, emptyList());

        });
        failTestWithMessage(
                "The class 'info.archinnov.achilles.internals.sample_classes.parser.view.TestViewCounter' cannot have counter columns because it is a materialized view",
                TestViewCounter.class);
    }

    @Test
    public void should_fail_building_view_meta_with_static_column() throws Exception {
        setExec(aptUtils -> {
            final String className = TestViewStatic.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            final EntityMetaCodeGen builder = new EntityMetaCodeGen(aptUtils);
            final List<FieldParser.FieldMetaSignature> parsingResults = getTypeParsingResults(aptUtils, typeElement, context);
            builder.buildEntityMeta(EntityType.VIEW, typeElement, context, parsingResults, emptyList());

        });
        failTestWithMessage(
                "The class 'info.archinnov.achilles.internals.sample_classes.parser.view.TestViewStatic' cannot have static columns because it is a materialized view",
                TestViewStatic.class);
    }

    @Test
    public void should_fail_building_view_meta_without_view_annotation() throws Exception {
        setExec(aptUtils -> {
            final String className = TestEntityWithSimplePartitionKey.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            final EntityMetaCodeGen builder = new EntityMetaCodeGen(aptUtils);
            final List<FieldParser.FieldMetaSignature> parsingResults = getTypeParsingResults(aptUtils, typeElement, context);
            builder.buildEntityMeta(EntityType.VIEW, typeElement, context, parsingResults, emptyList());

        });
        failTestWithMessage(
                "Missing @MaterializedView annotation on entity class 'info.archinnov.achilles.internals.sample_classes.parser.entity.TestEntityWithSimplePartitionKey'",
                TestEntityWithSimplePartitionKey.class);
    }
}