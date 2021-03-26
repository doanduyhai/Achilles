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

package info.archinnov.achilles.it;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ProtocolVersion;

import info.archinnov.achilles.generated.meta.entity.*;
import info.archinnov.achilles.generated.meta.udt.TestUDT_AchillesMeta;
import info.archinnov.achilles.internals.factory.TupleTypeFactory;
import info.archinnov.achilles.internals.factory.UserTypeFactory;
import info.archinnov.achilles.internals.schema.SchemaContext;
import info.archinnov.achilles.schema.SchemaGenerator;

public class TestSchemaGenerator extends AbstractTestUtil {

    private SchemaContext context = new SchemaContext("my_ks", false, false);

    @Test
    public void should_build_schema_for_entity_with_static_column() throws Exception {
        //Given
        final EntityWithStaticColumn_AchillesMeta meta = new EntityWithStaticColumn_AchillesMeta();

        //When
        final String actual = meta.generateSchema(context);

        //Then
        assertThat(actual.trim()).isEqualTo(readCodeBlockFromFile(
                "schema/should_build_schema_for_entity_with_static_column.cql"));
    }

    @Test
    public void should_build_schema_for_entity_with_counter_column() throws Exception {
        //Given
        final EntityWithCounterColumn_AchillesMeta meta = new EntityWithCounterColumn_AchillesMeta();

        //When
        final String actual = meta.generateSchema(context);

        //Then
        assertThat(actual.trim()).isEqualTo(readCodeBlockFromFile(
                "schema/should_build_schema_for_entity_with_counter_column.cql"));
    }

    @Test
    public void should_build_schema_for_entity_with_static_counter_column() throws Exception {
        //Given
        final EntityWithStaticCounterColumn_AchillesMeta meta = new EntityWithStaticCounterColumn_AchillesMeta();

        //When
        final String actual = meta.generateSchema(context);

        //Then
        assertThat(actual.trim()).isEqualTo(readCodeBlockFromFile(
                "schema/should_build_schema_for_entity_with_static_counter_column.cql"));
    }

    @Test
    public void should_build_schema_for_child_entity() throws Exception {
        //Given
        final EntityAsChild_AchillesMeta meta = new EntityAsChild_AchillesMeta();

        //When
        final String actual = meta.generateSchema(context);

        //Then
        assertThat(actual.trim()).isEqualTo(readCodeBlockFromFile(
                "schema/should_build_schema_for_child_entity.cql"));
    }

    @Test
    public void should_build_schema_for_complex_types() throws Exception {
        //Given
        final EntityWithComplexTypes_AchillesMeta meta = new EntityWithComplexTypes_AchillesMeta();
        final CodecRegistry codecRegistry = new CodecRegistry();
        TupleTypeFactory tupleTypeFactory = new TupleTypeFactory(ProtocolVersion.NEWEST_SUPPORTED, codecRegistry);
        UserTypeFactory userTypeFactory = new UserTypeFactory(ProtocolVersion.NEWEST_SUPPORTED, codecRegistry);

        meta.inject(userTypeFactory, tupleTypeFactory);

        //When
        final String actual = meta.generateSchema(context);

        //Then
        assertThat(actual.trim()).isEqualTo(readCodeBlockFromFile(
                "schema/should_build_schema_for_complex_types.cql"));
    }

    @Test
    public void should_build_schema_for_complex_counter() throws Exception {
        //Given
        final EntityWithComplexCounters_AchillesMeta meta = new EntityWithComplexCounters_AchillesMeta();

        //When
        final String actual = meta.generateSchema(context);

        //Then
        assertThat(actual.trim()).isEqualTo(readCodeBlockFromFile(
                "schema/should_build_schema_for_complex_counter.cql"));
    }

    @Test
    public void should_build_schema_for_complex_indices() throws Exception {
        //Given
        final EntityWithComplexIndices_AchillesMeta meta = new EntityWithComplexIndices_AchillesMeta();

        //When
        final String actual = meta.generateSchema(new SchemaContext("my_ks", false, true));

        //Then
        assertThat(actual.trim()).isEqualTo(readCodeBlockFromFile(
                "schema/should_build_schema_for_complex_indices.cql"));
    }

    @Test
    public void should_build_schema_for_entity_with_static_annotations() throws Exception {
        //Given
        final EntityWithStaticAnnotations_AchillesMeta meta = new EntityWithStaticAnnotations_AchillesMeta();

        //When
        final String actual = meta.generateSchema(context);

        //Then
        assertThat(actual.trim()).isEqualTo(readCodeBlockFromFile(
                "schema/should_build_schema_for_entity_with_static_annotations.cql"));
    }

    @Test
    public void should_build_schema_for_udt() throws Exception {
        //Given
        final TestUDT_AchillesMeta meta = TestUDT_AchillesMeta.INSTANCE;

        //When
        final String actual = meta.generateSchema(new SchemaContext("my_ks", true, false));

        //Then
        assertThat(actual.trim()).isEqualTo(readCodeBlockFromFile(
                "schema/should_build_schema_for_udt.cql"));
    }

    @Test
    public void should_use_schema_generator() throws Exception {
        //Given

        //When
        final String schema = SchemaGenerator.builder()
                .withKeyspace("test")
                .generateCustomTypes(true)
                .generate();

        //Then
        assertThat(schema.trim()).isEqualTo(readCodeBlockFromFile(
                "schema/should_use_schema_generator.cql"));
    }

    @Test
    public void should_use_schema_generator_and_append_to_string() throws Exception {
        //Given
        StringBuilder builder = new StringBuilder();

        //When
        SchemaGenerator.builder().withKeyspace("test").generateTo(builder);

        //Then
        assertThat(builder.toString().trim()).isEqualTo(readCodeBlockFromFile(
                "schema/should_use_schema_generator.cql"));
    }


    @Test
    public void should_build_schema_for_simple_entity() throws Exception {
        //Given
        final SimpleEntity_AchillesMeta meta = new SimpleEntity_AchillesMeta();

        //When
        final String actual = meta.generateSchema(context);

        //Then
        assertThat(actual.trim()).isEqualTo(readCodeBlockFromFile(
                "schema/should_build_schema_for_simple_entity.cql"));
    }

    @Test
    public void should_build_schema_for_entity_with_simple_partition_key() throws Exception {
        //Given
        final EntityWithSimplePartitionKey_AchillesMeta meta = new EntityWithSimplePartitionKey_AchillesMeta();

        //When
        final String actual = meta.generateSchema(context);

        //Then
        assertThat(actual.trim()).isEqualTo(readCodeBlockFromFile(
                "schema/should_build_schema_for_entity_with_simple_partition_key.cql"));
    }

    @Test
    public void should_build_schema_for_entity_with_composite_partition_key() throws Exception {
        //Given
        final EntityWithCompositePartitionKey_AchillesMeta meta = new EntityWithCompositePartitionKey_AchillesMeta();

        //When
        final String actual = meta.generateSchema(context);

        //Then
        assertThat(actual.trim()).isEqualTo(readCodeBlockFromFile(
                "schema/should_build_schema_for_entity_with_composite_partition_key.cql"));
    }

    @Test
    public void should_build_schema_for_entity_with_clustering_columns() throws Exception {
        //Given
        final EntityWithClusteringColumns_AchillesMeta meta = new EntityWithClusteringColumns_AchillesMeta();

        //When
        final String actual = meta.generateSchema(context);

        //Then
        assertThat(actual.trim()).isEqualTo(readCodeBlockFromFile(
                "schema/should_build_schema_for_entity_with_clustering_columns.cql"));
    }

    @Test
    public void should_build_schema_for_view() throws Exception {
        //Given
        final ViewSensorByType_AchillesMeta meta = new ViewSensorByType_AchillesMeta();
        final EntitySensor_AchillesMeta baseTableMeta = new EntitySensor_AchillesMeta();
        meta.setBaseClassProperty(baseTableMeta);

        //When
        final String actual = meta.generateSchema(context);

        //Then
        assertThat(actual.trim()).isEqualTo(readCodeBlockFromFile(
                "schema/should_build_schema_for_view.cql"));
    }
}
