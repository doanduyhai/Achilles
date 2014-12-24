/*
 * Copyright (C) 2012-2014 DuyHai DOAN
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package info.archinnov.achilles.internal.metadata.parsing;

import static java.lang.String.format;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.UUID;

import info.archinnov.achilles.internal.metadata.holder.ClusteringComponents;
import info.archinnov.achilles.internal.metadata.holder.CompoundPKProperties;
import info.archinnov.achilles.internal.metadata.holder.PartitionComponents;
import info.archinnov.achilles.internal.metadata.parsing.context.EntityParsingContext;
import info.archinnov.achilles.internal.metadata.parsing.context.PropertyParsingContext;
import info.archinnov.achilles.schemabuilder.Create.Options.ClusteringOrder;
import info.archinnov.achilles.schemabuilder.Create.Options.ClusteringOrder.Sorting;
import info.archinnov.achilles.test.parser.entity.*;
import info.archinnov.achilles.type.NamingStrategy;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.*;
import org.mockito.runners.MockitoJUnitRunner;
import info.archinnov.achilles.exception.AchillesBeanMappingException;
import info.archinnov.achilles.test.parser.entity.CompoundPKWithDuplicateOrder;

@RunWith(MockitoJUnitRunner.class)
public class CompoundPKParserTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @InjectMocks
    private CompoundPKParser parser;

    private PropertyParser propertyParser = new PropertyParser();

    @Mock
    private EntityParsingContext context;

    @Mock
    private PropertyParsingContext propertyParsingContext;

    @Captor
    private ArgumentCaptor<Field> fieldArgumentCaptor;

    @Before
    public void setUp() {
        when(context.getNamingStrategy()).thenReturn(NamingStrategy.LOWER_CASE);
        when(context.duplicateForClass(Mockito.<Class<?>>any())).thenReturn(context);
    }

    @Test
    public void should_parse_compound_pk() throws Exception {
        when(context.getCurrentEntityClass()).thenReturn((Class) CorrectCompoundPK.class);


        final Field nameField = CorrectCompoundPK.class.getDeclaredField("name");
        final Field rankField = CorrectCompoundPK.class.getDeclaredField("rank");
        parser = new CompoundPKParser(new PropertyParsingContext(context, nameField));


        CompoundPKProperties props = parser.parseCompoundPK(CorrectCompoundPK.class, propertyParser);

        final PartitionComponents partitionComponents = props.getPartitionComponents();
        assertThat(partitionComponents.getComponentClasses()).containsExactly(String.class);
        assertThat(partitionComponents.getComponentFields()).containsExactly(nameField);
        assertThat(partitionComponents.getComponentNames()).containsExactly("name");
        assertThat(partitionComponents.getCQLComponentNames()).containsExactly("name");

        final ClusteringComponents clusteringComponents = props.getClusteringComponents();
        assertThat(clusteringComponents.getComponentClasses()).containsExactly(int.class);
        assertThat(clusteringComponents.getComponentFields()).containsExactly(rankField);
        assertThat(clusteringComponents.getComponentNames()).containsExactly("rank");
        assertThat(clusteringComponents.getCQLComponentNames()).containsExactly("rank");
        assertThat(clusteringComponents.getClusteringOrders()).containsExactly(new ClusteringOrder("rank", Sorting.ASC));
    }

    @Test
    public void should_parse_compound_pk_with_reversed_key() throws Exception {

        when(context.getCurrentEntityClass()).thenReturn((Class) CorrectCompoundPKReversedKey.class);

        final Field nameField = CorrectCompoundPKReversedKey.class.getDeclaredField("name");
        final Field rankField = CorrectCompoundPKReversedKey.class.getDeclaredField("rank");
        final Field countField = CorrectCompoundPKReversedKey.class.getDeclaredField("count");
        parser = new CompoundPKParser(new PropertyParsingContext(context, nameField));

        CompoundPKProperties props = parser.parseCompoundPK(CorrectCompoundPKReversedKey.class, propertyParser);

        final PartitionComponents partitionComponents = props.getPartitionComponents();
        assertThat(partitionComponents.getComponentClasses()).containsExactly(String.class);
        assertThat(partitionComponents.getComponentFields()).containsExactly(nameField);
        assertThat(partitionComponents.getComponentNames()).containsExactly("name");
        assertThat(partitionComponents.getCQLComponentNames()).containsExactly("name");

        final ClusteringComponents clusteringComponents = props.getClusteringComponents();
        assertThat(clusteringComponents.getComponentClasses()).containsExactly(int.class, int.class);
        assertThat(clusteringComponents.getComponentFields()).containsExactly(rankField, countField);
        assertThat(clusteringComponents.getComponentNames()).containsExactly("rank", "count");
        assertThat(clusteringComponents.getCQLComponentNames()).containsExactly("rank", "count");
        assertThat(clusteringComponents.getClusteringOrders()).containsExactly(new ClusteringOrder("rank", Sorting.DESC), new ClusteringOrder("count", Sorting.DESC));
    }

    @Test
    public void should_exception_when_compound_pk_incorrect_type() throws Exception {

        when(context.getCurrentEntityClass()).thenReturn((Class) CompoundPKIncorrectType.class);

        final Field nameField = CompoundPKIncorrectType.class.getDeclaredField("name");
        parser = new CompoundPKParser(new PropertyParsingContext(context, nameField));

        exception.expect(AchillesBeanMappingException.class);
        exception.expectMessage(format("The column '%s' cannot be a list because it belongs to the partition key","name"));

        parser.parseCompoundPK(CompoundPKIncorrectType.class, propertyParser);
    }

    @Test
    public void should_exception_when_compound_pk_wrong_key_order() throws Exception {

        when(context.getCurrentEntityClass()).thenReturn((Class) CompoundPKWithNegativeOrder.class);

        final Field nameField = CompoundPKIncorrectType.class.getDeclaredField("name");
        parser = new CompoundPKParser(new PropertyParsingContext(context, nameField));

        exception.expect(AchillesBeanMappingException.class);
        exception.expectMessage(format("The partition components ordering is wrong for @CompoundPrimaryKey class '%s'",CompoundPKWithNegativeOrder.class.getCanonicalName()));

        parser.parseCompoundPK(CompoundPKWithNegativeOrder.class, propertyParser);
    }

    @Test
    public void should_exception_when_compound_pk_has_no_annotation() throws Exception {
        when(context.getCurrentEntityClass()).thenReturn((Class) CompoundPKWithNoAnnotation.class);

        final Field nameField = CompoundPKWithNoAnnotation.class.getDeclaredField("name");
        parser = new CompoundPKParser(new PropertyParsingContext(context, nameField));

        exception.expect(AchillesBeanMappingException.class);
        exception.expectMessage(format("Please use @PartitionKey and @ClusteringColumn annotations for the @CompoundPrimaryKey class '%s'",CompoundPKWithNoAnnotation.class.getCanonicalName()));

        parser.parseCompoundPK(CompoundPKWithNoAnnotation.class, propertyParser);
    }

    @Test
    public void should_exception_when_compound_pk_has_duplicate_order() throws Exception {
        when(context.getCurrentEntityClass()).thenReturn((Class) CompoundPKWithDuplicateOrder.class);

        final Field nameField = CompoundPKWithDuplicateOrder.class.getDeclaredField("name");
        parser = new CompoundPKParser(new PropertyParsingContext(context, nameField));

        exception.expect(AchillesBeanMappingException.class);
        exception.expectMessage(format("The partition components ordering is wrong for @CompoundPrimaryKey class '%s",CompoundPKWithDuplicateOrder.class.getCanonicalName()));

        parser.parseCompoundPK(CompoundPKWithDuplicateOrder.class, propertyParser);
    }


    @Test
    public void should_exception_when_compound_pk_has_only_one_component() throws Exception {
        when(context.getCurrentEntityClass()).thenReturn((Class) CompoundPKWithOnlyOneComponent.class);

        final Field userField = CompoundPKWithOnlyOneComponent.class.getDeclaredField("userId");
        parser = new CompoundPKParser(new PropertyParsingContext(context, userField));

        exception.expect(AchillesBeanMappingException.class);
        exception.expectMessage(format("There should be at least 2 fields annotated with @PartitionKey or @ClusteringColumn for the @CompoundPrimaryKey class '%s'",CompoundPKWithOnlyOneComponent.class.getCanonicalName()));
        parser.parseCompoundPK(CompoundPKWithOnlyOneComponent.class, propertyParser);
    }

    @Test
    public void should_parse_compound_pk_with_compound_partition_key() throws Exception {
        when(context.getCurrentEntityClass()).thenReturn((Class) CompoundPKWithCompositePartitionKey.class);

        final Field idField = CompoundPKWithCompositePartitionKey.class.getDeclaredField("id");
        final Field typeField = CompoundPKWithCompositePartitionKey.class.getDeclaredField("type");
        final Field dateField = CompoundPKWithCompositePartitionKey.class.getDeclaredField("date");
        parser = new CompoundPKParser(new PropertyParsingContext(context, idField));

        CompoundPKProperties props = parser.parseCompoundPK(CompoundPKWithCompositePartitionKey.class, propertyParser);

        final PartitionComponents partitionComponents = props.getPartitionComponents();
        assertThat(partitionComponents.getComponentClasses()).containsExactly(Long.class, String.class);
        assertThat(partitionComponents.getComponentFields()).containsExactly(idField, typeField);
        assertThat(partitionComponents.getComponentNames()).containsExactly("id", "type");
        assertThat(partitionComponents.getCQLComponentNames()).containsExactly("id", "type");

        final ClusteringComponents clusteringComponents = props.getClusteringComponents();
        assertThat(clusteringComponents.getComponentClasses()).containsExactly(UUID.class);
        assertThat(clusteringComponents.getComponentFields()).containsExactly(dateField);
        assertThat(clusteringComponents.getComponentNames()).containsExactly("date");
        assertThat(clusteringComponents.getCQLComponentNames()).containsExactly("date");
        assertThat(clusteringComponents.getClusteringOrders()).containsExactly(new ClusteringOrder("date", Sorting.ASC));

    }

    @Test
    public void should_exception_when_compound_pk_has_inconsistent_compound_partition_key() throws Exception {
        when(context.getCurrentEntityClass()).thenReturn((Class) CompoundPKWithInconsistentCompoundPartitionKey.class);

        final Field idField = CompoundPKWithInconsistentCompoundPartitionKey.class.getDeclaredField("id");
        parser = new CompoundPKParser(new PropertyParsingContext(context, idField));


        exception.expect(AchillesBeanMappingException.class);
        exception.expectMessage(format("The partition components ordering is wrong for @CompoundPrimaryKey class '%s'",CompoundPKWithInconsistentCompoundPartitionKey.class.getCanonicalName()));
        parser.parseCompoundPK(CompoundPKWithInconsistentCompoundPartitionKey.class, propertyParser);
    }

    @Test
    public void should_exception_when_compound_pk_has_static_column() throws Exception {
        when(context.getCurrentEntityClass()).thenReturn((Class) CompoundPKWithStaticColumn.class);

        final Field nameField = CompoundPKWithStaticColumn.class.getDeclaredField("name");
        parser = new CompoundPKParser(new PropertyParsingContext(context, nameField));

        exception.expect(AchillesBeanMappingException.class);
        exception.expectMessage(format("The property 'rank' of class '%s' cannot be a static column because it belongs to the primary key", CompoundPKWithStaticColumn.class.getCanonicalName()));

        parser.parseCompoundPK(CompoundPKWithStaticColumn.class, propertyParser);
    }

    @Test
    public void should_parse_compound_pk_as_compound_partition_key() throws Exception {

        when(context.getCurrentEntityClass()).thenReturn((Class) CompoundPKAsCompoundPartitionKey.class);

        final Field idField = CompoundPKAsCompoundPartitionKey.class.getDeclaredField("id");
        final Field typeField = CompoundPKAsCompoundPartitionKey.class.getDeclaredField("type");
        parser = new CompoundPKParser(new PropertyParsingContext(context, idField));

        CompoundPKProperties props = parser.parseCompoundPK(CompoundPKAsCompoundPartitionKey.class, propertyParser);

        final PartitionComponents partitionComponents = props.getPartitionComponents();
        assertThat(partitionComponents.getComponentClasses()).containsExactly(Long.class, String.class);
        assertThat(partitionComponents.getComponentFields()).containsExactly(idField, typeField);
        assertThat(partitionComponents.getComponentNames()).containsExactly("id", "type");
        assertThat(partitionComponents.getCQLComponentNames()).containsExactly("id", "type");

        final ClusteringComponents clusteringComponents = props.getClusteringComponents();
        assertThat(clusteringComponents.getComponentClasses()).isEmpty();
        assertThat(clusteringComponents.getComponentFields()).isEmpty();
        assertThat(clusteringComponents.getComponentNames()).isEmpty();
        assertThat(clusteringComponents.getCQLComponentNames()).isEmpty();
        assertThat(clusteringComponents.getClusteringOrders()).isEmpty();
    }

    @Test
    public void should_parse_compound_pk_with_inheritance() throws Exception {
        //When
        when(context.getCurrentEntityClass()).thenReturn((Class) CompoundPKChild1.class);

        final Field partitionKeyField = CompoundPKParent.class.getDeclaredField("partitionKey");
        final Field clusteringKeyField = CompoundPKChild1.class.getDeclaredField("clustering");
        parser = new CompoundPKParser(new PropertyParsingContext(context, partitionKeyField));

        CompoundPKProperties props = parser.parseCompoundPK(CompoundPKChild1.class, propertyParser);

        //Then
        final PartitionComponents partitionComponents = props.getPartitionComponents();
        assertThat(partitionComponents.getComponentClasses()).containsExactly(String.class);
        assertThat(partitionComponents.getComponentFields()).containsExactly(partitionKeyField);
        assertThat(partitionComponents.getComponentNames()).containsExactly("partitionKey");
        assertThat(partitionComponents.getCQLComponentNames()).containsExactly("partition_key");

        final ClusteringComponents clusteringComponents = props.getClusteringComponents();
        assertThat(clusteringComponents.getComponentClasses()).containsExactly(Long.class);
        assertThat(clusteringComponents.getComponentFields()).containsExactly(clusteringKeyField);
        assertThat(clusteringComponents.getComponentNames()).containsExactly("clustering");
        assertThat(clusteringComponents.getCQLComponentNames()).containsExactly("clustering_key");
        assertThat(clusteringComponents.getClusteringOrders()).containsExactly(new ClusteringOrder("clustering_key", Sorting.ASC));
    }

    @Test
    public void should_parse_compound_pk_with_complicated_inheritance() throws Exception {
        //When
        when(context.getCurrentEntityClass()).thenReturn((Class) CompoundPKChild3.class);

        final Field partitionKey1Field = CompoundPKParent.class.getDeclaredField("partitionKey");
        final Field partitionKey2Field = CompoundPKChild2.class.getDeclaredField("partitionKey2");
        final Field clusteringKeyField = CompoundPKChild3.class.getDeclaredField("clustering");
        parser = new CompoundPKParser(new PropertyParsingContext(context, partitionKey1Field));

        CompoundPKProperties props = parser.parseCompoundPK(CompoundPKChild3.class, propertyParser);

        //Then
        final PartitionComponents partitionComponents = props.getPartitionComponents();
        assertThat(partitionComponents.getComponentClasses()).containsExactly(String.class, Long.class);
        assertThat(partitionComponents.getComponentFields()).containsExactly(partitionKey1Field, partitionKey2Field);
        assertThat(partitionComponents.getComponentNames()).containsExactly("partitionKey", "partitionKey2");
        assertThat(partitionComponents.getCQLComponentNames()).containsExactly("partition_key", "partition_key2");

        final ClusteringComponents clusteringComponents = props.getClusteringComponents();
        assertThat(clusteringComponents.getComponentClasses()).containsExactly(UUID.class);
        assertThat(clusteringComponents.getComponentFields()).containsExactly(clusteringKeyField);
        assertThat(clusteringComponents.getComponentNames()).containsExactly("clustering");
        assertThat(clusteringComponents.getCQLComponentNames()).containsExactly("clustering_key");
        assertThat(clusteringComponents.getClusteringOrders()).containsExactly(new ClusteringOrder("clustering_key", Sorting.ASC));
    }

}
