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
import java.util.List;
import java.util.UUID;

import info.archinnov.achilles.internal.metadata.holder.ClusteringComponents;
import info.archinnov.achilles.internal.metadata.holder.PartitionComponents;
import info.archinnov.achilles.internal.metadata.parsing.context.EntityParsingContext;
import info.archinnov.achilles.internal.metadata.parsing.context.PropertyParsingContext;
import info.archinnov.achilles.schemabuilder.Create.Options.ClusteringOrder;
import info.archinnov.achilles.schemabuilder.Create.Options.ClusteringOrder.Sorting;
import info.archinnov.achilles.test.parser.entity.EmbeddedKeyChild2;
import info.archinnov.achilles.test.parser.entity.EmbeddedKeyParent;
import info.archinnov.achilles.type.NamingStrategy;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.*;
import org.mockito.runners.MockitoJUnitRunner;
import info.archinnov.achilles.exception.AchillesBeanMappingException;
import info.archinnov.achilles.internal.metadata.holder.EmbeddedIdProperties;
import info.archinnov.achilles.test.parser.entity.CorrectEmbeddedKey;
import info.archinnov.achilles.test.parser.entity.CorrectEmbeddedReversedKey;
import info.archinnov.achilles.test.parser.entity.EmbeddedKeyAsCompoundPartitionKey;
import info.archinnov.achilles.test.parser.entity.EmbeddedKeyChild1;
import info.archinnov.achilles.test.parser.entity.EmbeddedKeyChild3;
import info.archinnov.achilles.test.parser.entity.EmbeddedKeyIncorrectType;
import info.archinnov.achilles.test.parser.entity.EmbeddedKeyWithCompoundPartitionKey;
import info.archinnov.achilles.test.parser.entity.EmbeddedKeyWithDuplicateOrder;
import info.archinnov.achilles.test.parser.entity.EmbeddedKeyWithInconsistentCompoundPartitionKey;
import info.archinnov.achilles.test.parser.entity.EmbeddedKeyWithNegativeOrder;
import info.archinnov.achilles.test.parser.entity.EmbeddedKeyWithNoAnnotation;
import info.archinnov.achilles.test.parser.entity.EmbeddedKeyWithOnlyOneComponent;
import info.archinnov.achilles.test.parser.entity.EmbeddedKeyWithStaticColumn;

@RunWith(MockitoJUnitRunner.class)
public class EmbeddedIdParserTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @InjectMocks
    private EmbeddedIdParser parser;

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
    public void should_parse_embedded_id() throws Exception {
        when(context.getCurrentEntityClass()).thenReturn((Class) CorrectEmbeddedKey.class);


        final Field nameField = CorrectEmbeddedKey.class.getDeclaredField("name");
        final Field rankField = CorrectEmbeddedKey.class.getDeclaredField("rank");
        parser = new EmbeddedIdParser(new PropertyParsingContext(context, nameField));


        EmbeddedIdProperties props = parser.parseEmbeddedId(CorrectEmbeddedKey.class, propertyParser);

        final PartitionComponents partitionComponents = props.getPartitionComponents();
        assertThat(partitionComponents.getComponentClasses()).containsExactly(String.class);
        assertThat(partitionComponents.getComponentFields()).containsExactly(nameField);
        assertThat(partitionComponents.getComponentNames()).containsExactly("name");
        assertThat(partitionComponents.getCQL3ComponentNames()).containsExactly("name");

        final ClusteringComponents clusteringComponents = props.getClusteringComponents();
        assertThat(clusteringComponents.getComponentClasses()).containsExactly(int.class);
        assertThat(clusteringComponents.getComponentFields()).containsExactly(rankField);
        assertThat(clusteringComponents.getComponentNames()).containsExactly("rank");
        assertThat(clusteringComponents.getCQL3ComponentNames()).containsExactly("rank");
        assertThat(clusteringComponents.getClusteringOrders()).containsExactly(new ClusteringOrder("rank", Sorting.ASC));
    }

    @Test
    public void should_parse_embedded_id_with_reversed_key() throws Exception {

        when(context.getCurrentEntityClass()).thenReturn((Class) CorrectEmbeddedReversedKey.class);

        final Field nameField = CorrectEmbeddedReversedKey.class.getDeclaredField("name");
        final Field rankField = CorrectEmbeddedReversedKey.class.getDeclaredField("rank");
        final Field countField = CorrectEmbeddedReversedKey.class.getDeclaredField("count");
        parser = new EmbeddedIdParser(new PropertyParsingContext(context, nameField));

        EmbeddedIdProperties props = parser.parseEmbeddedId(CorrectEmbeddedReversedKey.class, propertyParser);

        final PartitionComponents partitionComponents = props.getPartitionComponents();
        assertThat(partitionComponents.getComponentClasses()).containsExactly(String.class);
        assertThat(partitionComponents.getComponentFields()).containsExactly(nameField);
        assertThat(partitionComponents.getComponentNames()).containsExactly("name");
        assertThat(partitionComponents.getCQL3ComponentNames()).containsExactly("name");

        final ClusteringComponents clusteringComponents = props.getClusteringComponents();
        assertThat(clusteringComponents.getComponentClasses()).containsExactly(int.class, int.class);
        assertThat(clusteringComponents.getComponentFields()).containsExactly(rankField, countField);
        assertThat(clusteringComponents.getComponentNames()).containsExactly("rank", "count");
        assertThat(clusteringComponents.getCQL3ComponentNames()).containsExactly("rank", "count");
        assertThat(clusteringComponents.getClusteringOrders()).containsExactly(new ClusteringOrder("rank", Sorting.DESC), new ClusteringOrder("count", Sorting.DESC));
    }

    @Test
    public void should_exception_when_embedded_id_incorrect_type() throws Exception {

        when(context.getCurrentEntityClass()).thenReturn((Class) EmbeddedKeyIncorrectType.class);

        final Field nameField = EmbeddedKeyIncorrectType.class.getDeclaredField("name");
        parser = new EmbeddedIdParser(new PropertyParsingContext(context, nameField));

        exception.expect(AchillesBeanMappingException.class);
        exception.expectMessage(format("The column '%s' cannot be a list because it belongs to the partition key","name"));

        parser.parseEmbeddedId(EmbeddedKeyIncorrectType.class, propertyParser);
    }

    @Test
    public void should_exception_when_embedded_id_wrong_key_order() throws Exception {

        when(context.getCurrentEntityClass()).thenReturn((Class) EmbeddedKeyWithNegativeOrder.class);

        final Field nameField = EmbeddedKeyIncorrectType.class.getDeclaredField("name");
        parser = new EmbeddedIdParser(new PropertyParsingContext(context, nameField));

        exception.expect(AchillesBeanMappingException.class);
        exception.expectMessage(format("The component ordering is wrong for @EmbeddedId class '%s'",EmbeddedKeyWithNegativeOrder.class.getCanonicalName()));

        parser.parseEmbeddedId(EmbeddedKeyWithNegativeOrder.class, propertyParser);
    }

    @Test
    public void should_exception_when_embedded_id_has_no_annotation() throws Exception {
        when(context.getCurrentEntityClass()).thenReturn((Class) EmbeddedKeyWithNoAnnotation.class);

        final Field nameField = EmbeddedKeyWithNoAnnotation.class.getDeclaredField("name");
        parser = new EmbeddedIdParser(new PropertyParsingContext(context, nameField));

        exception.expect(AchillesBeanMappingException.class);
        exception.expectMessage(format("There should be at least 2 fields annotated with @Order for the @EmbeddedId class '%s'",EmbeddedKeyWithNoAnnotation.class.getCanonicalName()));

        parser.parseEmbeddedId(EmbeddedKeyWithNoAnnotation.class, propertyParser);
    }

    @Test
    public void should_exception_when_embedded_id_has_duplicate_order() throws Exception {
        when(context.getCurrentEntityClass()).thenReturn((Class) EmbeddedKeyWithDuplicateOrder.class);

        final Field nameField = EmbeddedKeyWithDuplicateOrder.class.getDeclaredField("name");
        parser = new EmbeddedIdParser(new PropertyParsingContext(context, nameField));

        exception.expect(AchillesBeanMappingException.class);
        exception.expectMessage(format("The order '1' is duplicated in @EmbeddedId class '%s",EmbeddedKeyWithDuplicateOrder.class.getCanonicalName()));

        parser.parseEmbeddedId(EmbeddedKeyWithDuplicateOrder.class, propertyParser);
    }


    @Test
    public void should_exception_when_embedded_id_has_only_one_component() throws Exception {
        when(context.getCurrentEntityClass()).thenReturn((Class) EmbeddedKeyWithOnlyOneComponent.class);

        final Field userField = EmbeddedKeyWithOnlyOneComponent.class.getDeclaredField("userId");
        parser = new EmbeddedIdParser(new PropertyParsingContext(context, userField));

        exception.expect(AchillesBeanMappingException.class);
        exception.expectMessage(format("There should be at least 2 fields annotated with @Order for the @EmbeddedId class '%s'",EmbeddedKeyWithOnlyOneComponent.class.getCanonicalName()));
        parser.parseEmbeddedId(EmbeddedKeyWithOnlyOneComponent.class, propertyParser);
    }

    @Test
    public void should_parse_embedded_id_with_compound_partition_key() throws Exception {
        when(context.getCurrentEntityClass()).thenReturn((Class) EmbeddedKeyWithCompoundPartitionKey.class);

        final Field idField = EmbeddedKeyWithCompoundPartitionKey.class.getDeclaredField("id");
        final Field typeField = EmbeddedKeyWithCompoundPartitionKey.class.getDeclaredField("type");
        final Field dateField = EmbeddedKeyWithCompoundPartitionKey.class.getDeclaredField("date");
        parser = new EmbeddedIdParser(new PropertyParsingContext(context, idField));

        EmbeddedIdProperties props = parser.parseEmbeddedId(EmbeddedKeyWithCompoundPartitionKey.class, propertyParser);

        final PartitionComponents partitionComponents = props.getPartitionComponents();
        assertThat(partitionComponents.getComponentClasses()).containsExactly(Long.class, String.class);
        assertThat(partitionComponents.getComponentFields()).containsExactly(idField, typeField);
        assertThat(partitionComponents.getComponentNames()).containsExactly("id", "type");
        assertThat(partitionComponents.getCQL3ComponentNames()).containsExactly("id", "type");

        final ClusteringComponents clusteringComponents = props.getClusteringComponents();
        assertThat(clusteringComponents.getComponentClasses()).containsExactly(UUID.class);
        assertThat(clusteringComponents.getComponentFields()).containsExactly(dateField);
        assertThat(clusteringComponents.getComponentNames()).containsExactly("date");
        assertThat(clusteringComponents.getCQL3ComponentNames()).containsExactly("date");
        assertThat(clusteringComponents.getClusteringOrders()).containsExactly(new ClusteringOrder("date", Sorting.ASC));

    }

    @Test
    public void should_exception_when_embedded_id_has_inconsistent_compound_partition_key() throws Exception {
        when(context.getCurrentEntityClass()).thenReturn((Class) EmbeddedKeyWithInconsistentCompoundPartitionKey.class);

        final Field idField = EmbeddedKeyWithInconsistentCompoundPartitionKey.class.getDeclaredField("id");
        parser = new EmbeddedIdParser(new PropertyParsingContext(context, idField));


        exception.expect(AchillesBeanMappingException.class);
        exception.expectMessage(format("The composite partition key ordering is wrong for @EmbeddedId class '%s'",EmbeddedKeyWithInconsistentCompoundPartitionKey.class.getCanonicalName()));
        parser.parseEmbeddedId(EmbeddedKeyWithInconsistentCompoundPartitionKey.class, propertyParser);
    }

    @Test
    public void should_exception_when_embedded_id_has_static_column() throws Exception {
        when(context.getCurrentEntityClass()).thenReturn((Class) EmbeddedKeyWithStaticColumn.class);

        final Field nameField = EmbeddedKeyWithStaticColumn.class.getDeclaredField("name");
        parser = new EmbeddedIdParser(new PropertyParsingContext(context, nameField));

        exception.expect(AchillesBeanMappingException.class);
        exception.expectMessage(format("The property 'rank' of class '%s' cannot be a static column because it belongs to the primary key", EmbeddedKeyWithStaticColumn.class.getCanonicalName()));

        parser.parseEmbeddedId(EmbeddedKeyWithStaticColumn.class, propertyParser);
    }

    @Test
    public void should_parse_embedded_id_as_compound_partition_key() throws Exception {

        when(context.getCurrentEntityClass()).thenReturn((Class) EmbeddedKeyAsCompoundPartitionKey.class);

        final Field idField = EmbeddedKeyAsCompoundPartitionKey.class.getDeclaredField("id");
        final Field typeField = EmbeddedKeyAsCompoundPartitionKey.class.getDeclaredField("type");
        parser = new EmbeddedIdParser(new PropertyParsingContext(context, idField));

        EmbeddedIdProperties props = parser.parseEmbeddedId(EmbeddedKeyAsCompoundPartitionKey.class, propertyParser);

        final PartitionComponents partitionComponents = props.getPartitionComponents();
        assertThat(partitionComponents.getComponentClasses()).containsExactly(Long.class, String.class);
        assertThat(partitionComponents.getComponentFields()).containsExactly(idField, typeField);
        assertThat(partitionComponents.getComponentNames()).containsExactly("id", "type");
        assertThat(partitionComponents.getCQL3ComponentNames()).containsExactly("id", "type");

        final ClusteringComponents clusteringComponents = props.getClusteringComponents();
        assertThat(clusteringComponents.getComponentClasses()).isEmpty();
        assertThat(clusteringComponents.getComponentFields()).isEmpty();
        assertThat(clusteringComponents.getComponentNames()).isEmpty();
        assertThat(clusteringComponents.getCQL3ComponentNames()).isEmpty();
        assertThat(clusteringComponents.getClusteringOrders()).isEmpty();
    }

    @Test
    public void should_parse_embedded_key_with_inheritance() throws Exception {
        //When
        when(context.getCurrentEntityClass()).thenReturn((Class) EmbeddedKeyChild1.class);

        final Field partitionKeyField = EmbeddedKeyParent.class.getDeclaredField("partitionKey");
        final Field clusteringKeyField = EmbeddedKeyChild1.class.getDeclaredField("clustering");
        parser = new EmbeddedIdParser(new PropertyParsingContext(context, partitionKeyField));

        EmbeddedIdProperties props = parser.parseEmbeddedId(EmbeddedKeyChild1.class, propertyParser);

        //Then
        final PartitionComponents partitionComponents = props.getPartitionComponents();
        assertThat(partitionComponents.getComponentClasses()).containsExactly(String.class);
        assertThat(partitionComponents.getComponentFields()).containsExactly(partitionKeyField);
        assertThat(partitionComponents.getComponentNames()).containsExactly("partitionKey");
        assertThat(partitionComponents.getCQL3ComponentNames()).containsExactly("partition_key");

        final ClusteringComponents clusteringComponents = props.getClusteringComponents();
        assertThat(clusteringComponents.getComponentClasses()).containsExactly(Long.class);
        assertThat(clusteringComponents.getComponentFields()).containsExactly(clusteringKeyField);
        assertThat(clusteringComponents.getComponentNames()).containsExactly("clustering");
        assertThat(clusteringComponents.getCQL3ComponentNames()).containsExactly("clustering_key");
        assertThat(clusteringComponents.getClusteringOrders()).containsExactly(new ClusteringOrder("clustering_key", Sorting.ASC));
    }

    @Test
    public void should_parse_embedded_key_with_complicated_inheritance() throws Exception {
        //When
        when(context.getCurrentEntityClass()).thenReturn((Class) EmbeddedKeyChild3.class);

        final Field partitionKey1Field = EmbeddedKeyParent.class.getDeclaredField("partitionKey");
        final Field partitionKey2Field = EmbeddedKeyChild2.class.getDeclaredField("partitionKey2");
        final Field clusteringKeyField = EmbeddedKeyChild3.class.getDeclaredField("clustering");
        parser = new EmbeddedIdParser(new PropertyParsingContext(context, partitionKey1Field));

        EmbeddedIdProperties props = parser.parseEmbeddedId(EmbeddedKeyChild3.class, propertyParser);

        //Then
        final PartitionComponents partitionComponents = props.getPartitionComponents();
        assertThat(partitionComponents.getComponentClasses()).containsExactly(String.class, Long.class);
        assertThat(partitionComponents.getComponentFields()).containsExactly(partitionKey1Field, partitionKey2Field);
        assertThat(partitionComponents.getComponentNames()).containsExactly("partitionKey", "partitionKey2");
        assertThat(partitionComponents.getCQL3ComponentNames()).containsExactly("partition_key", "partition_key2");

        final ClusteringComponents clusteringComponents = props.getClusteringComponents();
        assertThat(clusteringComponents.getComponentClasses()).containsExactly(UUID.class);
        assertThat(clusteringComponents.getComponentFields()).containsExactly(clusteringKeyField);
        assertThat(clusteringComponents.getComponentNames()).containsExactly("clustering");
        assertThat(clusteringComponents.getCQL3ComponentNames()).containsExactly("clustering_key");
        assertThat(clusteringComponents.getClusteringOrders()).containsExactly(new ClusteringOrder("clustering_key", Sorting.ASC));
    }

}
