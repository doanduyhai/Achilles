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

import static info.archinnov.achilles.schemabuilder.Create.Options.ClusteringOrder.Sorting.DESC;
import static org.fest.assertions.api.Assertions.assertThat;
import java.lang.reflect.Method;
import java.util.UUID;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.runners.MockitoJUnitRunner;
import info.archinnov.achilles.exception.AchillesBeanMappingException;
import info.archinnov.achilles.internal.metadata.holder.EmbeddedIdProperties;
import info.archinnov.achilles.test.parser.entity.CorrectEmbeddedKey;
import info.archinnov.achilles.test.parser.entity.CorrectEmbeddedReversedKey;
import info.archinnov.achilles.test.parser.entity.EmbeddedKeyAsCompoundPartitionKey;
import info.archinnov.achilles.test.parser.entity.EmbeddedKeyChild1;
import info.archinnov.achilles.test.parser.entity.EmbeddedKeyChild3;
import info.archinnov.achilles.test.parser.entity.EmbeddedKeyIncorrectType;
import info.archinnov.achilles.test.parser.entity.EmbeddedKeyNotInstantiable;
import info.archinnov.achilles.test.parser.entity.EmbeddedKeyWithCompoundPartitionKey;
import info.archinnov.achilles.test.parser.entity.EmbeddedKeyWithDuplicateOrder;
import info.archinnov.achilles.test.parser.entity.EmbeddedKeyWithInconsistentCompoundPartitionKey;
import info.archinnov.achilles.test.parser.entity.EmbeddedKeyWithNegativeOrder;
import info.archinnov.achilles.test.parser.entity.EmbeddedKeyWithNoAnnotation;
import info.archinnov.achilles.test.parser.entity.EmbeddedKeyWithOnlyOneComponent;
import info.archinnov.achilles.test.parser.entity.EmbeddedKeyWithTimeUUID;

@RunWith(MockitoJUnitRunner.class)
public class EmbeddedIdParserTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @InjectMocks
    private EmbeddedIdParser parser;

    @Test
    public void should_parse_embedded_id() throws Exception {
        Method nameGetter = CorrectEmbeddedKey.class.getMethod("getName");
        Method nameSetter = CorrectEmbeddedKey.class.getMethod("setName", String.class);

        Method rankGetter = CorrectEmbeddedKey.class.getMethod("getRank");
        Method rankSetter = CorrectEmbeddedKey.class.getMethod("setRank", int.class);

        EmbeddedIdProperties props = parser.parseEmbeddedId(CorrectEmbeddedKey.class);

        assertThat(props.getComponentGetters()).containsExactly(nameGetter, rankGetter);
        assertThat(props.getComponentSetters()).containsExactly(nameSetter, rankSetter);
        assertThat(props.getComponentClasses()).containsExactly(String.class, int.class);
        assertThat(props.getComponentNames()).containsExactly("name", "rank");
        assertThat(props.getOrderingComponent()).isEqualTo("rank");
        assertThat(props.getClusteringComponentNames()).containsExactly("rank");
        assertThat(props.getClusteringComponentClasses()).containsExactly(int.class);
        assertThat(props.getPartitionComponentNames()).containsExactly("name");
        assertThat(props.getPartitionComponentClasses()).containsExactly(String.class);
    }

    @Test
    public void should_parse_embedded_id_with_reversed_key() throws Exception {
        Method nameGetter = CorrectEmbeddedReversedKey.class.getMethod("getName");
        Method nameSetter = CorrectEmbeddedReversedKey.class.getMethod("setName", String.class);

        Method rankGetter = CorrectEmbeddedReversedKey.class.getMethod("getRank");
        Method rankSetter = CorrectEmbeddedReversedKey.class.getMethod("setRank", int.class);

        Method countGetter = CorrectEmbeddedReversedKey.class.getMethod("getCount");
        Method countSetter = CorrectEmbeddedReversedKey.class.getMethod("setCount", int.class);


        EmbeddedIdProperties props = parser.parseEmbeddedId(CorrectEmbeddedReversedKey.class);

        assertThat(props.getComponentGetters()).containsExactly(nameGetter, rankGetter, countGetter);
        assertThat(props.getComponentSetters()).containsExactly(nameSetter, rankSetter, countSetter);
        assertThat(props.getComponentClasses()).containsExactly(String.class, int.class, int.class);
        assertThat(props.getComponentNames()).containsExactly("name", "rank", "count");
        assertThat(props.getOrderingComponent()).isEqualTo("rank");
        assertThat(props.getClusteringComponentNames()).containsExactly("rank","count");
        assertThat(props.getClusteringComponentClasses()).containsExactly(int.class,int.class);
        assertThat(props.getPartitionComponentNames()).containsExactly("name");
        assertThat(props.getPartitionComponentClasses()).containsExactly(String.class);
        assertThat(props.getCluseringOrders().get(0).getSorting()).isEqualTo(DESC);
        assertThat(props.getCluseringOrders().get(1).getSorting()).isEqualTo(DESC);
    }

    @Test
    public void should_parse_embedded_id_with_time_uuid() throws Exception {
        EmbeddedIdProperties props = parser.parseEmbeddedId(EmbeddedKeyWithTimeUUID.class);

        assertThat(props.getTimeUUIDComponents()).containsExactly("date");
        assertThat(props.getComponentNames()).containsExactly("date", "ranking");
    }

    @Test
    public void should_exception_when_embedded_id_incorrect_type() throws Exception {
        exception.expect(AchillesBeanMappingException.class);
        exception.expectMessage("The class 'java.util.List' is not a valid component type for the @EmbeddedId class '"
                + EmbeddedKeyIncorrectType.class.getCanonicalName() + "'");

        parser.parseEmbeddedId(EmbeddedKeyIncorrectType.class);
    }

    @Test
    public void should_exception_when_embedded_id_wrong_key_order() throws Exception {
        exception.expect(AchillesBeanMappingException.class);
        exception.expectMessage("The component ordering is wrong for @EmbeddedId class '"
                + EmbeddedKeyWithNegativeOrder.class.getCanonicalName() + "'");

        parser.parseEmbeddedId(EmbeddedKeyWithNegativeOrder.class);
    }

    @Test
    public void should_exception_when_embedded_id_has_no_annotation() throws Exception {
        exception.expect(AchillesBeanMappingException.class);
        exception.expectMessage("There should be at least 2 fields annotated with @Order for the @EmbeddedId class '"
                + EmbeddedKeyWithNoAnnotation.class.getCanonicalName() + "'");

        parser.parseEmbeddedId(EmbeddedKeyWithNoAnnotation.class);
    }

    @Test
    public void should_exception_when_embedded_id_has_duplicate_order() throws Exception {
        exception.expect(AchillesBeanMappingException.class);

        exception.expectMessage("The order '1' is duplicated in @EmbeddedId class '"
                + EmbeddedKeyWithDuplicateOrder.class.getCanonicalName() + "'");

        parser.parseEmbeddedId(EmbeddedKeyWithDuplicateOrder.class);
    }

    @Test
    public void should_exception_when_embedded_id_no_pulic_default_constructor() throws Exception {
        exception.expect(AchillesBeanMappingException.class);
        exception.expectMessage("The @EmbeddedId class '" + EmbeddedKeyNotInstantiable.class.getCanonicalName()
                + "' should have a public default constructor");

        parser.parseEmbeddedId(EmbeddedKeyNotInstantiable.class);
    }

    @Test
    public void should_exception_when_embedded_id_has_only_one_component() throws Exception {
        exception.expect(AchillesBeanMappingException.class);
        exception.expectMessage("There should be at least 2 fields annotated with @Order for the @EmbeddedId class '"
                + EmbeddedKeyWithOnlyOneComponent.class.getCanonicalName() + "'");
        parser.parseEmbeddedId(EmbeddedKeyWithOnlyOneComponent.class);
    }

    @Test
    public void should_parse_embedded_id_with_compound_partition_key() throws Exception {
        EmbeddedIdProperties props = parser.parseEmbeddedId(EmbeddedKeyWithCompoundPartitionKey.class);

        assertThat(props.isCompositePartitionKey()).isTrue();
        assertThat(props.getPartitionComponentNames()).containsExactly("id", "type");
        assertThat(props.getPartitionComponentClasses()).containsExactly(Long.class, String.class);

        assertThat(props.getClusteringComponentNames()).containsExactly("date");
        assertThat(props.getClusteringComponentClasses()).containsExactly(UUID.class);

        assertThat(props.getComponentClasses()).containsExactly(Long.class, String.class, UUID.class);
        assertThat(props.getComponentNames()).containsExactly("id", "type", "date");
        assertThat(props.getComponentGetters()).hasSize(3);
        assertThat(props.getComponentSetters()).hasSize(3);
    }

    @Test
    public void should_exception_when_embedded_id_has_inconsistent_compound_partition_key() throws Exception {
        exception.expect(AchillesBeanMappingException.class);
        exception.expectMessage("The composite partition key ordering is wrong for @EmbeddedId class '"
                + EmbeddedKeyWithInconsistentCompoundPartitionKey.class.getCanonicalName() + "'");
        parser.parseEmbeddedId(EmbeddedKeyWithInconsistentCompoundPartitionKey.class);
    }

    @Test
    public void should_parse_embedded_id_as_compound_partition_key() throws Exception {

        EmbeddedIdProperties props = parser.parseEmbeddedId(EmbeddedKeyAsCompoundPartitionKey.class);

        assertThat(props.isCompositePartitionKey()).isTrue();
        assertThat(props.getPartitionComponentNames()).containsExactly("id", "type");
        assertThat(props.getPartitionComponentClasses()).containsExactly(Long.class, String.class);

        assertThat(props.getClusteringComponentNames()).isEmpty();
        assertThat(props.getClusteringComponentClasses()).isEmpty();

        assertThat(props.getComponentClasses()).containsExactly(Long.class, String.class);
        assertThat(props.getComponentNames()).containsExactly("id", "type");
        assertThat(props.getComponentGetters()).hasSize(2);
        assertThat(props.getComponentSetters()).hasSize(2);
    }

    @Test
    public void should_parse_embedded_key_with_inheritance() throws Exception {
        //When
        EmbeddedIdProperties props = parser.parseEmbeddedId(EmbeddedKeyChild1.class);

        //Then
        assertThat(props.isCompositePartitionKey()).isFalse();
        assertThat(props.getPartitionComponentNames()).containsExactly("partition_key");
        assertThat(props.getPartitionComponentClasses()).containsExactly(String.class);

        assertThat(props.getClusteringComponentNames()).containsExactly("clustering");
        assertThat(props.getClusteringComponentClasses()).containsExactly(Long.class);

        assertThat(props.getComponentClasses()).containsExactly(String.class, Long.class);
        assertThat(props.getComponentNames()).containsExactly("partition_key", "clustering");
        assertThat(props.getComponentGetters()).hasSize(2);
        assertThat(props.getComponentSetters()).hasSize(2);
    }

    @Test
    public void should_parse_embedded_key_with_complicated_inheritance() throws Exception {
        //When
        EmbeddedIdProperties props = parser.parseEmbeddedId(EmbeddedKeyChild3.class);

        //Then
        assertThat(props.isCompositePartitionKey()).isTrue();
        assertThat(props.getPartitionComponentNames()).containsExactly("partition_key", "partition_key2");
        assertThat(props.getPartitionComponentClasses()).containsExactly(String.class, Long.class);

        assertThat(props.getClusteringComponentNames()).containsExactly("clustering");
        assertThat(props.getClusteringComponentClasses()).containsExactly(UUID.class);

        assertThat(props.getComponentClasses()).containsExactly(String.class, Long.class, UUID.class);
        assertThat(props.getComponentNames()).containsExactly("partition_key", "partition_key2", "clustering");
        assertThat(props.getComponentGetters()).hasSize(3);
        assertThat(props.getComponentSetters()).hasSize(3);
    }

}
