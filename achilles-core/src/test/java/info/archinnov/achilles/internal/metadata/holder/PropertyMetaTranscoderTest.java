package info.archinnov.achilles.internal.metadata.holder;

import static info.archinnov.achilles.internal.metadata.holder.EmbeddedIdPropertiesBuilder.buildEmbeddedIdProperties;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.COMPOUND_PRIMARY_KEY;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.LIST;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.MAP;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.SET;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.SIMPLE;
import static java.util.Arrays.asList;
import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import info.archinnov.achilles.schemabuilder.Create.Options.ClusteringOrder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

@RunWith(MockitoJUnitRunner.class)
public class PropertyMetaTranscoderTest {

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private PropertyMeta meta;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private CompoundPKProperties compoundPKProperties;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private EntityMeta entityMeta;

    private PropertyMetaTranscoder view;

    @Before
    public void setUp() {
        view = new PropertyMetaTranscoder(meta);
        when(meta.getCompoundPKProperties()).thenReturn(compoundPKProperties);
    }

    @Test
    public void should_decode_from_pk_components() throws Exception {
        //Given
        Long id = 10L;
        Date date = new Date();

        CompleteBean pk = new CompleteBean();

        when(meta.type()).thenReturn(COMPOUND_PRIMARY_KEY);
        when(meta.forValues().instantiate()).thenReturn(pk);

        PropertyMeta idMeta = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);
        PropertyMeta dateMeta = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);

        final PartitionComponents partitionComponents = new PartitionComponents(asList(idMeta));
        final ClusteringComponents clusteringComponents = new ClusteringComponents(asList(dateMeta), Arrays.<ClusteringOrder>asList());
        when(meta.getCompoundPKProperties()).thenReturn(buildEmbeddedIdProperties(partitionComponents, clusteringComponents, "entity"));

        when(idMeta.forTranscoding().decodeFromCassandra(id)).thenReturn(id);
        when(dateMeta.forTranscoding().decodeFromCassandra(date)).thenReturn(date);

        //When
        final Object actual = view.decodeFromComponents(asList(id, date));

        //Then
        assertThat(actual).isSameAs(pk);
        verify(idMeta.forValues()).setValueToField(pk, id);
        verify(dateMeta.forValues()).setValueToField(pk, date);
    }

    @Test
    public void should_decode_simple_from_Cassandra() throws Exception {
        //Given
        when(meta.type()).thenReturn(SIMPLE);
        when(meta.getSimpleCodec().decode(10L)).thenReturn(10L);

        //When

        //Then
        assertThat(view.decodeFromCassandra(10L)).isEqualTo(10L);
    }

    @Test
    public void should_decode_list_from_Cassandra() throws Exception {
        //Given
        when(meta.type()).thenReturn(LIST);
        final List<Long> fromCassandra = asList(10L);
        when(meta.getListCodec().decode(fromCassandra)).thenReturn(fromCassandra);

        //When

        //Then
        assertThat(view.decodeFromCassandra(fromCassandra)).isSameAs(fromCassandra);
    }

    @Test
    public void should_decode_set_from_Cassandra() throws Exception {
        //Given
        when(meta.type()).thenReturn(SET);
        final Set<Long> fromCassandra = Sets.newHashSet(10L);
        when(meta.getSetCodec().decode(fromCassandra)).thenReturn(fromCassandra);

        //When

        //Then
        assertThat(view.decodeFromCassandra(fromCassandra)).isSameAs(fromCassandra);
    }

    @Test
    public void should_decode_map_from_Cassandra() throws Exception {
        //Given
        when(meta.type()).thenReturn(MAP);
        final Map<Integer,String> fromCassandra = ImmutableMap.of(1,"a");
        when(meta.getMapCodec().decode(fromCassandra)).thenReturn(fromCassandra);

        //When

        //Then
        assertThat(view.decodeFromCassandra(fromCassandra)).isSameAs(fromCassandra);
    }

    @Test
    public void should_encode_simple_from_Cassandra() throws Exception {
        //Given
        when(meta.type()).thenReturn(SIMPLE);
        when(meta.getSimpleCodec().encode(10L)).thenReturn(10L);

        //When

        //Then
        assertThat(view.encodeToCassandra(10L)).isEqualTo(10L);
    }

    @Test
    public void should_encode_list_from_Cassandra() throws Exception {
        //Given
        when(meta.type()).thenReturn(LIST);
        final List<Long> fromJava = asList(10L);
        when(meta.getListCodec().encode(fromJava)).thenReturn(fromJava);

        //When

        //Then
        assertThat(view.encodeToCassandra(fromJava)).isSameAs(fromJava);
    }

    @Test
    public void should_encode_set_from_Cassandra() throws Exception {
        //Given
        when(meta.type()).thenReturn(SET);
        final Set<Long> fromJava = Sets.newHashSet(10L);
        when(meta.getSetCodec().encode(fromJava)).thenReturn(fromJava);

        //When

        //Then
        assertThat(view.encodeToCassandra(fromJava)).isSameAs(fromJava);
    }

    @Test
    public void should_encode_map_from_Cassandra() throws Exception {
        //Given
        when(meta.type()).thenReturn(MAP);
        final Map<Integer,String> fromJava = ImmutableMap.of(1,"a");
        when(meta.getMapCodec().encode(fromJava)).thenReturn(fromJava);

        //When

        //Then
        assertThat(view.encodeToCassandra(fromJava)).isSameAs(fromJava);
    }

    @Test
    public void should_get_value_and_encode_for_Cassandra() throws Exception {
        //Given
        Object entity = new Object();
        when(meta.forValues().getValueFromField(entity)).thenReturn(10L);
        when(meta.type()).thenReturn(SIMPLE);
        when(meta.getSimpleCodec().encode(10L)).thenReturn(10L);

        //When

        //Then
        assertThat(view.getAndEncodeValueForCassandra(entity)).isEqualTo(10L);
    }

    @Test
    public void should_encode_pk_to_components() throws Exception {
        Long id = 10L;
        Date date = new Date();

        CompleteBean pk = new CompleteBean();

        when(meta.type()).thenReturn(COMPOUND_PRIMARY_KEY);

        PropertyMeta idMeta = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);
        PropertyMeta dateMeta = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);

        final PartitionComponents partitionComponents = new PartitionComponents(asList(idMeta));
        final ClusteringComponents clusteringComponents = new ClusteringComponents(asList(dateMeta), Arrays.<ClusteringOrder>asList());
        when(meta.getCompoundPKProperties()).thenReturn(buildEmbeddedIdProperties(partitionComponents, clusteringComponents, "entity"));

        when(idMeta.forTranscoding().getAndEncodeValueForCassandra(pk)).thenReturn(id);
        when(dateMeta.forTranscoding().getAndEncodeValueForCassandra(pk)).thenReturn(date);

        //When
        final List<Object> encoded = view.encodeToComponents(pk, false);

        //Then
        assertThat(encoded).containsExactly(id, date);
    }

    @Test
    public void should_encode_only_partition_component_from_pk() throws Exception {
        Long id = 10L;

        CompleteBean pk = new CompleteBean();

        when(meta.type()).thenReturn(COMPOUND_PRIMARY_KEY);

        PropertyMeta idMeta = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);

        when(meta.getCompoundPKProperties().getPartitionComponents()).thenReturn(new PartitionComponents(asList(idMeta)));

        when(idMeta.forTranscoding().getAndEncodeValueForCassandra(pk)).thenReturn(id);

        //When
        final List<Object> encoded = view.encodeToComponents(pk, true);

        //Then
        assertThat(encoded).containsExactly(id);
    }

    @Test
    public void should_encode_null_pk() throws Exception {
        //Given
        when(meta.type()).thenReturn(COMPOUND_PRIMARY_KEY);

        //When
        final List<Object> encoded = view.encodeToComponents(null, true);

        //Then
        assertThat(encoded).isEmpty();
    }

    @Test
    public void should_force_encode_to_JSON() throws Exception {
        //Given
        view = new PropertyMetaTranscoder(new PropertyMeta());
        CompleteBean entity = new CompleteBean();
        entity.setId(10L);

        //When
        final String json = view.forceEncodeToJSONForCounter(entity);

        //Then
        assertThat(json).isEqualTo("{\"id\":10}");
    }

    @Test
    public void should_not_encode_string_to_JSON() throws Exception {
        //Given
        view = new PropertyMetaTranscoder(new PropertyMeta());
        //When
        final String json = view.forceEncodeToJSONForCounter("test");

        //Then
        assertThat(json).isEqualTo("test");
    }

    @Test
    public void should_encode_partition_components() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0,Long.MAX_VALUE);
        final int bucket = 10;

        when(meta.type()).thenReturn(COMPOUND_PRIMARY_KEY);
        PropertyMeta idMeta = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);
        PropertyMeta bucketMeta = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);
        PropertyMeta anotherMeta = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);
        when(meta.getCompoundPKProperties().getPartitionComponents()).thenReturn(new PartitionComponents(asList(idMeta, bucketMeta, anotherMeta)));
        when(idMeta.forTranscoding().encodeToCassandra(id)).thenReturn(id);
        when(bucketMeta.forTranscoding().encodeToCassandra(bucket)).thenReturn(bucket);

        //When
        final List<Object> encoded = view.encodePartitionComponents(Arrays.<Object>asList(id, bucket));

        //Then
        assertThat(encoded).containsExactly(id, bucket);
    }

    @Test
    public void should_encode_partition_components_IN() throws Exception {
        //Given
        final int bucket1 = 10, bucket2 = 11;

        when(meta.type()).thenReturn(COMPOUND_PRIMARY_KEY);
        PropertyMeta idMeta = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);
        PropertyMeta bucketMeta = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);
        when(meta.getCompoundPKProperties().getPartitionComponents()).thenReturn(new PartitionComponents(asList(idMeta, bucketMeta)));
        when(bucketMeta.forTranscoding().encodeToCassandra(bucket1)).thenReturn(bucket1);
        when(bucketMeta.forTranscoding().encodeToCassandra(bucket2)).thenReturn(bucket2);

        //When
        final List<Object> encoded = view.encodePartitionComponentsIN(Arrays.<Object>asList(bucket1, bucket2));

        //Then
        assertThat(encoded).containsExactly(bucket1, bucket2);
    }


    @Test
    public void should_encode_clustering_components() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0,Long.MAX_VALUE);
        final int bucket = 10;

        when(meta.type()).thenReturn(COMPOUND_PRIMARY_KEY);
        PropertyMeta idMeta = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);
        PropertyMeta bucketMeta = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);
        when(meta.getCompoundPKProperties().getClusteringComponents()).thenReturn(new ClusteringComponents(asList(idMeta, bucketMeta), Arrays.<ClusteringOrder>asList()));
        when(idMeta.forTranscoding().encodeToCassandra(id)).thenReturn(id);
        when(bucketMeta.forTranscoding().encodeToCassandra(bucket)).thenReturn(bucket);

        //When
        final List<Object> encoded = view.encodeClusteringKeys(Arrays.<Object>asList(id, bucket));

        //Then
        assertThat(encoded).containsExactly(id, bucket);
    }

    @Test
    public void should_encode_clustering_components_IN() throws Exception {
        //Given
        final int bucket1 = 10, bucket2 = 11;

        when(meta.type()).thenReturn(COMPOUND_PRIMARY_KEY);
        PropertyMeta idMeta = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);
        PropertyMeta bucketMeta = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);
        when(meta.getCompoundPKProperties().getClusteringComponents()).thenReturn(new ClusteringComponents(asList(idMeta, bucketMeta), Arrays.<ClusteringOrder>asList()));
        when(bucketMeta.forTranscoding().encodeToCassandra(bucket1)).thenReturn(bucket1);
        when(bucketMeta.forTranscoding().encodeToCassandra(bucket2)).thenReturn(bucket2);

        //When
        final List<Object> encoded = view.encodeClusteringKeysIN(Arrays.<Object>asList(bucket1, bucket2));

        //Then
        assertThat(encoded).containsExactly(bucket1, bucket2);
    }

}