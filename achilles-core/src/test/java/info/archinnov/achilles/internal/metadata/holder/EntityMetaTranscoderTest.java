package info.archinnov.achilles.internal.metadata.holder;


import static java.util.Arrays.asList;
import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Mockito.*;

import info.archinnov.achilles.type.IndexCondition;
import info.archinnov.achilles.options.Options.LWTCondition;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;

@RunWith(MockitoJUnitRunner.class)
public class EntityMetaTranscoderTest {

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private EntityMeta meta;

    private EntityMetaTranscoder view;

    @Before
    public void setUp() {
        view = new EntityMetaTranscoder(meta);
    }

    @Test
    public void should_encode_LWT_condition_value() throws Exception {
        //Given
        PropertyMeta nameMeta = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);
        LWTCondition LWTCondition = spy(new LWTCondition("name", "DuyHai"));

        when(meta.getAllMetasExceptCounters()).thenReturn(asList(nameMeta));
        when(nameMeta.getCQLColumnName()).thenReturn("name");
        when(nameMeta.getPropertyName()).thenReturn("name");
        when(nameMeta.forTranscoding().encodeToCassandra("DuyHai")).thenReturn("DuyHai");

        //When
        final Object encoded = view.encodeCasConditionValue(LWTCondition);

        //Then
        assertThat(encoded).isEqualTo("DuyHai");
        verify(LWTCondition).encodedValue("DuyHai");
    }

    @Test
    public void should_encode_index_condition_value() throws Exception {
        //Given
        PropertyMeta nameMeta = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);
        IndexCondition indexCondition = spy(new IndexCondition("name", "DuyHai"));

        when(meta.getAllMetasExceptCounters()).thenReturn(asList(nameMeta));
        when(nameMeta.getCQLColumnName()).thenReturn("name");
        when(nameMeta.getPropertyName()).thenReturn("name");
        when(nameMeta.forTranscoding().encodeToCassandra("DuyHai")).thenReturn("DuyHai");

        //When
        final Object encoded = view.encodeIndexConditionValue(indexCondition);

        //Then
        assertThat(encoded).isEqualTo("DuyHai");
        verify(indexCondition).encodedValue("DuyHai");
    }

    @Test
    public void should_find_cql_name_for_partition_key() throws Exception {
        //Given
        PropertyMeta compoundPKMeta = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);
        PropertyMeta partitionKeyMeta = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);

        when(meta.getAllMetasExceptCounters()).thenReturn(asList(compoundPKMeta));
        when(compoundPKMeta.type()).thenReturn(PropertyType.COMPOUND_PRIMARY_KEY);

        when(compoundPKMeta.getCompoundPKProperties().getPartitionComponents().getPropertyMetas()).thenReturn(asList(partitionKeyMeta));
        when(compoundPKMeta.getCompoundPKProperties().getClusteringComponents().getPropertyMetas()).thenReturn(new ArrayList<PropertyMeta>());
        when(partitionKeyMeta.getCQLColumnName()).thenReturn("partitionKey");

        //When
        final PropertyMeta found = view.findPropertyMetaByCQLName("partitionKey");

        //Then
        assertThat(found).isSameAs(partitionKeyMeta);
    }

    @Test
    public void should_find_cql_name_for_clustering_key() throws Exception {
        //Given
        PropertyMeta compoundPKMeta = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);
        PropertyMeta clusteringColumnMeta = mock(PropertyMeta.class, RETURNS_DEEP_STUBS);

        when(meta.getAllMetasExceptCounters()).thenReturn(asList(compoundPKMeta));
        when(compoundPKMeta.type()).thenReturn(PropertyType.COMPOUND_PRIMARY_KEY);

        when(compoundPKMeta.getCompoundPKProperties().getPartitionComponents().getPropertyMetas()).thenReturn(new ArrayList<PropertyMeta>());
        when(compoundPKMeta.getCompoundPKProperties().getClusteringComponents().getPropertyMetas()).thenReturn(asList(clusteringColumnMeta));
        when(clusteringColumnMeta.getCQLColumnName()).thenReturn("clusteringColumn");

        //When
        final PropertyMeta found = view.findPropertyMetaByCQLName("clusteringColumn");

        //Then
        assertThat(found).isSameAs(clusteringColumnMeta);
    }
}
