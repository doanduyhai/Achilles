package info.archinnov.achilles.query.slice;

import static info.archinnov.achilles.type.BoundingMode.EXCLUSIVE_BOUNDS;
import static info.archinnov.achilles.type.ConsistencyLevel.EACH_QUORUM;
import static info.archinnov.achilles.type.OrderingMode.DESCENDING;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.compound.CompoundKeyValidator;
import info.archinnov.achilles.context.ConfigurationContext;
import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.SliceQueryExecutor;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.query.SliceQuery;
import info.archinnov.achilles.test.mapping.entity.ClusteredEntity;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

@RunWith(MockitoJUnitRunner.class)
public class RootSliceQueryBuilderTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Mock(answer = Answers.CALLS_REAL_METHODS)
    private RootSliceQueryBuilder<PersistenceContext, ClusteredEntity> builder;

    private Class<ClusteredEntity> entityClass = ClusteredEntity.class;

    @Mock
    private SliceQueryExecutor<PersistenceContext> sliceQueryExecutor;

    @Mock
    private ConfigurationContext configContext;

    @Mock
    private EntityMeta meta;

    @Mock
    private PropertyMeta<?, ?> idMeta;

    @Mock
    private CompoundKeyValidator compoundKeyValidator;

    @Before
    public void setUp()
    {
        Whitebox.setInternalState(builder, "sliceQueryExecutor", sliceQueryExecutor);
        Whitebox.setInternalState(builder, "entityClass", (Object) entityClass);
        Whitebox.setInternalState(builder, "compoundKeyValidator", compoundKeyValidator);
        Whitebox.setInternalState(builder, "meta", meta);
        Whitebox.setInternalState(builder, "idMeta", idMeta);

        when(meta.getIdMeta()).thenReturn((PropertyMeta) idMeta);
        when(meta.getClassName()).thenReturn("entityClass");
        doCallRealMethod().when(builder).partitionKeyInternal(any());
    }

    @Test
    public void should_set_partition_keys() throws Exception
    {
        builder.partitionKeyInternal(11L);

        verify(compoundKeyValidator).validatePartitionKey(idMeta, 11L);

        assertThat(Whitebox.getInternalState(builder, "partitionKey")).isEqualTo(11L);
    }

    @Test
    public void should_set_clustering_from() throws Exception
    {
        builder.partitionKeyInternal(10L).fromClusteringsInternal(11L, "a", 12);

        verify(compoundKeyValidator).validateClusteringKeys(idMeta, 11L, "a", 12);

        assertThat(builder.buildClusterQuery().getClusteringsFrom()).containsExactly(10L, 11L, "a",
                12);

    }

    @Test
    public void should_set_clustering_to() throws Exception
    {
        builder.partitionKeyInternal(10L).toClusteringsInternal(11L, "a", 12);

        verify(compoundKeyValidator).validateClusteringKeys(idMeta, 11L, "a", 12);

        assertThat(builder.buildClusterQuery().getClusteringsTo()).containsExactly(10L, 11L, "a",
                12);
    }

    @Test
    public void should_set_ordering() throws Exception
    {
        builder.partitionKeyInternal(10L).ordering(DESCENDING);

        assertThat(builder.buildClusterQuery().getOrdering()).isEqualTo(DESCENDING);
    }

    @Test
    public void should_exception_when_null_ordering() throws Exception
    {
        exception.expect(AchillesException.class);
        exception
                .expectMessage("Ordering mode for slice query for entity 'entityClass' should not be null");

        builder.partitionKeyInternal(10L).ordering(null);
    }

    @Test
    public void should_set_bounding_mode() throws Exception
    {
        builder.partitionKeyInternal(10L).bounding(EXCLUSIVE_BOUNDS);

        assertThat(builder.buildClusterQuery().getBounding()).isEqualTo(EXCLUSIVE_BOUNDS);
    }

    @Test
    public void should_exception_when_null_bounding() throws Exception
    {
        exception.expect(AchillesException.class);
        exception
                .expectMessage("Bounding mode for slice query for entity 'entityClass' should not be null");

        builder.partitionKeyInternal(10L).bounding(null);
    }

    @Test
    public void should_set_consistency_level() throws Exception
    {
        builder.partitionKeyInternal(10L).consistencyLevel(EACH_QUORUM);

        assertThat(builder.buildClusterQuery().getConsistencyLevel()).isEqualTo(EACH_QUORUM);
    }

    @Test
    public void should_exception_when_null_consistency_level() throws Exception
    {
        exception.expect(AchillesException.class);
        exception
                .expectMessage("ConsistencyLevel for slice query for entity 'entityClass' should not be null");

        builder.partitionKeyInternal(10L).consistencyLevel(null);
    }

    @Test
    public void should_set_limit() throws Exception
    {
        builder.partitionKeyInternal(10L).limit(53);

        assertThat(builder.buildClusterQuery().getLimit()).isEqualTo(53);
    }

    @Test
    public void should_get() throws Exception
    {
        Long partitionKey = RandomUtils.nextLong();
        List<ClusteredEntity> list = mock(List.class);
        when(sliceQueryExecutor.get(any(SliceQuery.class))).thenReturn(list);

        List<ClusteredEntity> actual = builder.partitionKeyInternal(partitionKey).get();

        assertThat(actual).isSameAs(list);
    }

    @Test
    public void should_get_n() throws Exception
    {
        Long partitionKey = RandomUtils.nextLong();
        List<ClusteredEntity> list = mock(List.class);
        when(sliceQueryExecutor.get(any(SliceQuery.class))).thenReturn(list);

        List<ClusteredEntity> actual = builder.partitionKeyInternal(partitionKey).get(5);

        assertThat(actual).isSameAs(list);
        assertThat(Whitebox.getInternalState(builder, "limit")).isEqualTo(5);
    }

    @Test
    public void should_get_first() throws Exception
    {
        Long partitionKey = RandomUtils.nextLong();
        ClusteredEntity entity = new ClusteredEntity();
        when(sliceQueryExecutor.get(any(SliceQuery.class))).thenReturn(Arrays.asList(entity));

        ClusteredEntity actual = builder.partitionKeyInternal(partitionKey).getFirstOccurence();

        assertThat(actual).isSameAs(entity);

        assertThat(Whitebox.getInternalState(builder, "limit")).isEqualTo(1);
    }

    @Test
    public void should_get_first_with_clustering_components() throws Exception
    {
        Long partitionKey = RandomUtils.nextLong();
        ClusteredEntity entity = new ClusteredEntity();
        when(sliceQueryExecutor.get(any(SliceQuery.class))).thenReturn(Arrays.asList(entity));

        Object[] clusteringComponents = new Object[] { 1, "name" };
        ClusteredEntity actual = builder.partitionKeyInternal(partitionKey)
                .getFirstOccurence(clusteringComponents);

        assertThat(actual).isSameAs(entity);

        assertThat(Whitebox.getInternalState(builder, "limit")).isEqualTo(1);
        assertThat(Whitebox.getInternalState(builder, "fromClusterings")).isEqualTo(clusteringComponents);
        assertThat(Whitebox.getInternalState(builder, "toClusterings")).isEqualTo(clusteringComponents);
    }

    @Test
    public void should_return_null_when_no_first() throws Exception
    {
        Long partitionKey = RandomUtils.nextLong();
        when(sliceQueryExecutor.get(any(SliceQuery.class))).thenReturn(new ArrayList<ClusteredEntity>());

        ClusteredEntity actual = builder.partitionKeyInternal(partitionKey).getFirstOccurence();

        assertThat(actual).isNull();

        assertThat(Whitebox.getInternalState(builder, "limit")).isEqualTo(1);
    }

    @Test
    public void should_get_first_n() throws Exception
    {
        Long partitionKey = RandomUtils.nextLong();
        List<ClusteredEntity> list = mock(List.class);
        when(sliceQueryExecutor.get(any(SliceQuery.class))).thenReturn(list);

        List<ClusteredEntity> actual = builder.partitionKeyInternal(partitionKey).getFirst(3);

        assertThat(Whitebox.getInternalState(builder, "limit")).isEqualTo(3);
    }

    @Test
    public void should_get_first_n_with_clustering_components() throws Exception
    {
        Long partitionKey = RandomUtils.nextLong();
        List<ClusteredEntity> list = mock(List.class);
        when(sliceQueryExecutor.get(any(SliceQuery.class))).thenReturn(list);

        Object[] clusteringComponents = new Object[] { 1, "name" };
        List<ClusteredEntity> actual = builder.partitionKeyInternal(partitionKey).getFirst(3, clusteringComponents);

        assertThat(Whitebox.getInternalState(builder, "limit")).isEqualTo(3);
        assertThat(Whitebox.getInternalState(builder, "fromClusterings")).isEqualTo(clusteringComponents);
        assertThat(Whitebox.getInternalState(builder, "toClusterings")).isEqualTo(clusteringComponents);
    }

    @Test
    public void should_get_last() throws Exception
    {
        Long partitionKey = RandomUtils.nextLong();

        ClusteredEntity entity = new ClusteredEntity();
        when(sliceQueryExecutor.get(any(SliceQuery.class))).thenReturn(Arrays.asList(entity));

        ClusteredEntity actual = builder
                .partitionKeyInternal(partitionKey)
                .getLastOccurence();

        assertThat(actual).isSameAs(entity);

        assertThat(Whitebox.getInternalState(builder, "ordering")).isEqualTo(DESCENDING);
        assertThat(Whitebox.getInternalState(builder, "limit")).isEqualTo(1);
    }

    @Test
    public void should_get_last_with_clustering_components() throws Exception
    {
        Long partitionKey = RandomUtils.nextLong();

        ClusteredEntity entity = new ClusteredEntity();
        when(sliceQueryExecutor.get(any(SliceQuery.class))).thenReturn(Arrays.asList(entity));

        Object[] clusteringComponents = new Object[] { 1, "name" };

        ClusteredEntity actual = builder
                .partitionKeyInternal(partitionKey)
                .getLastOccurence(clusteringComponents);

        assertThat(actual).isSameAs(entity);

        assertThat(Whitebox.getInternalState(builder, "ordering")).isEqualTo(DESCENDING);
        assertThat(Whitebox.getInternalState(builder, "limit")).isEqualTo(1);
        assertThat(Whitebox.getInternalState(builder, "fromClusterings")).isEqualTo(clusteringComponents);
        assertThat(Whitebox.getInternalState(builder, "toClusterings")).isEqualTo(clusteringComponents);
    }

    @Test
    public void should_get_last_n() throws Exception
    {
        Long partitionKey = RandomUtils.nextLong();

        List<ClusteredEntity> list = mock(List.class);
        when(sliceQueryExecutor.get(any(SliceQuery.class))).thenReturn(list);

        List<ClusteredEntity> actual = builder.partitionKeyInternal(partitionKey).getLast(6);

        assertThat(Whitebox.getInternalState(builder, "ordering")).isEqualTo(DESCENDING);
        assertThat(Whitebox.getInternalState(builder, "limit")).isEqualTo(6);
    }

    @Test
    public void should_get_last_n_with_clustering_components() throws Exception
    {
        Long partitionKey = RandomUtils.nextLong();

        List<ClusteredEntity> list = mock(List.class);
        when(sliceQueryExecutor.get(any(SliceQuery.class))).thenReturn(list);

        Object[] clusteringComponents = new Object[] { 1, "name" };
        List<ClusteredEntity> actual = builder
                .partitionKeyInternal(partitionKey)
                .getLast(6, clusteringComponents);

        assertThat(Whitebox.getInternalState(builder, "ordering")).isEqualTo(DESCENDING);
        assertThat(Whitebox.getInternalState(builder, "limit")).isEqualTo(6);
        assertThat(Whitebox.getInternalState(builder, "fromClusterings")).isEqualTo(clusteringComponents);
        assertThat(Whitebox.getInternalState(builder, "toClusterings")).isEqualTo(clusteringComponents);

    }

    @Test
    public void should_get_iterator() throws Exception
    {
        Long partitionKey = RandomUtils.nextLong();
        Iterator<ClusteredEntity> iterator = mock(Iterator.class);

        when(sliceQueryExecutor.iterator(any(SliceQuery.class))).thenReturn(iterator);
        Iterator<ClusteredEntity> actual = builder.partitionKeyInternal(partitionKey).iterator();

        assertThat(actual).isSameAs(iterator);
    }

    @Test
    public void should_get_iterator_with_components() throws Exception
    {
        Long partitionKey = RandomUtils.nextLong();
        Iterator<ClusteredEntity> iterator = mock(Iterator.class);
        Object[] clusteringComponents = new Object[] { 1, "name" };

        when(sliceQueryExecutor.iterator(any(SliceQuery.class))).thenReturn(iterator);
        Iterator<ClusteredEntity> actual = builder.partitionKeyInternal(partitionKey)
                .iteratorWithComponents(clusteringComponents);

        assertThat(actual).isSameAs(iterator);
        assertThat(Whitebox.getInternalState(builder, "fromClusterings")).isEqualTo(clusteringComponents);
        assertThat(Whitebox.getInternalState(builder, "toClusterings")).isEqualTo(clusteringComponents);

    }

    @Test
    public void should_get_iterator_with_batch_size() throws Exception
    {
        Long partitionKey = RandomUtils.nextLong();
        Iterator<ClusteredEntity> iterator = mock(Iterator.class);

        when(sliceQueryExecutor.iterator(any(SliceQuery.class))).thenReturn(iterator);
        Iterator<ClusteredEntity> actual = builder.partitionKeyInternal(partitionKey).iterator(7);

        assertThat(Whitebox.getInternalState(builder, "batchSize")).isEqualTo(7);
        assertThat(actual).isSameAs(iterator);
    }

    @Test
    public void should_get_iterator_with_batch_size_and_components() throws Exception
    {
        Long partitionKey = RandomUtils.nextLong();
        Iterator<ClusteredEntity> iterator = mock(Iterator.class);
        Object[] clusteringComponents = new Object[] { 1, "name" };

        when(sliceQueryExecutor.iterator(any(SliceQuery.class))).thenReturn(iterator);
        Iterator<ClusteredEntity> actual = builder.partitionKeyInternal(partitionKey)
                .iteratorWithComponents(7, clusteringComponents);

        assertThat(Whitebox.getInternalState(builder, "batchSize")).isEqualTo(7);
        assertThat(actual).isSameAs(iterator);
        assertThat(Whitebox.getInternalState(builder, "fromClusterings")).isEqualTo(clusteringComponents);
        assertThat(Whitebox.getInternalState(builder, "toClusterings")).isEqualTo(clusteringComponents);

    }

    @Test
    public void should_remove() throws Exception
    {
        Long partitionKey = RandomUtils.nextLong();
        builder.partitionKeyInternal(partitionKey).remove();

        verify(sliceQueryExecutor).remove(any(SliceQuery.class));
    }

    @Test
    public void should_remove_n() throws Exception
    {
        Long partitionKey = RandomUtils.nextLong();
        builder.partitionKeyInternal(partitionKey).remove(8);

        assertThat(Whitebox.getInternalState(builder, "limit")).isEqualTo(8);
        verify(sliceQueryExecutor).remove(any(SliceQuery.class));
    }

    @Test
    public void should_remove_first() throws Exception
    {
        Long partitionKey = RandomUtils.nextLong();
        builder.partitionKeyInternal(partitionKey).removeFirstOccurence();

        assertThat(Whitebox.getInternalState(builder, "limit")).isEqualTo(1);
        verify(sliceQueryExecutor).remove(any(SliceQuery.class));
    }

    @Test
    public void should_remove_first_with_clustering_components() throws Exception
    {
        Long partitionKey = RandomUtils.nextLong();

        Object[] clusteringComponents = new Object[] { 1, "name" };

        builder.partitionKeyInternal(partitionKey).removeFirstOccurence(clusteringComponents);

        assertThat(Whitebox.getInternalState(builder, "limit")).isEqualTo(1);
        assertThat(Whitebox.getInternalState(builder, "fromClusterings")).isEqualTo(clusteringComponents);
        assertThat(Whitebox.getInternalState(builder, "toClusterings")).isEqualTo(clusteringComponents);
        verify(sliceQueryExecutor).remove(any(SliceQuery.class));
    }

    @Test
    public void should_remove_first_n() throws Exception
    {
        Long partitionKey = RandomUtils.nextLong();
        builder.partitionKeyInternal(partitionKey).removeFirst(9);

        assertThat(Whitebox.getInternalState(builder, "limit")).isEqualTo(9);
        verify(sliceQueryExecutor).remove(any(SliceQuery.class));
    }

    @Test
    public void should_remove_first_n_with_clustering_components() throws Exception
    {
        Long partitionKey = RandomUtils.nextLong();
        Object[] clusteringComponents = new Object[] { 1, "name" };
        builder.partitionKeyInternal(partitionKey).removeFirst(9, clusteringComponents);

        assertThat(Whitebox.getInternalState(builder, "limit")).isEqualTo(9);
        assertThat(Whitebox.getInternalState(builder, "fromClusterings")).isEqualTo(clusteringComponents);
        assertThat(Whitebox.getInternalState(builder, "toClusterings")).isEqualTo(clusteringComponents);
        verify(sliceQueryExecutor).remove(any(SliceQuery.class));
    }

    @Test
    public void should_remove_last() throws Exception
    {
        Long partitionKey = RandomUtils.nextLong();
        builder.partitionKeyInternal(partitionKey).removeLastOccurence();

        assertThat(Whitebox.getInternalState(builder, "ordering")).isEqualTo(DESCENDING);
        assertThat(Whitebox.getInternalState(builder, "limit")).isEqualTo(1);

        verify(sliceQueryExecutor).remove(any(SliceQuery.class));
    }

    @Test
    public void should_remove_last_with_clustering_components() throws Exception
    {
        Long partitionKey = RandomUtils.nextLong();
        Object[] clusteringComponents = new Object[] { 1, "name" };
        builder.partitionKeyInternal(partitionKey).removeLastOccurence(clusteringComponents);

        assertThat(Whitebox.getInternalState(builder, "ordering")).isEqualTo(DESCENDING);
        assertThat(Whitebox.getInternalState(builder, "limit")).isEqualTo(1);
        assertThat(Whitebox.getInternalState(builder, "fromClusterings")).isEqualTo(clusteringComponents);
        assertThat(Whitebox.getInternalState(builder, "toClusterings")).isEqualTo(clusteringComponents);
        verify(sliceQueryExecutor).remove(any(SliceQuery.class));
    }

    @Test
    public void should_remove_last_n() throws Exception
    {
        Long partitionKey = RandomUtils.nextLong();
        builder.partitionKeyInternal(partitionKey).removeLast(10);

        assertThat(Whitebox.getInternalState(builder, "ordering")).isEqualTo(DESCENDING);
        assertThat(Whitebox.getInternalState(builder, "limit")).isEqualTo(10);
        verify(sliceQueryExecutor).remove(any(SliceQuery.class));
    }

    @Test
    public void should_remove_last_n_with_clustering_components() throws Exception
    {
        Long partitionKey = RandomUtils.nextLong();
        Object[] clusteringComponents = new Object[] { 1, "name" };
        builder.partitionKeyInternal(partitionKey).removeLast(10, clusteringComponents);

        assertThat(Whitebox.getInternalState(builder, "ordering")).isEqualTo(DESCENDING);
        assertThat(Whitebox.getInternalState(builder, "limit")).isEqualTo(10);
        assertThat(Whitebox.getInternalState(builder, "fromClusterings")).isEqualTo(clusteringComponents);
        assertThat(Whitebox.getInternalState(builder, "toClusterings")).isEqualTo(clusteringComponents);
        verify(sliceQueryExecutor).remove(any(SliceQuery.class));
    }
}
