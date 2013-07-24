package info.archinnov.achilles.query.slice;

import static info.archinnov.achilles.type.BoundingMode.INCLUSIVE_END_BOUND_ONLY;
import static info.archinnov.achilles.type.OrderingMode.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.query.SliceQuery;
import info.archinnov.achilles.test.mapping.entity.ClusteredEntity;
import info.archinnov.achilles.type.ConsistencyLevel;
import java.util.Arrays;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;
import com.datastax.driver.core.querybuilder.Ordering;

/**
 * CQLSliceQueryTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class CQLSliceQueryTest {

    private CQLSliceQuery<ClusteredEntity> cqlSliceQuery;

    @Mock
    private SliceQuery<ClusteredEntity> sliceQuery;

    @Before
    public void setUp()
    {
        when(sliceQuery.getClusteringsFrom()).thenReturn(Arrays.<Object> asList(1, 2));
        when(sliceQuery.getClusteringsTo()).thenReturn(Arrays.<Object> asList(1, 2));
    }

    @Test
    public void should_get_fixed_components_when_same_size() throws Exception
    {
        when(sliceQuery.getClusteringsFrom()).thenReturn(Arrays.<Object> asList(11L, "a", 11.0));
        when(sliceQuery.getClusteringsTo()).thenReturn(Arrays.<Object> asList(11L, "a", 12.0));

        cqlSliceQuery = new CQLSliceQuery<ClusteredEntity>(sliceQuery);

        assertThat(cqlSliceQuery.getFixedComponents()).containsExactly(11L, "a");
    }

    @Test
    public void should_get_fixed_components_when_start_same_as_end() throws Exception
    {
        when(sliceQuery.getClusteringsFrom()).thenReturn(Arrays.<Object> asList(11L, "a", 12.0));
        when(sliceQuery.getClusteringsTo()).thenReturn(Arrays.<Object> asList(11L, "a", 12.0));

        cqlSliceQuery = new CQLSliceQuery<ClusteredEntity>(sliceQuery);

        assertThat(cqlSliceQuery.getFixedComponents()).containsExactly(11L, "a", 12.0);
    }

    @Test
    public void should_get_fixed_components_when_start_more_than_end() throws Exception
    {

        when(sliceQuery.getClusteringsFrom()).thenReturn(Arrays.<Object> asList(11L, "a", 11.0));
        when(sliceQuery.getClusteringsTo()).thenReturn(Arrays.<Object> asList(11L, "a"));

        cqlSliceQuery = new CQLSliceQuery<ClusteredEntity>(sliceQuery);

        assertThat(cqlSliceQuery.getFixedComponents()).containsExactly(11L, "a");
    }

    @Test
    public void should_get_last_components_when_same_size() throws Exception
    {
        when(sliceQuery.getClusteringsFrom()).thenReturn(Arrays.<Object> asList(11L, "a", 11.0));
        when(sliceQuery.getClusteringsTo()).thenReturn(Arrays.<Object> asList(11L, "a", 12.0));

        cqlSliceQuery = new CQLSliceQuery<ClusteredEntity>(sliceQuery);

        assertThat(cqlSliceQuery.getLastStartComponent()).isEqualTo(11.0);
        assertThat(cqlSliceQuery.getLastEndComponent()).isEqualTo(12.0);
    }

    @Test
    public void should_get_last_components_when_start_same_as_end() throws Exception
    {
        when(sliceQuery.getClusteringsFrom()).thenReturn(Arrays.<Object> asList(11L, "a", 12.0));
        when(sliceQuery.getClusteringsTo()).thenReturn(Arrays.<Object> asList(11L, "a", 12.0));

        cqlSliceQuery = new CQLSliceQuery<ClusteredEntity>(sliceQuery);

        assertThat(cqlSliceQuery.getLastStartComponent()).isNull();
        assertThat(cqlSliceQuery.getLastEndComponent()).isNull();
    }

    @Test
    public void should_get_last_components_when_start_more_than_end() throws Exception
    {
        when(sliceQuery.getClusteringsFrom()).thenReturn(Arrays.<Object> asList(11L, "a", 11.0));
        when(sliceQuery.getClusteringsTo()).thenReturn(Arrays.<Object> asList(11L, "a"));

        cqlSliceQuery = new CQLSliceQuery<ClusteredEntity>(sliceQuery);

        assertThat(cqlSliceQuery.getLastStartComponent()).isEqualTo(11.0);
        assertThat(cqlSliceQuery.getLastEndComponent()).isNull();
    }

    @Test
    public void should_get_last_components_when_end_more_than_start() throws Exception
    {
        when(sliceQuery.getClusteringsFrom()).thenReturn(Arrays.<Object> asList(11L, "a"));
        when(sliceQuery.getClusteringsTo()).thenReturn(Arrays.<Object> asList(11L, "a", 12.0));

        cqlSliceQuery = new CQLSliceQuery<ClusteredEntity>(sliceQuery);

        assertThat(cqlSliceQuery.getLastStartComponent()).isNull();
        assertThat(cqlSliceQuery.getLastEndComponent()).isEqualTo(12.0);
    }

    @Test
    public void should_get_limit() throws Exception
    {
        when(sliceQuery.getLimit()).thenReturn(99);

        cqlSliceQuery = new CQLSliceQuery<ClusteredEntity>(sliceQuery);

        assertThat(cqlSliceQuery.getLimit()).isEqualTo(99);
    }

    @Test
    public void should_get_cql_consistency_level() throws Exception
    {
        when(sliceQuery.getConsistencyLevel()).thenReturn(ConsistencyLevel.EACH_QUORUM);

        cqlSliceQuery = new CQLSliceQuery<ClusteredEntity>(sliceQuery);

        assertThat(cqlSliceQuery.getConsistencyLevel()).isEqualTo(
                com.datastax.driver.core.ConsistencyLevel.EACH_QUORUM);
    }

    @Test
    public void should_get_bounding() throws Exception
    {
        when(sliceQuery.getBounding()).thenReturn(INCLUSIVE_END_BOUND_ONLY);

        cqlSliceQuery = new CQLSliceQuery<ClusteredEntity>(sliceQuery);

        assertThat(cqlSliceQuery.getBounding()).isEqualTo(INCLUSIVE_END_BOUND_ONLY);
    }

    @Test
    public void should_get_ordering_asc() throws Exception
    {
        sliceQuery = mock(SliceQuery.class, RETURNS_DEEP_STUBS);
        when(sliceQuery.getMeta().getIdMeta().getOrderingComponent()).thenReturn("orderingComp");
        when(sliceQuery.getOrdering()).thenReturn(ASCENDING);

        cqlSliceQuery = new CQLSliceQuery<ClusteredEntity>(sliceQuery);

        Ordering ordering = cqlSliceQuery.getOrdering();
        assertThat(Whitebox.getInternalState(ordering, "name")).isEqualTo("orderingComp");
        assertThat((Boolean) Whitebox.getInternalState(ordering, "isDesc")).isFalse();
    }

    @Test
    public void should_get_ordering_des() throws Exception
    {
        sliceQuery = mock(SliceQuery.class, RETURNS_DEEP_STUBS);
        when(sliceQuery.getMeta().getIdMeta().getOrderingComponent()).thenReturn("orderingComp");
        when(sliceQuery.getOrdering()).thenReturn(DESCENDING);

        cqlSliceQuery = new CQLSliceQuery<ClusteredEntity>(sliceQuery);

        Ordering ordering = cqlSliceQuery.getOrdering();
        assertThat(Whitebox.getInternalState(ordering, "name")).isEqualTo("orderingComp");
        assertThat((Boolean) Whitebox.getInternalState(ordering, "isDesc")).isTrue();
    }

    @Test
    public void should_get_components_name() throws Exception
    {
        sliceQuery = mock(SliceQuery.class, RETURNS_DEEP_STUBS);
        when(sliceQuery.getMeta().getIdMeta().getComponentNames()).thenReturn(Arrays.asList("id", "count", "name"));

        cqlSliceQuery = new CQLSliceQuery<ClusteredEntity>(sliceQuery);

        assertThat(cqlSliceQuery.getComponentNames()).containsExactly("id", "count", "name");
    }

    @Test
    public void should_get_meta() throws Exception
    {
        EntityMeta meta = new EntityMeta();
        when(sliceQuery.getMeta()).thenReturn(meta);

        cqlSliceQuery = new CQLSliceQuery<ClusteredEntity>(sliceQuery);

        assertThat(cqlSliceQuery.getMeta()).isSameAs(meta);
    }
}
