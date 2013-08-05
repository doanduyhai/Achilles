package info.archinnov.achilles.query;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.test.integration.entity.ClusteredEntity;
import info.archinnov.achilles.type.BoundingMode;
import info.archinnov.achilles.type.OrderingMode;
import org.junit.Test;

/**
 * SliceQueryTest
 * 
 * @author DuyHai DOAN
 * 
 */
public class SliceQueryTest {

    @Test
    public void should_build_new_slice_query() throws Exception
    {
        EntityMeta meta = new EntityMeta();
        SliceQuery<ClusteredEntity> sliceQuery = new SliceQuery<ClusteredEntity>(ClusteredEntity.class, meta, 11L,
                new Object[] { "a" }, new Object[] { "b" },
                OrderingMode.ASCENDING, BoundingMode.INCLUSIVE_BOUNDS, null, 100, 99, false);

        assertThat(sliceQuery.getEntityClass()).isSameAs(ClusteredEntity.class);
        assertThat(sliceQuery.getBatchSize()).isEqualTo(99);
        assertThat(sliceQuery.getBounding()).isEqualTo(BoundingMode.INCLUSIVE_BOUNDS);
        assertThat(sliceQuery.getClusteringsFrom()).containsExactly(11L, "a");
        assertThat(sliceQuery.getClusteringsTo()).containsExactly(11L, "b");
        assertThat(sliceQuery.getConsistencyLevel()).isNull();
        assertThat(sliceQuery.getLimit()).isEqualTo(100);
        assertThat(sliceQuery.getMeta()).isSameAs(meta);
        assertThat(sliceQuery.getOrdering()).isSameAs(OrderingMode.ASCENDING);
        assertThat(sliceQuery.getPartitionKey()).isEqualTo(11L);
        assertThat(sliceQuery.isLimitSet()).isFalse();
    }

    @Test
    public void should_return_true_when_no_component() throws Exception
    {
        EntityMeta meta = new EntityMeta();
        SliceQuery<ClusteredEntity> sliceQuery = new SliceQuery<ClusteredEntity>(ClusteredEntity.class, meta, 11L,
                null, null,
                OrderingMode.ASCENDING, BoundingMode.INCLUSIVE_BOUNDS, null, 100, 99, false);

        assertThat(sliceQuery.hasNoComponent()).isTrue();
    }
}
