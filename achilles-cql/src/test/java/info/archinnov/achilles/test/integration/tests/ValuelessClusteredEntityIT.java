package info.archinnov.achilles.test.integration.tests;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.entity.manager.CQLEntityManager;
import info.archinnov.achilles.junit.AchillesInternalCQLResource;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.test.integration.entity.ValuelessClusteredEntity;
import info.archinnov.achilles.test.integration.entity.ValuelessClusteredEntity.CompoundKey;
import info.archinnov.achilles.type.BoundingMode;
import info.archinnov.achilles.type.OptionsBuilder;
import info.archinnov.achilles.type.OrderingMode;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.Rule;
import org.junit.Test;

/**
 * ValuelessClusteredEntityIT
 * 
 * @author DuyHai DOAN
 * 
 */
public class ValuelessClusteredEntityIT {

    @Rule
    public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(Steps.AFTER_TEST,
            "ValuelessClusteredEntity");

    private CQLEntityManager em = resource.getEm();

    @Test
    public void should_persist_and_find() throws Exception
    {
        Long id = RandomUtils.nextLong();
        String name = "name";
        CompoundKey compoundKey = new CompoundKey(id, name);
        ValuelessClusteredEntity entity = new ValuelessClusteredEntity(compoundKey);

        em.persist(entity);

        ValuelessClusteredEntity found = em.find(ValuelessClusteredEntity.class, compoundKey);

        assertThat(found).isNotNull();
    }

    @Test
    public void should_merge_and_get_reference() throws Exception
    {
        Long id = RandomUtils.nextLong();
        String name = "name";
        CompoundKey compoundKey = new CompoundKey(id, name);
        ValuelessClusteredEntity entity = new ValuelessClusteredEntity(compoundKey);

        em.merge(entity);

        ValuelessClusteredEntity found = em.getReference(ValuelessClusteredEntity.class, compoundKey);

        assertThat(found).isNotNull();
    }

    @Test
    public void should_persist_with_ttl() throws Exception
    {
        Long id = RandomUtils.nextLong();
        String name = "name";
        CompoundKey compoundKey = new CompoundKey(id, name);
        ValuelessClusteredEntity entity = new ValuelessClusteredEntity(compoundKey);

        em.persist(entity, OptionsBuilder.withTtl(2));

        Thread.sleep(3000);

        assertThat(em.find(ValuelessClusteredEntity.class, compoundKey)).isNull();
    }

    @Test
    public void should_merge_with_ttl() throws Exception
    {
        Long id = RandomUtils.nextLong();
        String name = "name";
        CompoundKey compoundKey = new CompoundKey(id, name);
        ValuelessClusteredEntity entity = new ValuelessClusteredEntity(compoundKey);

        em.merge(entity, OptionsBuilder.withTtl(2));

        Thread.sleep(3000);

        assertThat(em.find(ValuelessClusteredEntity.class, compoundKey)).isNull();
    }

    @Test
    public void should_find_by_slice_query() throws Exception
    {
        Long id = RandomUtils.nextLong();
        String name1 = "name1";
        String name2 = "name2";
        String name3 = "name3";
        String name4 = "name4";
        String name5 = "name5";

        em.persist(new ValuelessClusteredEntity(new CompoundKey(id, name1)));
        em.persist(new ValuelessClusteredEntity(new CompoundKey(id, name2)));
        em.persist(new ValuelessClusteredEntity(new CompoundKey(id, name3)));
        em.persist(new ValuelessClusteredEntity(new CompoundKey(id, name4)));
        em.persist(new ValuelessClusteredEntity(new CompoundKey(id, name5)));

        List<ValuelessClusteredEntity> result = em.sliceQuery(ValuelessClusteredEntity.class)
                .partitionKey(id)
                .fromClusterings(name5)
                .toClusterings(name2)
                .bounding(BoundingMode.INCLUSIVE_START_BOUND_ONLY)
                .ordering(OrderingMode.DESCENDING)
                .limit(3)
                .get();

        assertThat(result).hasSize(3);
        assertThat(result.get(0).getId().getName()).isEqualTo(name5);
        assertThat(result.get(1).getId().getName()).isEqualTo(name4);
        assertThat(result.get(2).getId().getName()).isEqualTo(name3);
    }

    @Test
    public void should_iterate_by_slice_query() throws Exception
    {
        Long id = RandomUtils.nextLong();
        String name1 = "name1";
        String name2 = "name2";
        String name3 = "name3";
        String name4 = "name4";
        String name5 = "name5";

        em.persist(new ValuelessClusteredEntity(new CompoundKey(id, name1)));
        em.persist(new ValuelessClusteredEntity(new CompoundKey(id, name2)));
        em.persist(new ValuelessClusteredEntity(new CompoundKey(id, name3)));
        em.persist(new ValuelessClusteredEntity(new CompoundKey(id, name4)));
        em.persist(new ValuelessClusteredEntity(new CompoundKey(id, name5)));

        Iterator<ValuelessClusteredEntity> iterator = em.sliceQuery(ValuelessClusteredEntity.class)
                .partitionKey(id)
                .fromClusterings(name5)
                .toClusterings(name2)
                .bounding(BoundingMode.INCLUSIVE_START_BOUND_ONLY)
                .ordering(OrderingMode.DESCENDING)
                .iterator();

        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.next().getId().getName()).isEqualTo(name5);
        assertThat(iterator.next().getId().getName()).isEqualTo(name4);
        assertThat(iterator.next().getId().getName()).isEqualTo(name3);
        assertThat(iterator.hasNext()).isFalse();
    }

    @Test
    public void should_remove_by_slice_query() throws Exception
    {
        Long id = RandomUtils.nextLong();
        String name1 = "name1";
        String name2 = "name2";
        String name3 = "name3";

        em.persist(new ValuelessClusteredEntity(new CompoundKey(id, name1)));
        em.persist(new ValuelessClusteredEntity(new CompoundKey(id, name2)));
        em.persist(new ValuelessClusteredEntity(new CompoundKey(id, name3)));

        em.sliceQuery(ValuelessClusteredEntity.class)
                .partitionKey(id)
                .fromClusterings(name2)
                .toClusterings(name2)
                .remove();

        List<ValuelessClusteredEntity> result = em.sliceQuery(ValuelessClusteredEntity.class)
                .partitionKey(id)
                .get();

        assertThat(result).hasSize(2);
        assertThat(result.get(0).getId().getName()).isEqualTo(name1);
        assertThat(result.get(1).getId().getName()).isEqualTo(name3);
    }
}
