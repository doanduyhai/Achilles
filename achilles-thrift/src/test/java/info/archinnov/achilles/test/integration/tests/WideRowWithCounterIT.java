package info.archinnov.achilles.test.integration.tests;

import static info.archinnov.achilles.common.ThriftCassandraDaoTest.getColumnFamilyDao;
import static info.archinnov.achilles.serializer.ThriftSerializerUtils.STRING_SRZ;
import static info.archinnov.achilles.table.TableNameNormalizer.normalizerAndValidateColumnFamilyName;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.common.ThriftCassandraDaoTest;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.manager.ThriftEntityManager;
import info.archinnov.achilles.proxy.wrapper.CounterBuilder;
import info.archinnov.achilles.test.integration.entity.ClusteredEntityWithCounter;
import info.archinnov.achilles.test.integration.entity.ClusteredEntityWithCounter.ClusteredKey;
import java.util.Iterator;
import java.util.List;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.mutation.Mutator;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.After;
import org.junit.Test;

/**
 * WideRowWithCounterIT
 * 
 * @author DuyHai DOAN
 * 
 */
public class WideRowWithCounterIT
{

    private ThriftGenericWideRowDao dao = getColumnFamilyDao(
            normalizerAndValidateColumnFamilyName("clustered_with_counter_value"),
            Long.class,
            Long.class);

    private ThriftEntityManager em = ThriftCassandraDaoTest.getEm();

    private ClusteredEntityWithCounter entity;

    private ClusteredKey compoundKey;

    @Test
    public void should_persist_and_find() throws Exception
    {
        long counterValue = RandomUtils.nextLong();
        compoundKey = new ClusteredKey(RandomUtils.nextLong(), "name");

        entity = new ClusteredEntityWithCounter(compoundKey, CounterBuilder.incr(counterValue));

        em.persist(entity);

        ClusteredEntityWithCounter found = em.find(ClusteredEntityWithCounter.class, compoundKey);

        assertThat(found.getId()).isEqualTo(compoundKey);
        assertThat(found.getCounter().get()).isEqualTo(counterValue);
    }

    @Test
    public void should_merge_and_get_reference() throws Exception
    {
        long counterValue = RandomUtils.nextLong();
        compoundKey = new ClusteredKey(RandomUtils.nextLong(), "name");
        entity = new ClusteredEntityWithCounter(compoundKey, CounterBuilder.incr(counterValue));

        em.merge(entity);

        ClusteredEntityWithCounter found = em.getReference(ClusteredEntityWithCounter.class,
                compoundKey);

        assertThat(found.getId()).isEqualTo(compoundKey);
        assertThat(found.getCounter().get()).isEqualTo(counterValue);
    }

    @Test
    public void should_merge_modifications() throws Exception
    {
        long counterValue = RandomUtils.nextLong();
        long incr = RandomUtils.nextLong();

        compoundKey = new ClusteredKey(RandomUtils.nextLong(), "name");

        entity = new ClusteredEntityWithCounter(compoundKey, CounterBuilder.incr(counterValue));

        entity = em.merge(entity);

        entity.getCounter().incr(incr);

        entity = em.find(ClusteredEntityWithCounter.class, compoundKey);

        assertThat(entity.getCounter().get()).isEqualTo(counterValue + incr);
    }

    @Test
    public void should_remove() throws Exception
    {
        long counterValue = RandomUtils.nextLong();
        compoundKey = new ClusteredKey(RandomUtils.nextLong(), "name");

        entity = new ClusteredEntityWithCounter(compoundKey, CounterBuilder.incr(counterValue));

        entity = em.merge(entity);

        em.remove(entity);

        Thread.sleep(2000);

        assertThat(em.find(ClusteredEntityWithCounter.class, compoundKey)).isNull();

    }

    @Test
    public void should_refresh() throws Exception
    {
        long counterValue = RandomUtils.nextLong();
        long incr = RandomUtils.nextLong();

        long partitionKey = RandomUtils.nextLong();
        String name = "name";
        compoundKey = new ClusteredKey(partitionKey, name);

        entity = new ClusteredEntityWithCounter(compoundKey, CounterBuilder.incr(counterValue));

        entity = em.merge(entity);

        Composite comp = new Composite();
        comp.setComponent(0, name, STRING_SRZ);
        Mutator<Long> mutator = dao.buildMutator();
        dao.incrementCounter(partitionKey, comp, incr);
        dao.executeMutator(mutator);

        em.refresh(entity);

        assertThat(entity.getCounter().get()).isEqualTo(counterValue + incr);

    }

    @Test
    public void should_query_with_default_params() throws Exception
    {
        long partitionKey = RandomUtils.nextLong();
        List<ClusteredEntityWithCounter> entities = em.sliceQuery(ClusteredEntityWithCounter.class)
                .partitionKey(partitionKey)
                .fromClusterings("name2")
                .toClusterings("name4")
                .get();

        assertThat(entities).isEmpty();

        insertValues(partitionKey, 5);

        entities = em.sliceQuery(ClusteredEntityWithCounter.class)
                .partitionKey(partitionKey)
                .fromClusterings("name2")
                .toClusterings("name4")
                .get();

        assertThat(entities).hasSize(3);

        assertThat(entities.get(0).getCounter().get()).isEqualTo(2);
        assertThat(entities.get(0).getId().getId()).isEqualTo(partitionKey);
        assertThat(entities.get(0).getId().getName()).isEqualTo("name2");
        assertThat(entities.get(1).getCounter().get()).isEqualTo(3);
        assertThat(entities.get(1).getId().getId()).isEqualTo(partitionKey);
        assertThat(entities.get(1).getId().getName()).isEqualTo("name3");
        assertThat(entities.get(2).getCounter().get()).isEqualTo(4);
        assertThat(entities.get(2).getId().getId()).isEqualTo(partitionKey);
        assertThat(entities.get(2).getId().getName()).isEqualTo("name4");

        entities = em.sliceQuery(ClusteredEntityWithCounter.class)
                .fromEmbeddedId(new ClusteredKey(partitionKey, "name2"))
                .toEmbeddedId(new ClusteredKey(partitionKey, "name4"))
                .get();

        assertThat(entities).hasSize(3);

        assertThat(entities.get(0).getCounter().get()).isEqualTo(2);
        assertThat(entities.get(0).getId().getId()).isEqualTo(partitionKey);
        assertThat(entities.get(0).getId().getName()).isEqualTo("name2");
        assertThat(entities.get(1).getCounter().get()).isEqualTo(3);
        assertThat(entities.get(1).getId().getId()).isEqualTo(partitionKey);
        assertThat(entities.get(1).getId().getName()).isEqualTo("name3");
        assertThat(entities.get(2).getCounter().get()).isEqualTo(4);
        assertThat(entities.get(2).getId().getId()).isEqualTo(partitionKey);
        assertThat(entities.get(2).getId().getName()).isEqualTo("name4");
    }

    @Test
    public void should_iterate_with_default_params() throws Exception
    {
        long partitionKey = RandomUtils.nextLong();
        insertValues(partitionKey, 5);

        Iterator<ClusteredEntityWithCounter> iter = em.sliceQuery(ClusteredEntityWithCounter.class)
                .partitionKey(partitionKey)
                .iterator();

        assertThat(iter.hasNext()).isTrue();
        ClusteredEntityWithCounter next = iter.next();
        assertThat(next.getId().getId()).isEqualTo(partitionKey);
        assertThat(next.getId().getName()).isEqualTo("name1");
        assertThat(next.getCounter().get()).isEqualTo(1L);
        assertThat(iter.hasNext()).isTrue();

        assertThat(iter.hasNext()).isTrue();
        next = iter.next();
        assertThat(next.getId().getId()).isEqualTo(partitionKey);
        assertThat(next.getId().getName()).isEqualTo("name2");
        assertThat(next.getCounter().get()).isEqualTo(2L);

        assertThat(iter.hasNext()).isTrue();
        next = iter.next();
        assertThat(next.getId().getId()).isEqualTo(partitionKey);
        assertThat(next.getId().getName()).isEqualTo("name3");
        assertThat(next.getCounter().get()).isEqualTo(3L);

        assertThat(iter.hasNext()).isTrue();
        next = iter.next();
        assertThat(next.getId().getId()).isEqualTo(partitionKey);
        assertThat(next.getId().getName()).isEqualTo("name4");
        assertThat(next.getCounter().get()).isEqualTo(4L);

        assertThat(iter.hasNext()).isTrue();
        next = iter.next();
        assertThat(next.getId().getId()).isEqualTo(partitionKey);
        assertThat(next.getId().getName()).isEqualTo("name5");
        assertThat(next.getCounter().get()).isEqualTo(5L);
        assertThat(iter.hasNext()).isFalse();
    }

    @Test
    public void should_remove_with_default_params() throws Exception
    {
        long partitionKey = RandomUtils.nextLong();
        insertValues(partitionKey, 5);

        em.sliceQuery(ClusteredEntityWithCounter.class)
                .partitionKey(partitionKey)
                .fromClusterings("name2")
                .toClusterings("name4")
                .remove();

        // Wait until counter column is really removed because of absence of tombstone
        Thread.sleep(1000);

        List<ClusteredEntityWithCounter> entities = em.sliceQuery(ClusteredEntityWithCounter.class)
                .partitionKey(partitionKey)
                .get(100);

        assertThat(entities).hasSize(2);

        assertThat(entities.get(0).getCounter().get()).isEqualTo(1L);
        assertThat(entities.get(1).getCounter().get()).isEqualTo(5L);
    }

    private void insertClusteredEntity(Long partitionKey, String name,
            Long clusteredCounter)
    {
        ClusteredKey embeddedId = new ClusteredKey(partitionKey, name);
        ClusteredEntityWithCounter entity = new ClusteredEntityWithCounter(embeddedId,
                CounterBuilder.incr(clusteredCounter));
        em.persist(entity);
    }

    private void insertValues(long partitionKey, int count)
    {
        String namePrefix = "name";

        for (int i = 1; i <= count; i++)
        {
            insertClusteredEntity(partitionKey, namePrefix + i, new Long(i));
        }
    }

    @After
    public void tearDown()
    {
        dao.truncate();
    }
}
