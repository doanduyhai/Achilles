package info.archinnov.achilles.test.integration.tests;

import static info.archinnov.achilles.common.CQLCassandraDaoTest.truncateTable;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.common.CQLCassandraDaoTest;
import info.archinnov.achilles.entity.manager.CQLEntityManager;
import info.archinnov.achilles.test.integration.entity.ClusteredEntityWithObjectValue;
import info.archinnov.achilles.test.integration.entity.ClusteredEntityWithObjectValue.ClusteredKey;
import info.archinnov.achilles.test.integration.entity.ClusteredEntityWithObjectValue.Holder;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.lang.math.RandomUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.Test;
import com.datastax.driver.core.Session;

public class WideRowWithObjectPropertyIT
{

    private CQLEntityManager em = CQLCassandraDaoTest.getEm();

    private Session session = CQLCassandraDaoTest.getCqlSession();

    private ClusteredEntityWithObjectValue entity;

    private ClusteredKey compoundKey;

    private ObjectMapper mapper = new ObjectMapper();

    @Test
    public void should_persist_and_find() throws Exception
    {
        compoundKey = new ClusteredKey(RandomUtils.nextLong(), "name");
        Holder holder = new Holder("content");
        entity = new ClusteredEntityWithObjectValue(compoundKey, holder);

        em.persist(entity);

        ClusteredEntityWithObjectValue found = em.find(ClusteredEntityWithObjectValue.class,
                compoundKey);

        assertThat(found.getId()).isEqualTo(compoundKey);
        assertThat(found.getValue()).isEqualTo(holder);
    }

    @Test
    public void should_merge_and_get_reference() throws Exception
    {
        compoundKey = new ClusteredKey(RandomUtils.nextLong(), "name");
        Holder holder = new Holder("content");
        entity = new ClusteredEntityWithObjectValue(compoundKey, holder);

        em.merge(entity);

        ClusteredEntityWithObjectValue found = em.getReference(
                ClusteredEntityWithObjectValue.class, compoundKey);

        assertThat(found.getId()).isEqualTo(compoundKey);
        assertThat(found.getValue()).isEqualTo(holder);
    }

    @Test
    public void should_merge_modifications() throws Exception
    {

        compoundKey = new ClusteredKey(RandomUtils.nextLong(), "name");
        Holder holder = new Holder("content");
        Holder newHolder = new Holder("new_content");
        entity = new ClusteredEntityWithObjectValue(compoundKey, holder);

        entity = em.merge(entity);

        entity.setValue(newHolder);
        em.merge(entity);

        entity = em.find(ClusteredEntityWithObjectValue.class, compoundKey);

        assertThat(entity.getValue()).isEqualTo(newHolder);
    }

    @Test
    public void should_remove() throws Exception
    {
        compoundKey = new ClusteredKey(RandomUtils.nextLong(), "name");
        Holder holder = new Holder("content");
        entity = new ClusteredEntityWithObjectValue(compoundKey, holder);

        entity = em.merge(entity);

        em.remove(entity);

        assertThat(em.find(ClusteredEntityWithObjectValue.class, compoundKey)).isNull();

    }

    @Test
    public void should_refresh() throws Exception
    {

        long partitionKey = RandomUtils.nextLong();
        compoundKey = new ClusteredKey(partitionKey, "name");
        Holder holder = new Holder("content");
        Holder newHolder = new Holder("new_content");

        entity = new ClusteredEntityWithObjectValue(compoundKey, holder);

        entity = em.merge(entity);

        session.execute("UPDATE clustered_with_object_value SET value='" + mapper.writeValueAsString(newHolder)
                + "' where id=" + partitionKey + " and name='name'");
        em.refresh(entity);

        assertThat(entity.getValue()).isEqualTo(newHolder);
    }

    @Test
    public void should_query_with_default_params() throws Exception
    {
        long partitionKey = RandomUtils.nextLong();
        List<ClusteredEntityWithObjectValue> entities = em.sliceQuery(ClusteredEntityWithObjectValue.class)
                .partitionKey(partitionKey)
                .fromClusterings("name2")
                .toClusterings("name4")
                .get();

        assertThat(entities).isEmpty();

        insertValues(partitionKey, 5);

        entities = em.sliceQuery(ClusteredEntityWithObjectValue.class)
                .partitionKey(partitionKey)
                .fromClusterings("name2")
                .toClusterings("name4")
                .get();

        assertThat(entities).hasSize(3);

        assertThat(entities.get(0).getValue().getContent()).isEqualTo("name2");
        assertThat(entities.get(0).getId().getId()).isEqualTo(partitionKey);
        assertThat(entities.get(0).getId().getName()).isEqualTo("name2");
        assertThat(entities.get(1).getValue().getContent()).isEqualTo("name3");
        assertThat(entities.get(1).getId().getId()).isEqualTo(partitionKey);
        assertThat(entities.get(1).getId().getName()).isEqualTo("name3");
        assertThat(entities.get(2).getValue().getContent()).isEqualTo("name4");
        assertThat(entities.get(2).getId().getId()).isEqualTo(partitionKey);
        assertThat(entities.get(2).getId().getName()).isEqualTo("name4");

        entities = em.sliceQuery(ClusteredEntityWithObjectValue.class)
                .fromEmbeddedId(new ClusteredKey(partitionKey, "name2"))
                .toEmbeddedId(new ClusteredKey(partitionKey, "name4"))
                .get();

        assertThat(entities).hasSize(3);

        assertThat(entities.get(0).getValue().getContent()).isEqualTo("name2");
        assertThat(entities.get(0).getId().getId()).isEqualTo(partitionKey);
        assertThat(entities.get(0).getId().getName()).isEqualTo("name2");
        assertThat(entities.get(1).getValue().getContent()).isEqualTo("name3");
        assertThat(entities.get(1).getId().getId()).isEqualTo(partitionKey);
        assertThat(entities.get(1).getId().getName()).isEqualTo("name3");
        assertThat(entities.get(2).getValue().getContent()).isEqualTo("name4");
        assertThat(entities.get(2).getId().getId()).isEqualTo(partitionKey);
        assertThat(entities.get(2).getId().getName()).isEqualTo("name4");;
    }

    @Test
    public void should_iterate_with_default_params() throws Exception
    {
        long partitionKey = RandomUtils.nextLong();
        insertValues(partitionKey, 5);

        Iterator<ClusteredEntityWithObjectValue> iter = em.sliceQuery(ClusteredEntityWithObjectValue.class)
                .partitionKey(partitionKey)
                .iterator();

        assertThat(iter.hasNext()).isTrue();
        ClusteredEntityWithObjectValue next = iter.next();
        assertThat(next.getId().getId()).isEqualTo(partitionKey);
        assertThat(next.getId().getName()).isEqualTo("name1");
        assertThat(next.getValue().getContent()).isEqualTo("name1");
        assertThat(iter.hasNext()).isTrue();

        assertThat(iter.hasNext()).isTrue();
        next = iter.next();
        assertThat(next.getId().getId()).isEqualTo(partitionKey);
        assertThat(next.getId().getName()).isEqualTo("name2");
        assertThat(next.getValue().getContent()).isEqualTo("name2");

        assertThat(iter.hasNext()).isTrue();
        next = iter.next();
        assertThat(next.getId().getId()).isEqualTo(partitionKey);
        assertThat(next.getId().getName()).isEqualTo("name3");
        assertThat(next.getValue().getContent()).isEqualTo("name3");

        assertThat(iter.hasNext()).isTrue();
        next = iter.next();
        assertThat(next.getId().getId()).isEqualTo(partitionKey);
        assertThat(next.getId().getName()).isEqualTo("name4");
        assertThat(next.getValue().getContent()).isEqualTo("name4");

        assertThat(iter.hasNext()).isTrue();
        next = iter.next();
        assertThat(next.getId().getId()).isEqualTo(partitionKey);
        assertThat(next.getId().getName()).isEqualTo("name5");
        assertThat(next.getValue().getContent()).isEqualTo("name5");
        assertThat(iter.hasNext()).isFalse();
    }

    @Test
    public void should_remove_with_default_params() throws Exception
    {
        long partitionKey = RandomUtils.nextLong();
        insertValues(partitionKey, 3);

        em.sliceQuery(ClusteredEntityWithObjectValue.class)
                .partitionKey(partitionKey)
                .fromClusterings("name2")
                .toClusterings("name2")
                .remove();

        List<ClusteredEntityWithObjectValue> entities = em.sliceQuery(ClusteredEntityWithObjectValue.class)
                .partitionKey(partitionKey)
                .get(100);

        assertThat(entities).hasSize(2);

        assertThat(entities.get(0).getValue().getContent()).isEqualTo("name1");
        assertThat(entities.get(1).getValue().getContent()).isEqualTo("name3");
    }

    private void insertClusteredEntity(Long partitionKey, String name,
            Holder clusteredValue)
    {
        ClusteredKey embeddedId = new ClusteredKey(partitionKey, name);
        ClusteredEntityWithObjectValue entity = new ClusteredEntityWithObjectValue(embeddedId,
                clusteredValue);
        em.persist(entity);
    }

    private void insertValues(long partitionKey, int count)
    {
        String namePrefix = "name";

        for (int i = 1; i <= count; i++)
        {
            insertClusteredEntity(partitionKey, namePrefix + i, new Holder(namePrefix + i));
        }
    }

    @After
    public void tearDown()
    {
        truncateTable("clustered_with_object_value");
    }
}
