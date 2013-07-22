package info.archinnov.achilles.test.integration.tests;

import static info.archinnov.achilles.common.ThriftCassandraDaoTest.getColumnFamilyDao;
import static info.archinnov.achilles.table.TableNameNormalizer.normalizerAndValidateColumnFamilyName;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.common.ThriftCassandraDaoTest;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.manager.ThriftEntityManager;
import info.archinnov.achilles.test.integration.entity.ClusteredEntityWithJoinEntity;
import info.archinnov.achilles.test.integration.entity.ClusteredEntityWithJoinEntity.ClusteredKey;
import info.archinnov.achilles.test.integration.entity.User;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.After;
import org.junit.Test;

public class WideRowWithJoinEntityIT
{

    private ThriftGenericWideRowDao dao = getColumnFamilyDao(
            normalizerAndValidateColumnFamilyName("clustered_with_join_value"),
            Long.class,
            User.class);

    private ThriftEntityManager em = ThriftCassandraDaoTest.getEm();

    private ClusteredEntityWithJoinEntity entity;

    private ClusteredKey compoundKey;

    @Test
    public void should_persist_and_find() throws Exception
    {
        compoundKey = new ClusteredKey(RandomUtils.nextLong(), "name");
        User user = new User(RandomUtils.nextLong(), "firstname", "lastname");
        entity = new ClusteredEntityWithJoinEntity(compoundKey, user);

        em.persist(entity);

        ClusteredEntityWithJoinEntity found = em.find(ClusteredEntityWithJoinEntity.class, compoundKey);

        assertThat(found.getId()).isEqualTo(compoundKey);
        User proxifiedUser = found.getFriend();
        assertThat(em.unwrap(proxifiedUser)).isEqualTo(user);
    }

    @Test
    public void should_merge_and_get_reference() throws Exception
    {
        compoundKey = new ClusteredKey(RandomUtils.nextLong(), "name");
        User user = new User(RandomUtils.nextLong(), "firstname", "lastname");
        entity = new ClusteredEntityWithJoinEntity(compoundKey, user);

        em.merge(entity);

        ClusteredEntityWithJoinEntity found = em.getReference(ClusteredEntityWithJoinEntity.class,
                compoundKey);

        assertThat(found.getId()).isEqualTo(compoundKey);
        User proxifiedUser = found.getFriend();
        assertThat(em.unwrap(proxifiedUser)).isEqualTo(user);
    }

    @Test
    public void should_merge_modifications() throws Exception
    {

        compoundKey = new ClusteredKey(RandomUtils.nextLong(), "name");
        User user = new User(RandomUtils.nextLong(), "firstname", "lastname");
        User newUser = new User(RandomUtils.nextLong(), "new_firstname", "new_lastname");
        entity = new ClusteredEntityWithJoinEntity(compoundKey, user);

        entity = em.merge(entity);

        entity.setFriend(newUser);
        em.merge(entity);

        entity = em.find(ClusteredEntityWithJoinEntity.class, compoundKey);

        User proxifiedUser = entity.getFriend();
        assertThat(em.unwrap(proxifiedUser)).isEqualTo(newUser);
    }

    @Test
    public void should_remove() throws Exception
    {
        compoundKey = new ClusteredKey(RandomUtils.nextLong(), "name");
        User user = new User(RandomUtils.nextLong(), "firstname", "lastname");
        entity = new ClusteredEntityWithJoinEntity(compoundKey, user);

        entity = em.merge(entity);

        em.remove(entity);

        assertThat(em.find(ClusteredEntityWithJoinEntity.class, compoundKey)).isNull();

    }

    @Test
    public void should_refresh() throws Exception
    {

        long partitionKey = RandomUtils.nextLong();
        compoundKey = new ClusteredKey(partitionKey, "name");
        User user = new User(RandomUtils.nextLong(), "firstname", "lastname");
        User newUser = new User(RandomUtils.nextLong(), "new_firstname", "new_lastname");

        entity = new ClusteredEntityWithJoinEntity(compoundKey, user);

        entity = em.merge(entity);

        ClusteredEntityWithJoinEntity anotherEntity = em.find(ClusteredEntityWithJoinEntity.class,
                compoundKey);
        anotherEntity.setFriend(newUser);
        em.merge(anotherEntity);

        em.refresh(entity);

        User proxifiedUser = entity.getFriend();
        assertThat(em.unwrap(proxifiedUser)).isEqualTo(newUser);

    }

    @Test
    public void should_query_with_default_params() throws Exception
    {
        long partitionKey = RandomUtils.nextLong();
        List<ClusteredEntityWithJoinEntity> entities = em.sliceQuery(ClusteredEntityWithJoinEntity.class)
                .partitionKey(partitionKey)
                .fromClusterings("name2")
                .toClusterings("name4")
                .get();

        assertThat(entities).isEmpty();

        insertValues(partitionKey, 5);

        entities = em.sliceQuery(ClusteredEntityWithJoinEntity.class)
                .partitionKey(partitionKey)
                .fromClusterings("name2")
                .toClusterings("name4")
                .get();

        assertThat(entities).hasSize(3);

        assertThat(entities.get(0).getFriend().getId()).isEqualTo(2);
        assertThat(entities.get(0).getId().getId()).isEqualTo(partitionKey);
        assertThat(entities.get(0).getId().getName()).isEqualTo("name2");
        assertThat(entities.get(1).getFriend().getId()).isEqualTo(3);
        assertThat(entities.get(1).getId().getId()).isEqualTo(partitionKey);
        assertThat(entities.get(1).getId().getName()).isEqualTo("name3");
        assertThat(entities.get(2).getFriend().getId()).isEqualTo(4);
        assertThat(entities.get(2).getId().getId()).isEqualTo(partitionKey);
        assertThat(entities.get(2).getId().getName()).isEqualTo("name4");

        entities = em.sliceQuery(ClusteredEntityWithJoinEntity.class)
                .fromEmbeddedId(new ClusteredKey(partitionKey, "name2"))
                .toEmbeddedId(new ClusteredKey(partitionKey, "name4"))
                .get();

        assertThat(entities).hasSize(3);

        assertThat(entities.get(0).getFriend().getId()).isEqualTo(2);
        assertThat(entities.get(0).getId().getId()).isEqualTo(partitionKey);
        assertThat(entities.get(0).getId().getName()).isEqualTo("name2");
        assertThat(entities.get(1).getFriend().getId()).isEqualTo(3);
        assertThat(entities.get(1).getId().getId()).isEqualTo(partitionKey);
        assertThat(entities.get(1).getId().getName()).isEqualTo("name3");
        assertThat(entities.get(2).getFriend().getId()).isEqualTo(4);
        assertThat(entities.get(2).getId().getId()).isEqualTo(partitionKey);
        assertThat(entities.get(2).getId().getName()).isEqualTo("name4");
    }

    @Test
    public void should_iterate_with_default_params() throws Exception
    {
        long partitionKey = RandomUtils.nextLong();
        insertValues(partitionKey, 5);

        Iterator<ClusteredEntityWithJoinEntity> iter = em.sliceQuery(ClusteredEntityWithJoinEntity.class)
                .partitionKey(partitionKey)
                .iterator();

        assertThat(iter.hasNext()).isTrue();
        ClusteredEntityWithJoinEntity next = iter.next();
        assertThat(next.getId().getId()).isEqualTo(partitionKey);
        assertThat(next.getId().getName()).isEqualTo("name1");
        assertThat(next.getFriend().getId()).isEqualTo(1L);
        assertThat(iter.hasNext()).isTrue();

        assertThat(iter.hasNext()).isTrue();
        next = iter.next();
        assertThat(next.getId().getId()).isEqualTo(partitionKey);
        assertThat(next.getId().getName()).isEqualTo("name2");
        assertThat(next.getFriend().getId()).isEqualTo(2L);

        assertThat(iter.hasNext()).isTrue();
        next = iter.next();
        assertThat(next.getId().getId()).isEqualTo(partitionKey);
        assertThat(next.getId().getName()).isEqualTo("name3");
        assertThat(next.getFriend().getId()).isEqualTo(3L);

        assertThat(iter.hasNext()).isTrue();
        next = iter.next();
        assertThat(next.getId().getId()).isEqualTo(partitionKey);
        assertThat(next.getId().getName()).isEqualTo("name4");
        assertThat(next.getFriend().getId()).isEqualTo(4L);

        assertThat(iter.hasNext()).isTrue();
        next = iter.next();
        assertThat(next.getId().getId()).isEqualTo(partitionKey);
        assertThat(next.getId().getName()).isEqualTo("name5");
        assertThat(next.getFriend().getId()).isEqualTo(5L);
        assertThat(iter.hasNext()).isFalse();
    }

    @Test
    public void should_remove_with_default_params() throws Exception
    {
        long partitionKey = RandomUtils.nextLong();
        insertValues(partitionKey, 5);

        em.sliceQuery(ClusteredEntityWithJoinEntity.class)
                .partitionKey(partitionKey)
                .fromClusterings("name2")
                .toClusterings("name4")
                .remove();

        List<ClusteredEntityWithJoinEntity> entities = em.sliceQuery(ClusteredEntityWithJoinEntity.class)
                .partitionKey(partitionKey)
                .get(100);

        assertThat(entities).hasSize(2);

        assertThat(entities.get(0).getFriend().getId()).isEqualTo(1L);
        assertThat(entities.get(1).getFriend().getId()).isEqualTo(5L);
    }

    private void insertClusteredEntity(Long partitionKey, String name,
            User friend)
    {
        ClusteredKey embeddedId = new ClusteredKey(partitionKey, name);
        ClusteredEntityWithJoinEntity entity = new ClusteredEntityWithJoinEntity(embeddedId,
                friend);
        em.persist(entity);
    }

    private void insertValues(long partitionKey, int count)
    {
        String namePrefix = "name";

        for (int i = 1; i <= count; i++)
        {
            User user = new User(new Long(i), "firstname" + i, "lastname" + i);
            insertClusteredEntity(partitionKey, namePrefix + i, user);
        }
    }

    @After
    public void tearDown()
    {
        dao.truncate();
    }
}
