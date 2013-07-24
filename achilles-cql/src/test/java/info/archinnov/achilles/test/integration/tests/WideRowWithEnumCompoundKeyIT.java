package info.archinnov.achilles.test.integration.tests;

import static info.archinnov.achilles.common.CQLCassandraDaoTest.truncateTable;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.common.CQLCassandraDaoTest;
import info.archinnov.achilles.entity.manager.CQLEntityManager;
import info.archinnov.achilles.test.integration.entity.ClusteredEntityWithEnumCompoundKey;
import info.archinnov.achilles.test.integration.entity.ClusteredEntityWithEnumCompoundKey.ClusteredKey;
import info.archinnov.achilles.test.integration.entity.ClusteredEntityWithEnumCompoundKey.Type;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.After;
import org.junit.Test;
import com.datastax.driver.core.Session;

public class WideRowWithEnumCompoundKeyIT
{

    private CQLEntityManager em = CQLCassandraDaoTest.getEm();

    private Session session = CQLCassandraDaoTest.getCqlSession();

    private ClusteredEntityWithEnumCompoundKey entity;

    private ClusteredKey compoundKey;

    @Test
    public void should_persist_and_get_reference() throws Exception
    {
        compoundKey = new ClusteredKey(RandomUtils.nextLong(), Type.AUDIO);

        entity = new ClusteredEntityWithEnumCompoundKey(compoundKey, "clustered_value");

        em.persist(entity);

        ClusteredEntityWithEnumCompoundKey found = em.getReference(
                ClusteredEntityWithEnumCompoundKey.class, compoundKey);

        assertThat(found.getId()).isEqualTo(compoundKey);
        assertThat(found.getValue()).isEqualTo("clustered_value");
    }

    @Test
    public void should_merge_and_find() throws Exception
    {
        compoundKey = new ClusteredKey(RandomUtils.nextLong(), Type.AUDIO);

        entity = new ClusteredEntityWithEnumCompoundKey(compoundKey, "clustered_value");

        em.merge(entity);

        ClusteredEntityWithEnumCompoundKey found = em.find(ClusteredEntityWithEnumCompoundKey.class,
                compoundKey);

        assertThat(found.getId()).isEqualTo(compoundKey);
        assertThat(found.getValue()).isEqualTo("clustered_value");
    }

    @Test
    public void should_merge_modifications() throws Exception
    {

        compoundKey = new ClusteredKey(RandomUtils.nextLong(), Type.FILE);

        entity = new ClusteredEntityWithEnumCompoundKey(compoundKey, "clustered_value");

        entity = em.merge(entity);

        entity.setValue("new_clustered_value");
        em.merge(entity);

        entity = em.find(ClusteredEntityWithEnumCompoundKey.class, compoundKey);

        assertThat(entity.getValue()).isEqualTo("new_clustered_value");
    }

    @Test
    public void should_remove() throws Exception
    {
        compoundKey = new ClusteredKey(RandomUtils.nextLong(), Type.IMAGE);

        entity = new ClusteredEntityWithEnumCompoundKey(compoundKey, "clustered_value");

        entity = em.merge(entity);

        em.remove(entity);

        assertThat(em.find(ClusteredEntityWithEnumCompoundKey.class, compoundKey)).isNull();

    }

    @Test
    public void should_refresh() throws Exception
    {

        long partitionKey = RandomUtils.nextLong();
        compoundKey = new ClusteredKey(partitionKey, Type.FILE);

        entity = new ClusteredEntityWithEnumCompoundKey(compoundKey, "clustered_value");

        entity = em.merge(entity);

        session.execute("UPDATE clustered_with_enum_compound set value='new_clustered_value' where id="
                + partitionKey + " and type = 'FILE'");

        em.refresh(entity);

        assertThat(entity.getValue()).isEqualTo("new_clustered_value");
    }

    @After
    public void tearDown()
    {
        truncateTable("clustered_with_enum_compound");
    }
}
