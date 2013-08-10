package info.archinnov.achilles.test.integration.tests;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.common.CQLCassandraDaoTest;
import info.archinnov.achilles.entity.manager.CQLEntityManager;
import info.archinnov.achilles.test.integration.entity.ValuelessEntity;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.Test;

/**
 * ValuelessEntityIT
 * 
 * @author DuyHai DOAN
 * 
 */
public class ValuelessEntityIT {
    private CQLEntityManager em = CQLCassandraDaoTest.getEm();

    @Test
    public void should_persist_and_find() throws Exception
    {
        Long id = RandomUtils.nextLong();
        ValuelessEntity entity = new ValuelessEntity(id);

        em.persist(entity);

        ValuelessEntity found = em.find(ValuelessEntity.class, id);

        assertThat(found).isNotNull();
    }

    @Test
    public void should_merge_and_get_reference() throws Exception
    {
        Long id = RandomUtils.nextLong();
        ValuelessEntity entity = new ValuelessEntity(id);

        em.merge(entity);

        ValuelessEntity found = em.getReference(ValuelessEntity.class, id);

        assertThat(found).isNotNull();
    }

    @Test
    public void should_persist_with_ttl() throws Exception
    {
        Long id = RandomUtils.nextLong();
        ValuelessEntity entity = new ValuelessEntity(id);

        em.persist(entity, 2);

        Thread.sleep(3000);

        assertThat(em.find(ValuelessEntity.class, id)).isNull();
    }

    @Test
    public void should_merge_with_ttl() throws Exception
    {
        Long id = RandomUtils.nextLong();
        ValuelessEntity entity = new ValuelessEntity(id);

        em.merge(entity, 2);

        Thread.sleep(3000);

        assertThat(em.find(ValuelessEntity.class, id)).isNull();
    }
}
