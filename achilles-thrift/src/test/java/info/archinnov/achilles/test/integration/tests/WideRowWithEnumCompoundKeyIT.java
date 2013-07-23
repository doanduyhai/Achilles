package info.archinnov.achilles.test.integration.tests;

import static info.archinnov.achilles.common.ThriftCassandraDaoTest.getColumnFamilyDao;
import static info.archinnov.achilles.table.TableNameNormalizer.normalizerAndValidateColumnFamilyName;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.common.ThriftCassandraDaoTest;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.manager.ThriftEntityManager;
import info.archinnov.achilles.serializer.ThriftSerializerUtils;
import info.archinnov.achilles.test.integration.entity.ClusteredEntityWithEnumCompoundKey;
import info.archinnov.achilles.test.integration.entity.ClusteredEntityWithEnumCompoundKey.ClusteredKey;
import info.archinnov.achilles.test.integration.entity.ClusteredEntityWithEnumCompoundKey.Type;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.mutation.Mutator;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.After;
import org.junit.Test;
import com.google.common.base.Optional;

public class WideRowWithEnumCompoundKeyIT
{

    private ThriftGenericWideRowDao dao = getColumnFamilyDao(
            normalizerAndValidateColumnFamilyName("clustered_with_enum_compound"),
            Long.class,
            String.class);

    private ThriftEntityManager em = ThriftCassandraDaoTest.getEm();

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

        Composite comp = new Composite();
        comp.setComponent(0, "FILE", ThriftSerializerUtils.STRING_SRZ);
        Mutator<Long> mutator = dao.buildMutator();
        dao.insertColumnBatch(partitionKey, comp, "new_clustered_value",
                Optional.<Integer> absent(), mutator);
        dao.executeMutator(mutator);

        em.refresh(entity);

        assertThat(entity.getValue()).isEqualTo("new_clustered_value");
    }

    @After
    public void tearDown()
    {
        dao.truncate();
    }
}
