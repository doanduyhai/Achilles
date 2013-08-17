package info.archinnov.achilles.test.integration.tests;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.entity.manager.CQLEntityManager;
import info.archinnov.achilles.junit.AchillesInternalCQLResource;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.test.integration.entity.CompleteBean;
import info.archinnov.achilles.test.integration.entity.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.integration.entity.EntityWithLocalQuorumConsistency;
import info.archinnov.achilles.test.integration.entity.EntityWithWriteOneAndReadLocalQuorumConsistency;
import info.archinnov.achilles.test.integration.utils.CassandraLogAsserter;
import info.archinnov.achilles.type.ConsistencyLevel;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.UnavailableException;

/**
 * ConsistencyLevelIT
 * 
 * @author DuyHai DOAN
 * 
 */
public class ConsistencyLevelIT
{

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Rule
    public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(Steps.AFTER_TEST, "CompleteBean",
            "consistency_test1", "consistency_test2");

    private CQLEntityManager em = resource.getEm();

    private CassandraLogAsserter logAsserter = new CassandraLogAsserter();

    private Long id = RandomUtils.nextLong();

    @Test
    public void should_throw_exception_when_persisting_with_local_quorum_consistency()
            throws Exception
    {
        EntityWithLocalQuorumConsistency bean = new EntityWithLocalQuorumConsistency();
        bean.setId(id);
        bean.setName("name");

        expectedEx.expect(InvalidQueryException.class);
        expectedEx
                .expectMessage("consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)");

        em.persist(bean);
    }

    @Test
    public void should_throw_exception_when_loading_entity_with_local_quorum_consistency()
            throws Exception
    {
        EntityWithWriteOneAndReadLocalQuorumConsistency bean = new EntityWithWriteOneAndReadLocalQuorumConsistency(
                id, "FN", "LN");

        em.persist(bean);

        expectedEx.expect(InvalidQueryException.class);
        expectedEx
                .expectMessage("consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)");

        em.find(EntityWithWriteOneAndReadLocalQuorumConsistency.class, id);
    }

    @Test
    public void should_recover_from_exception_and_reinit_consistency_level() throws Exception
    {
        EntityWithWriteOneAndReadLocalQuorumConsistency bean = new EntityWithWriteOneAndReadLocalQuorumConsistency(
                id, "FN", "LN");

        try
        {
            em.persist(bean);
            em.find(EntityWithWriteOneAndReadLocalQuorumConsistency.class, id);
        } catch (InvalidQueryException e)
        {
            // Should recover from exception
        }
        CompleteBean newBean = new CompleteBean();
        newBean.setId(id);
        newBean.setName("name");

        em.persist(newBean);

        newBean = em.find(CompleteBean.class, newBean.getId());

        assertThat(newBean).isNotNull();
        assertThat(newBean.getName()).isEqualTo("name");
    }

    @Test
    public void should_persist_with_runtime_consistency_level_overriding_predefined_one()
            throws Exception
    {
        CompleteBean entity = CompleteBeanTestBuilder
                .builder()
                .randomId()
                .name("name zerferg")
                .buid();

        try
        {
            em.persist(entity, ConsistencyLevel.EACH_QUORUM);
        } catch (InvalidQueryException e)
        {
            assertThat(e)
                    .hasMessage(
                            "consistency level EACH_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)");
        }

        logAsserter.prepareLogLevel();
        em.persist(entity, ConsistencyLevel.ALL);
        CompleteBean found = em.find(CompleteBean.class, entity.getId());
        assertThat(found.getName()).isEqualTo("name zerferg");
        logAsserter.assertConsistencyLevels(ConsistencyLevel.ONE, ConsistencyLevel.ALL);
    }

    @Test
    public void should_merge_with_runtime_consistency_level_overriding_predefined_one()
            throws Exception
    {
        CompleteBean entity = CompleteBeanTestBuilder
                .builder()
                .randomId()
                .name("name zeruioze")
                .buid();

        try
        {
            em.merge(entity, ConsistencyLevel.EACH_QUORUM);
        } catch (InvalidQueryException e)
        {
            assertThat(e)
                    .hasMessage(
                            "consistency level EACH_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)");
        }

        logAsserter.prepareLogLevel();
        em.merge(entity, ConsistencyLevel.ALL);
        CompleteBean found = em.find(CompleteBean.class, entity.getId());
        assertThat(found.getName()).isEqualTo("name zeruioze");
        logAsserter.assertConsistencyLevels(ConsistencyLevel.ONE, ConsistencyLevel.ALL);
    }

    @Test
    public void should_find_with_runtime_consistency_level_overriding_predefined_one()
            throws Exception
    {
        CompleteBean entity = CompleteBeanTestBuilder
                .builder()
                .randomId()
                .name("name rtprt")
                .buid();
        em.persist(entity);

        try
        {
            em.find(CompleteBean.class, entity.getId(), ConsistencyLevel.EACH_QUORUM);
        } catch (InvalidQueryException e)
        {
            assertThat(e)
                    .hasMessage(
                            "EACH_QUORUM ConsistencyLevel is only supported for writes");
        }
        logAsserter.prepareLogLevel();
        CompleteBean found = em.find(CompleteBean.class, entity.getId(), ConsistencyLevel.ALL);
        assertThat(found.getName()).isEqualTo("name rtprt");
        logAsserter.assertConsistencyLevels(ConsistencyLevel.ALL, ConsistencyLevel.QUORUM);
    }

    @Test
    public void should_refresh_with_runtime_consistency_level_overriding_predefined_one()
            throws Exception
    {
        CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("name").buid();
        entity = em.merge(entity);

        try
        {
            em.refresh(entity, ConsistencyLevel.EACH_QUORUM);
        } catch (InvalidQueryException e)
        {
            assertThat(e)
                    .hasMessage("EACH_QUORUM ConsistencyLevel is only supported for writes");
        }
        logAsserter.prepareLogLevel();
        em.refresh(entity, ConsistencyLevel.ALL);
        logAsserter.assertConsistencyLevels(ConsistencyLevel.ALL, ConsistencyLevel.QUORUM);
    }

    @Test
    public void should_remove_with_runtime_consistency_level_overriding_predefined_one()
            throws Exception
    {
        CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("name").buid();
        entity = em.merge(entity);

        try
        {
            em.remove(entity, ConsistencyLevel.EACH_QUORUM);
        } catch (InvalidQueryException e)
        {
            assertThat(e)
                    .hasMessage(
                            "consistency level EACH_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)");
        }
        logAsserter.prepareLogLevel();
        em.remove(entity, ConsistencyLevel.ALL);
        assertThat(em.find(CompleteBean.class, entity.getId())).isNull();
        logAsserter.assertConsistencyLevels(ConsistencyLevel.ONE, ConsistencyLevel.ALL);
    }

    @Test
    public void should_reinit_consistency_level_after_exception() throws Exception
    {
        CompleteBean entity = CompleteBeanTestBuilder
                .builder()
                .randomId()
                .name("name qzerferf")
                .buid();
        try
        {
            em.merge(entity, ConsistencyLevel.EACH_QUORUM);
        } catch (InvalidQueryException e)
        {
            assertThat(e)
                    .hasMessage(
                            "consistency level EACH_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)");
        }
        logAsserter.prepareLogLevel();
        em.merge(entity, ConsistencyLevel.ALL);
        CompleteBean found = em.find(CompleteBean.class, entity.getId());
        assertThat(found.getName()).isEqualTo("name qzerferf");
        logAsserter.assertConsistencyLevels(ConsistencyLevel.ONE, ConsistencyLevel.ALL);
    }

    //    @Test
    //    public void should_batch_with_runtime_consistency_level() throws Exception
    //    {
    //        CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("name").buid();
    //        Tweet tweet = TweetTestBuilder.tweet().randomId().content("test_tweet").buid();
    //
    //        logAsserter.prepareLogLevel();
    //        ThriftBatchingEntityManager batchingEm = em.batchingEntityManager();
    //        batchingEm.startBatch(ONE, QUORUM);
    //        batchingEm.persist(entity);
    //        batchingEm.persist(tweet);
    //
    //        batchingEm.endBatch();
    //        logAsserter.assertConsistencyLevels(ONE, QUORUM);
    //    }

    @Test
    public void should_get_counter_with_consistency_level() throws Exception
    {
        CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("name").buid();
        entity = em.merge(entity);
        try
        {
            entity.getVersion().get(ConsistencyLevel.EACH_QUORUM);
        } catch (InvalidQueryException e)
        {
            assertThat(e)
                    .hasMessage("EACH_QUORUM ConsistencyLevel is only supported for writes");
        }
    }

    @Test
    public void should_increment_counter_with_consistency_level() throws Exception
    {
        CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("name").buid();
        entity = em.merge(entity);
        try
        {
            entity.getVersion().incr(ConsistencyLevel.THREE);
        } catch (UnavailableException e)
        {
            assertThat(e)
                    .hasMessage(
                            "Not enough replica available for query at consistency THREE (3 required but only 1 alive)");
        }
    }

    @Test
    public void should_increment_n_counter_with_consistency_level() throws Exception
    {
        CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("name").buid();
        entity = em.merge(entity);
        try
        {
            entity.getVersion().incr(10L, ConsistencyLevel.THREE);
        } catch (UnavailableException e)
        {
            assertThat(e)
                    .hasMessage(
                            "Not enough replica available for query at consistency THREE (3 required but only 1 alive)");
        }
    }

    @Test
    public void should_decrement_counter_with_consistency_level() throws Exception
    {
        CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("name").buid();
        entity = em.merge(entity);
        try
        {
            entity.getVersion().decr(ConsistencyLevel.THREE);
        } catch (UnavailableException e)
        {
            assertThat(e)
                    .hasMessage(
                            "Not enough replica available for query at consistency THREE (3 required but only 1 alive)");
        }
    }

    @Test
    public void should_decrement_counter_n_with_consistency_level() throws Exception
    {
        CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("name").buid();
        entity = em.merge(entity);
        try
        {
            entity.getVersion().decr(10L, ConsistencyLevel.THREE);
        } catch (UnavailableException e)
        {
            assertThat(e)
                    .hasMessage(
                            "Not enough replica available for query at consistency THREE (3 required but only 1 alive)");
        }
    }

}
