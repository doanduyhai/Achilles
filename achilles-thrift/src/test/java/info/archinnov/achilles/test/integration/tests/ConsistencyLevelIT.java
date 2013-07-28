package info.archinnov.achilles.test.integration.tests;

import static info.archinnov.achilles.type.ConsistencyLevel.*;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.common.ThriftCassandraDaoTest;
import info.archinnov.achilles.consistency.ThriftConsistencyLevelPolicy;
import info.archinnov.achilles.entity.manager.ThriftBatchingEntityManager;
import info.archinnov.achilles.entity.manager.ThriftEntityManager;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.test.builders.TweetTestBuilder;
import info.archinnov.achilles.test.integration.entity.CompleteBean;
import info.archinnov.achilles.test.integration.entity.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.integration.entity.EntityWithLocalQuorumConsistency;
import info.archinnov.achilles.test.integration.entity.EntityWithWriteOneAndReadLocalQuorumConsistency;
import info.archinnov.achilles.test.integration.entity.Tweet;
import info.archinnov.achilles.test.integration.utils.CassandraLogAsserter;
import info.archinnov.achilles.type.ConsistencyLevel;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.exceptions.HInvalidRequestException;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

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

    private CassandraLogAsserter logAsserter = new CassandraLogAsserter();

    private ThriftEntityManager em = ThriftCassandraDaoTest.getEm();

    private Cluster cluster = ThriftCassandraDaoTest.getCluster();

    private String keyspaceName = ThriftCassandraDaoTest.getKeyspace().getKeyspaceName();

    private Long id = RandomUtils.nextLong();

    private ThriftConsistencyLevelPolicy policy = ThriftCassandraDaoTest.getConsistencyPolicy();

    @Test
    public void should_throw_exception_when_persisting_with_local_quorum_consistency()
            throws Exception
    {
        EntityWithLocalQuorumConsistency bean = new EntityWithLocalQuorumConsistency();
        bean.setId(id);
        bean.setName("name");

        expectedEx.expect(HInvalidRequestException.class);
        expectedEx
                .expectMessage("InvalidRequestException(why:consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy))");

        em.persist(bean);
        assertThatConsistencyLevelsAreReinitialized();
    }

    @Test
    public void should_throw_exception_when_loading_entity_with_local_quorum_consistency()
            throws Exception
    {
        EntityWithWriteOneAndReadLocalQuorumConsistency bean = new EntityWithWriteOneAndReadLocalQuorumConsistency(
                id, "FN", "LN");

        em.persist(bean);

        expectedEx.expect(AchillesException.class);
        expectedEx
                .expectMessage("Error when loading entity type '"
                        + EntityWithWriteOneAndReadLocalQuorumConsistency.class.getCanonicalName()
                        + "' with key '"
                        + id
                        + "'. Cause : InvalidRequestException(why:consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)");

        em.find(EntityWithWriteOneAndReadLocalQuorumConsistency.class, id);
        assertThatConsistencyLevelsAreReinitialized();
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
        } catch (AchillesException e)
        {
            // Should reinit consistency level to default
        }
        CompleteBean newBean = new CompleteBean();
        newBean.setId(id);
        newBean.setName("name");

        em.persist(newBean);

        newBean = em.find(CompleteBean.class, newBean.getId());

        assertThat(newBean).isNotNull();
        assertThat(newBean.getName()).isEqualTo("name");
        assertThatConsistencyLevelsAreReinitialized();
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
        } catch (HInvalidRequestException e)
        {
            assertThat(e)
                    .hasMessage(
                            "InvalidRequestException(why:consistency level EACH_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy))");
        }

        assertThatConsistencyLevelsAreReinitialized();

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
        } catch (HInvalidRequestException e)
        {
            assertThat(e)
                    .hasMessage(
                            "InvalidRequestException(why:consistency level EACH_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy))");
        }
        assertThatConsistencyLevelsAreReinitialized();

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
        } catch (AchillesException e)
        {
            assertThat(e)
                    .hasMessage(
                            "Error when loading entity type '"
                                    + CompleteBean.class.getCanonicalName()
                                    + "' with key '"
                                    + entity.getId()
                                    + "'. Cause : InvalidRequestException(why:EACH_QUORUM ConsistencyLevel is only supported for writes)");
        }
        assertThatConsistencyLevelsAreReinitialized();
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
        } catch (AchillesException e)
        {
            assertThat(e)
                    .hasMessage(
                            "Error when loading entity type '"
                                    + CompleteBean.class.getCanonicalName()
                                    + "' with key '"
                                    + entity.getId()
                                    + "'. Cause : InvalidRequestException(why:EACH_QUORUM ConsistencyLevel is only supported for writes)");
        }
        assertThatConsistencyLevelsAreReinitialized();
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
        } catch (HInvalidRequestException e)
        {
            assertThat(e)
                    .hasMessage(
                            "InvalidRequestException(why:consistency level EACH_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy))");
        }
        assertThatConsistencyLevelsAreReinitialized();
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
        } catch (HInvalidRequestException e)
        {
            assertThat(e)
                    .hasMessage(
                            "InvalidRequestException(why:consistency level EACH_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy))");
        }
        assertThatConsistencyLevelsAreReinitialized();
        logAsserter.prepareLogLevel();
        em.merge(entity, ConsistencyLevel.ALL);
        CompleteBean found = em.find(CompleteBean.class, entity.getId());
        assertThat(found.getName()).isEqualTo("name qzerferf");
        logAsserter.assertConsistencyLevels(ConsistencyLevel.ONE, ConsistencyLevel.ALL);
    }

    @Test
    public void should_batch_with_runtime_consistency_level() throws Exception
    {
        CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("name").buid();
        Tweet tweet = TweetTestBuilder.tweet().randomId().content("test_tweet").buid();

        logAsserter.prepareLogLevel();
        ThriftBatchingEntityManager batchingEm = em.batchingEntityManager();
        batchingEm.startBatch(ONE, QUORUM);
        batchingEm.persist(entity);
        batchingEm.persist(tweet);

        batchingEm.endBatch();
        logAsserter.assertConsistencyLevels(ONE, QUORUM);
    }

    @Test
    public void should_get_counter_with_consistency_level() throws Exception
    {
        CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("name").buid();
        entity = em.merge(entity);
        try
        {
            entity.getVersion().get(ConsistencyLevel.EACH_QUORUM);
        } catch (HInvalidRequestException e)
        {
            assertThat(e)
                    .hasMessage(
                            "InvalidRequestException(why:EACH_QUORUM ConsistencyLevel is only supported for writes)");
        }
        assertThatConsistencyLevelsAreReinitialized();
    }

    @Test
    public void should_increment_counter_with_consistency_level() throws Exception
    {
        CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("name").buid();
        entity = em.merge(entity);
        try
        {
            entity.getVersion().incr(ConsistencyLevel.EACH_QUORUM);
        } catch (HInvalidRequestException e)
        {
            assertThat(e)
                    .hasMessage(
                            "InvalidRequestException(why:consistency level EACH_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy))");
        }
        assertThatConsistencyLevelsAreReinitialized();
    }

    @Test
    public void should_increment_n_counter_with_consistency_level() throws Exception
    {
        CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("name").buid();
        entity = em.merge(entity);
        try
        {
            entity.getVersion().incr(10L, ConsistencyLevel.EACH_QUORUM);
        } catch (HInvalidRequestException e)
        {
            assertThat(e)
                    .hasMessage(
                            "InvalidRequestException(why:consistency level EACH_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy))");
        }
        assertThatConsistencyLevelsAreReinitialized();
    }

    @Test
    public void should_decrement_counter_with_consistency_level() throws Exception
    {
        CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("name").buid();
        entity = em.merge(entity);
        try
        {
            entity.getVersion().decr(ConsistencyLevel.EACH_QUORUM);
        } catch (HInvalidRequestException e)
        {
            assertThat(e)
                    .hasMessage(
                            "InvalidRequestException(why:consistency level EACH_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy))");
        }
        assertThatConsistencyLevelsAreReinitialized();
    }

    @Test
    public void should_decrement_counter_n_with_consistency_level() throws Exception
    {
        CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("name").buid();
        entity = em.merge(entity);
        try
        {
            entity.getVersion().decr(10L, ConsistencyLevel.EACH_QUORUM);
        } catch (HInvalidRequestException e)
        {
            assertThat(e)
                    .hasMessage(
                            "InvalidRequestException(why:consistency level EACH_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy))");
        }
        assertThatConsistencyLevelsAreReinitialized();
    }

    private void assertThatConsistencyLevelsAreReinitialized()
    {
        assertThat(policy.getCurrentReadLevel()).isNull();
        assertThat(policy.getCurrentWriteLevel()).isNull();
    }

    @After
    public void cleanThreadLocals()
    {
        policy.reinitCurrentConsistencyLevels();
        policy.reinitDefaultConsistencyLevels();
        cluster.truncate(keyspaceName, "CompleteBean");
        cluster.truncate(keyspaceName, "Tweet");
    }

    @AfterClass
    public static void cleanUp()
    {
        ThriftCassandraDaoTest.getConsistencyPolicy().reinitCurrentConsistencyLevels();
        ThriftCassandraDaoTest.getConsistencyPolicy().reinitDefaultConsistencyLevels();
    }
}
