package info.archinnov.achilles.dao;

import static info.archinnov.achilles.dao.AchillesConfigurableConsistencyLevelPolicy.defaultReadConsistencyLevelTL;
import static info.archinnov.achilles.dao.AchillesConfigurableConsistencyLevelPolicy.defaultWriteConsistencyLevelTL;
import static info.archinnov.achilles.entity.manager.ThriftEntityManager.currentReadConsistencyLevel;
import static info.archinnov.achilles.entity.manager.ThriftEntityManager.currentWriteConsistencyLevel;
import static me.prettyprint.cassandra.service.OperationType.META_READ;
import static me.prettyprint.cassandra.service.OperationType.META_WRITE;
import static me.prettyprint.cassandra.service.OperationType.READ;
import static me.prettyprint.cassandra.service.OperationType.WRITE;
import static me.prettyprint.hector.api.HConsistencyLevel.ANY;
import static me.prettyprint.hector.api.HConsistencyLevel.EACH_QUORUM;
import static me.prettyprint.hector.api.HConsistencyLevel.LOCAL_QUORUM;
import static me.prettyprint.hector.api.HConsistencyLevel.QUORUM;
import static me.prettyprint.hector.api.HConsistencyLevel.THREE;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.entity.type.ConsistencyLevel;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

/**
 * AchillesConfigurableConsistencyLevelPolicyTest
 * 
 * @author DuyHai DOAN
 * 
 */
public class AchillesConfigurableConsistencyLevelPolicyTest
{

	private AchillesConfigurableConsistencyLevelPolicy policy = new AchillesConfigurableConsistencyLevelPolicy();

	@Before
	public void setUp()
	{
		defaultReadConsistencyLevelTL.remove();
		defaultWriteConsistencyLevelTL.remove();
	}

	@Test
	public void should_get_default_consistency_level_for_read_and_write() throws Exception
	{
		assertThat(policy.get(READ)).isEqualTo(QUORUM);
		assertThat(policy.get(WRITE)).isEqualTo(QUORUM);
	}

	@Test
	public void should_get_consistency_level_for_read_and_write_from_thread_local()
			throws Exception
	{
		defaultReadConsistencyLevelTL.set(LOCAL_QUORUM);
		defaultWriteConsistencyLevelTL.set(ANY);

		assertThat(policy.get(READ)).isEqualTo(LOCAL_QUORUM);
		assertThat(policy.get(WRITE)).isEqualTo(ANY);
	}

	@Test
	public void should_get_consistency_level_for_meta_read_and_write_from_default()
			throws Exception
	{
		defaultReadConsistencyLevelTL.set(LOCAL_QUORUM);
		defaultWriteConsistencyLevelTL.set(ANY);

		assertThat(policy.get(META_READ)).isEqualTo(QUORUM);
		assertThat(policy.get(META_WRITE)).isEqualTo(QUORUM);
	}

	@Test
	public void should_get_default_consistency_level_for_read_and_write_and_cf() throws Exception
	{
		policy.setConsistencyLevelForRead(QUORUM, "cf");
		policy.setConsistencyLevelForWrite(THREE, "cf");
		assertThat(policy.get(READ, "cf")).isEqualTo(QUORUM);
		assertThat(policy.get(WRITE, "cf")).isEqualTo(THREE);
	}

	@Test
	public void should_get_consistency_level_for_read_and_write_from_thread_local_and_cf()
			throws Exception
	{

		defaultReadConsistencyLevelTL.set(LOCAL_QUORUM);
		defaultWriteConsistencyLevelTL.set(ANY);

		assertThat(policy.get(READ, "cf")).isEqualTo(LOCAL_QUORUM);
		assertThat(policy.get(WRITE, "cf")).isEqualTo(ANY);
	}

	@Test
	public void should_get_consistency_level_for_meta_read_and_write_from_default_and_cf()
			throws Exception
	{
		defaultReadConsistencyLevelTL.set(LOCAL_QUORUM);
		defaultWriteConsistencyLevelTL.set(ANY);

		assertThat(policy.get(META_READ, "cf")).isEqualTo(QUORUM);
		assertThat(policy.get(META_WRITE, "cf")).isEqualTo(QUORUM);
	}

	@Test
	public void should_load_consistency_for_read_and_write() throws Exception
	{
		policy.setConsistencyLevelForRead(QUORUM, "cf1");
		policy.setConsistencyLevelForWrite(THREE, "cf1");

		policy.loadConsistencyLevelForRead("cf1");
		policy.loadConsistencyLevelForWrite("cf1");

		assertThat(defaultReadConsistencyLevelTL.get()).isEqualTo(QUORUM);
		assertThat(defaultWriteConsistencyLevelTL.get()).isEqualTo(THREE);
	}

	@Test
	public void should_load_current_consistency_level_for_read_and_write() throws Exception
	{
		currentReadConsistencyLevel.set(ConsistencyLevel.EACH_QUORUM);
		currentWriteConsistencyLevel.set(ConsistencyLevel.LOCAL_QUORUM);

		policy.setConsistencyLevelForRead(QUORUM, "cf2");
		policy.setConsistencyLevelForWrite(THREE, "cf2");

		policy.loadConsistencyLevelForRead("cf2");
		policy.loadConsistencyLevelForWrite("cf2");

		assertThat(defaultReadConsistencyLevelTL.get()).isEqualTo(EACH_QUORUM);
		assertThat(defaultWriteConsistencyLevelTL.get()).isEqualTo(LOCAL_QUORUM);

	}

	@Test
	public void should_reinit_consistency_level_for_read_and_write() throws Exception
	{
		policy.setConsistencyLevelForRead(QUORUM, "cf3");
		policy.setConsistencyLevelForWrite(THREE, "cf3");

		policy.reinitDefaultConsistencyLevel();

		assertThat(defaultReadConsistencyLevelTL.get()).isNull();
		assertThat(defaultWriteConsistencyLevelTL.get()).isNull();
	}

	@Test
	public void should_get_consistency_for_read_and_write_from_map() throws Exception
	{
		policy.setConsistencyLevelForRead(QUORUM, "cf4");
		policy.setConsistencyLevelForWrite(THREE, "cf4");

		assertThat(policy.getConsistencyLevelForRead("cf4")).isEqualTo(QUORUM);
		assertThat(policy.getConsistencyLevelForWrite("cf4")).isEqualTo(THREE);
	}

	@AfterClass
	public static void cleanThreadLocals()
	{
		defaultReadConsistencyLevelTL.remove();
		defaultWriteConsistencyLevelTL.remove();
		currentReadConsistencyLevel.remove();
		currentWriteConsistencyLevel.remove();
	}
}
