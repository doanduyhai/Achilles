/**
 *
 * Copyright (C) 2012-2013 DuyHai DOAN
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package info.archinnov.achilles.consistency;

import static info.archinnov.achilles.consistency.ThriftConsistencyLevelPolicy.*;
import static me.prettyprint.cassandra.service.OperationType.*;
import static me.prettyprint.hector.api.HConsistencyLevel.*;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.type.ConsistencyLevel;

import java.util.HashMap;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

public class ThriftConsistencyLevelPolicyTest {

	private ThriftConsistencyLevelPolicy policy = new ThriftConsistencyLevelPolicy(ConsistencyLevel.ONE,
			ConsistencyLevel.ONE, new HashMap<String, ConsistencyLevel>(), new HashMap<String, ConsistencyLevel>());

	@Before
	public void setUp() {
		defaultReadConsistencyLevelTL.remove();
		defaultWriteConsistencyLevelTL.remove();
		currentReadConsistencyLevel.remove();
		currentWriteConsistencyLevel.remove();

	}

	@Test
	public void should_get_default_consistency_level_for_read_and_write() throws Exception {
		assertThat(policy.get(READ)).isEqualTo(ONE);
		assertThat(policy.get(WRITE)).isEqualTo(ONE);
	}

	@Test
	public void should_get_consistency_level_for_read_and_write_from_thread_local() throws Exception {
		defaultReadConsistencyLevelTL.set(LOCAL_QUORUM);
		defaultWriteConsistencyLevelTL.set(ANY);

		assertThat(policy.get(READ)).isEqualTo(LOCAL_QUORUM);
		assertThat(policy.get(WRITE)).isEqualTo(ANY);
	}

	@Test
	public void should_get_consistency_level_for_meta_read_and_write_from_default() throws Exception {
		defaultReadConsistencyLevelTL.set(LOCAL_QUORUM);
		defaultWriteConsistencyLevelTL.set(ANY);

		assertThat(policy.get(META_READ)).isEqualTo(ONE);
		assertThat(policy.get(META_WRITE)).isEqualTo(ONE);
	}

	@Test
	public void should_get_consistency_level_for_read_and_write_from_thread_local_and_cf() throws Exception {

		defaultReadConsistencyLevelTL.set(LOCAL_QUORUM);
		defaultWriteConsistencyLevelTL.set(ANY);

		assertThat(policy.get(READ, "cf")).isEqualTo(LOCAL_QUORUM);
		assertThat(policy.get(WRITE, "cf")).isEqualTo(ANY);
	}

	@Test
	public void should_get_consistency_level_for_meta_read_and_write_from_default_and_cf() throws Exception {
		defaultReadConsistencyLevelTL.set(LOCAL_QUORUM);
		defaultWriteConsistencyLevelTL.set(ANY);

		assertThat(policy.get(META_READ, "cf")).isEqualTo(ONE);
		assertThat(policy.get(META_WRITE, "cf")).isEqualTo(ONE);
	}

	@Test
	public void should_load_consistency_for_read_and_write() throws Exception {
		policy.setConsistencyLevelForRead(ConsistencyLevel.QUORUM, "cf1");
		policy.setConsistencyLevelForWrite(ConsistencyLevel.THREE, "cf1");

		policy.loadConsistencyLevelForRead("cf1");
		policy.loadConsistencyLevelForWrite("cf1");

		assertThat(defaultReadConsistencyLevelTL.get()).isEqualTo(QUORUM);
		assertThat(defaultWriteConsistencyLevelTL.get()).isEqualTo(THREE);
	}

	@Test
	public void should_load_current_consistency_level_for_read_and_write() throws Exception {
		currentReadConsistencyLevel.set(ConsistencyLevel.EACH_QUORUM);
		currentWriteConsistencyLevel.set(ConsistencyLevel.LOCAL_QUORUM);

		policy.setConsistencyLevelForRead(ConsistencyLevel.QUORUM, "cf2");
		policy.setConsistencyLevelForWrite(ConsistencyLevel.THREE, "cf2");

		policy.loadConsistencyLevelForRead("cf2");
		policy.loadConsistencyLevelForWrite("cf2");

		assertThat(defaultReadConsistencyLevelTL.get()).isEqualTo(EACH_QUORUM);
		assertThat(defaultWriteConsistencyLevelTL.get()).isEqualTo(LOCAL_QUORUM);

	}

	@Test
	public void should_reinit_consistency_level_for_read_and_write() throws Exception {
		policy.setConsistencyLevelForRead(ConsistencyLevel.QUORUM, "cf3");
		policy.setConsistencyLevelForWrite(ConsistencyLevel.THREE, "cf3");

		policy.reinitDefaultConsistencyLevels();

		assertThat(defaultReadConsistencyLevelTL.get()).isNull();
		assertThat(defaultWriteConsistencyLevelTL.get()).isNull();
	}

	@Test
	public void should_get_current_read_level() throws Exception {
		currentReadConsistencyLevel.set(ConsistencyLevel.LOCAL_QUORUM);
		assertThat(policy.getCurrentReadLevel()).isEqualTo(ConsistencyLevel.LOCAL_QUORUM);
	}

	@Test
	public void should_set_current_read_level() throws Exception {
		policy.setCurrentReadLevel(ConsistencyLevel.LOCAL_QUORUM);
		assertThat(currentReadConsistencyLevel.get()).isEqualTo(ConsistencyLevel.LOCAL_QUORUM);
	}

	@AfterClass
	public static void cleanThreadLocals() {
		defaultReadConsistencyLevelTL.remove();
		defaultWriteConsistencyLevelTL.remove();
		currentReadConsistencyLevel.remove();
		currentWriteConsistencyLevel.remove();
	}
}
