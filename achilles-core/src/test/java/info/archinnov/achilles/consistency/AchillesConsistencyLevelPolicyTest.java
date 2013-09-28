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

import static info.archinnov.achilles.type.ConsistencyLevel.*;
import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.type.ConsistencyLevel;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AchillesConsistencyLevelPolicyTest {

	@Mock
	private AchillesConsistencyLevelPolicy policy;

	private Map<String, ConsistencyLevel> readCfConsistencyLevels = new HashMap<String, ConsistencyLevel>();
	private Map<String, ConsistencyLevel> writeCfConsistencyLevels = new HashMap<String, ConsistencyLevel>();

	@Before
	public void setUp() {
		doCallRealMethod().when(policy).setReadCfConsistencyLevels(
				Mockito.<Map<String, ConsistencyLevel>> any());
		doCallRealMethod().when(policy).setWriteCfConsistencyLevels(
				Mockito.<Map<String, ConsistencyLevel>> any());

		policy.setReadCfConsistencyLevels(readCfConsistencyLevels);
		policy.setWriteCfConsistencyLevels(writeCfConsistencyLevels);

		readCfConsistencyLevels.clear();
		writeCfConsistencyLevels.clear();

		doCallRealMethod().when(policy).getConsistencyLevelForRead(
				any(String.class));
		doCallRealMethod().when(policy).getConsistencyLevelForWrite(
				any(String.class));
	}

	@Test
	public void should_get_consistency_for_read_and_write_from_map()
			throws Exception {
		doCallRealMethod().when(policy).setConsistencyLevelForRead(
				any(ConsistencyLevel.class), any(String.class));
		doCallRealMethod().when(policy).setConsistencyLevelForWrite(
				any(ConsistencyLevel.class), any(String.class));

		policy.setConsistencyLevelForRead(ONE, "cf1");
		policy.setConsistencyLevelForWrite(TWO, "cf1");

		assertThat(policy.getConsistencyLevelForRead("cf1")).isEqualTo(ONE);
		assertThat(policy.getConsistencyLevelForWrite("cf1")).isEqualTo(TWO);
	}

	@Test
	public void should_get_consistency_for_read_and_write_from_default()
			throws Exception {
		doCallRealMethod().when(policy).setDefaultGlobalReadConsistencyLevel(
				any(ConsistencyLevel.class));
		doCallRealMethod().when(policy).setDefaultGlobalWriteConsistencyLevel(
				any(ConsistencyLevel.class));

		policy.setDefaultGlobalReadConsistencyLevel(THREE);
		policy.setDefaultGlobalWriteConsistencyLevel(QUORUM);

		assertThat(policy.getConsistencyLevelForRead("cf2")).isEqualTo(THREE);
		assertThat(policy.getConsistencyLevelForWrite("cf2")).isEqualTo(QUORUM);
	}
}
