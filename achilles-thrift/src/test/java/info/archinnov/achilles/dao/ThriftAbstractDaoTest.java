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
package info.archinnov.achilles.dao;

import static info.archinnov.achilles.entity.metadata.PropertyType.SIMPLE;
import static org.mockito.Mockito.verify;
import info.archinnov.achilles.consistency.ThriftConsistencyLevelPolicy;
import info.archinnov.achilles.serializer.ThriftSerializerUtils;
import info.archinnov.achilles.test.integration.AchillesInternalThriftResource;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.Composite;

import org.apache.cassandra.utils.Pair;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.internal.util.reflection.Whitebox;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ThriftAbstractDaoTest {

	@Rule
	public AchillesInternalThriftResource resource = new AchillesInternalThriftResource("CompleteBean");

	private ThriftGenericEntityDao abstractDao;

	@Mock
	private Cluster cluster = resource.getCluster();

	private Keyspace keyspace = resource.getKeyspace();

	@Mock
	private ThriftConsistencyLevelPolicy policy;

	private String columnFamily = "CompleteBean";

	@Before
	public void setUp() {
		abstractDao = new ThriftGenericEntityDao(cluster, keyspace, columnFamily, policy, Pair.create(Long.class,
				String.class));
	}

	@Test
	public void should_reinit_consistency_level() throws Exception {

		Composite composite = new Composite();
		composite.setComponent(0, SIMPLE.flag(), ThriftSerializerUtils.BYTE_SRZ);
		composite.setComponent(1, "name", ThriftSerializerUtils.STRING_SRZ);
		abstractDao.getValue(123L, composite);
		verify(policy).loadConsistencyLevelForRead(columnFamily);
		verify(policy).reinitDefaultConsistencyLevels();
	}

	@Test
	public void should_reinit_consistency_level_after_exception() throws Exception {
		Whitebox.setInternalState(abstractDao, "columnFamily", "xxx");
		try {
			Composite composite = new Composite();
			composite.setComponent(0, SIMPLE.flag(), ThriftSerializerUtils.BYTE_SRZ);
			composite.setComponent(1, "name", ThriftSerializerUtils.STRING_SRZ);
			abstractDao.getValue(123L, composite);
		} catch (RuntimeException e) {
			verify(policy).loadConsistencyLevelForRead("xxx");
			verify(policy).reinitDefaultConsistencyLevels();
		}

	}
}
