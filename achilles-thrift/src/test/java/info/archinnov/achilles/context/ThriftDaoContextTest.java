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
package info.archinnov.achilles.context;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.dao.ThriftCounterDao;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ThriftDaoContextTest {
	private ThriftDaoContext context;

	private Map<String, ThriftGenericEntityDao> entityDaosMap = new HashMap<String, ThriftGenericEntityDao>();
	private Map<String, ThriftGenericWideRowDao> columnFamilyDaosMap = new HashMap<String, ThriftGenericWideRowDao>();

	@Mock
	private ThriftGenericEntityDao entityDao;

	@Mock
	private ThriftGenericWideRowDao columnFamilyDao;

	@Mock
	private ThriftCounterDao thriftCounterDao;

	@Before
	public void setUp() {
		context = new ThriftDaoContext(entityDaosMap, columnFamilyDaosMap,
				thriftCounterDao);
	}

	@Test
	public void should_get_counter_dao() throws Exception {
		assertThat(context.getCounterDao()).isSameAs(thriftCounterDao);
	}

	@Test
	public void should_get_entity_dao() throws Exception {
		entityDaosMap.put("dao", entityDao);
		assertThat((ThriftGenericEntityDao) context.findEntityDao("dao"))
				.isSameAs(entityDao);
	}

	@Test
	public void should_get_wide_row_dao() throws Exception {
		columnFamilyDaosMap.put("dao", columnFamilyDao);
		assertThat((ThriftGenericWideRowDao) context.findWideRowDao("dao"))
				.isSameAs(columnFamilyDao);
	}
}
