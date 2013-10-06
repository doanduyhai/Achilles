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
package info.archinnov.achilles.entity.manager;

import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.context.ConfigurationContext;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CQLPersistenceManagerFactoryTest {

	@Mock(answer = Answers.CALLS_REAL_METHODS)
	private CQLPersistenceManagerFactory pmf;

	@Mock
	private ConfigurationContext configContext;

	@Mock
	private AchillesConsistencyLevelPolicy policy;

	@Test
	public void should_create_entity_manager() throws Exception {
		when(configContext.getConsistencyPolicy()).thenReturn(policy);
		pmf.setConfigContext(configContext);
		CQLPersistenceManager manager = pmf.createPersistenceManager();
		assertThat(manager).isNotNull();
	}
}
