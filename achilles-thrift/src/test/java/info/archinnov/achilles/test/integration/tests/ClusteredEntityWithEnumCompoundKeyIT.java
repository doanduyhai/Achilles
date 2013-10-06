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
package info.archinnov.achilles.test.integration.tests;

import static info.archinnov.achilles.table.TableNameNormalizer.normalizerAndValidateColumnFamilyName;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.manager.ThriftPersistenceManager;
import info.archinnov.achilles.junit.AchillesInternalThriftResource;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.serializer.ThriftSerializerUtils;
import info.archinnov.achilles.test.integration.entity.ClusteredEntityWithEnumCompoundKey;
import info.archinnov.achilles.test.integration.entity.ClusteredEntityWithEnumCompoundKey.ClusteredKey;
import info.archinnov.achilles.test.integration.entity.ClusteredEntityWithEnumCompoundKey.Type;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.mutation.Mutator;

import org.apache.commons.lang.math.RandomUtils;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.base.Optional;

public class ClusteredEntityWithEnumCompoundKeyIT {

	@Rule
	public AchillesInternalThriftResource resource = new AchillesInternalThriftResource(Steps.AFTER_TEST,
			"clustered_with_enum_compound");

	private ThriftPersistenceManager manager = resource.getPersistenceManager();

	private ThriftGenericWideRowDao dao = resource.getColumnFamilyDao(
			normalizerAndValidateColumnFamilyName("clustered_with_enum_compound"), Long.class, String.class);

	private ClusteredEntityWithEnumCompoundKey entity;

	private ClusteredKey compoundKey;

	@Test
	public void should_persist_and_get_reference() throws Exception {
		compoundKey = new ClusteredKey(RandomUtils.nextLong(), Type.AUDIO);

		entity = new ClusteredEntityWithEnumCompoundKey(compoundKey, "clustered_value");

		manager.persist(entity);

		ClusteredEntityWithEnumCompoundKey found = manager.getReference(ClusteredEntityWithEnumCompoundKey.class,
				compoundKey);

		assertThat(found.getId()).isEqualTo(compoundKey);
		assertThat(found.getValue()).isEqualTo("clustered_value");
	}

	@Test
	public void should_merge_and_find() throws Exception {
		compoundKey = new ClusteredKey(RandomUtils.nextLong(), Type.AUDIO);

		entity = new ClusteredEntityWithEnumCompoundKey(compoundKey, "clustered_value");

		manager.merge(entity);

		ClusteredEntityWithEnumCompoundKey found = manager.find(ClusteredEntityWithEnumCompoundKey.class, compoundKey);

		assertThat(found.getId()).isEqualTo(compoundKey);
		assertThat(found.getValue()).isEqualTo("clustered_value");
	}

	@Test
	public void should_merge_modifications() throws Exception {

		compoundKey = new ClusteredKey(RandomUtils.nextLong(), Type.FILE);

		entity = new ClusteredEntityWithEnumCompoundKey(compoundKey, "clustered_value");

		entity = manager.merge(entity);

		entity.setValue("new_clustered_value");
		manager.merge(entity);

		entity = manager.find(ClusteredEntityWithEnumCompoundKey.class, compoundKey);

		assertThat(entity.getValue()).isEqualTo("new_clustered_value");
	}

	@Test
	public void should_remove() throws Exception {
		compoundKey = new ClusteredKey(RandomUtils.nextLong(), Type.IMAGE);

		entity = new ClusteredEntityWithEnumCompoundKey(compoundKey, "clustered_value");

		entity = manager.merge(entity);

		manager.remove(entity);

		assertThat(manager.find(ClusteredEntityWithEnumCompoundKey.class, compoundKey)).isNull();

	}

	@Test
	public void should_refresh() throws Exception {

		long partitionKey = RandomUtils.nextLong();
		compoundKey = new ClusteredKey(partitionKey, Type.FILE);

		entity = new ClusteredEntityWithEnumCompoundKey(compoundKey, "clustered_value");

		entity = manager.merge(entity);

		Composite comp = new Composite();
		comp.setComponent(0, "FILE", ThriftSerializerUtils.STRING_SRZ);
		Mutator<Long> mutator = dao.buildMutator();
		dao.insertColumnBatch(partitionKey, comp, "new_clustered_value", Optional.<Integer> absent(),
				Optional.<Long> absent(), mutator);
		dao.executeMutator(mutator);

		manager.refresh(entity);

		assertThat(entity.getValue()).isEqualTo("new_clustered_value");
	}
}
