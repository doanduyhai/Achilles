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
package info.archinnov.achilles.iterator;

import static info.archinnov.achilles.serializer.ThriftSerializerUtils.STRING_SRZ;
import static info.archinnov.achilles.type.ConsistencyLevel.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.consistency.ThriftConsistencyLevelPolicy;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.operations.ThriftJoinEntityLoader;
import info.archinnov.achilles.test.builders.CompositeTestBuilder;
import info.archinnov.achilles.test.builders.HColumnTestBuilder;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.UserBean;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

@RunWith(MockitoJUnitRunner.class)
public class ThriftJoinSliceIteratorTest {
	@Mock
	private PropertyMeta propertyMeta;

	@Mock
	private SliceQuery<Long, Composite, Object> query;

	@Mock
	private QueryResult<ColumnSlice<Composite, Object>> queryResult;

	@Mock
	private ColumnSlice<Composite, Object> columnSlice;

	@Mock
	private List<HColumn<Composite, Object>> hColumns;

	@Mock
	private Iterator<HColumn<Composite, Object>> columnsIterator;

	@Mock
	private ThriftJoinEntityLoader joinLoader;

	@Mock
	private ThriftGenericEntityDao joinEntityDao;

	private UserBean user1 = new UserBean();
	private UserBean user2 = new UserBean();
	private UserBean user3 = new UserBean();

	private EntityMeta joinEntityMeta = new EntityMeta();

	private ThriftJoinSliceIterator<Long, Integer, UserBean> iterator;

	@Mock
	private ThriftConsistencyLevelPolicy policy;

	private String columnFamily = "cf";

	@Before
	public void setUp() {
		when(query.execute()).thenReturn(queryResult);
		when(queryResult.get()).thenReturn(columnSlice);
		when(columnSlice.getColumns()).thenReturn(hColumns);
		when(hColumns.iterator()).thenReturn(columnsIterator);
		when((Class) propertyMeta.getValueClass()).thenReturn(UserBean.class);

		user1.setName("user1");
		user2.setName("user2");
		user3.setName("user3");

		when(propertyMeta.joinMeta()).thenReturn(joinEntityMeta);

		PropertyMeta joinIdMeta = new PropertyMeta();
		joinIdMeta.setValueClass(Long.class);
		when(propertyMeta.joinIdMeta()).thenReturn(joinIdMeta);
	}

	@Test
	public void should_return_3_entities() throws Exception {

		PropertyMeta idMeta = PropertyMetaTestBuilder //
				.valueClass(Long.class).type(PropertyType.SIMPLE).build();

		joinEntityMeta.setIdMeta(idMeta);
		when(propertyMeta.type()).thenReturn(PropertyType.JOIN_MAP);
		when(propertyMeta.isJoin()).thenReturn(true);
		when(propertyMeta.joinMeta()).thenReturn(joinEntityMeta);
		when(propertyMeta.joinIdMeta()).thenReturn(idMeta);

		Composite start = new Composite(), //
		end = new Composite(), //
		name1 = CompositeTestBuilder.builder().values("name1").buildSimple(), //
		name2 = CompositeTestBuilder.builder().values("name2").buildSimple(), //
		name3 = CompositeTestBuilder.builder().values("name3").buildSimple();

		Long joinId1 = 11L, joinId2 = 12L, joinId3 = 13L;
		Integer ttl = 10;

		HColumn<Composite, Object> hCol1 = HColumnTestBuilder.simple(name1,
				(Object) joinId1, ttl);
		HColumn<Composite, Object> hCol2 = HColumnTestBuilder.simple(name2,
				(Object) joinId2, ttl);
		HColumn<Composite, Object> hCol3 = HColumnTestBuilder.simple(name3,
				(Object) joinId3, ttl);

		Map<Object, Object> entitiesMap = new HashMap<Object, Object>();
		entitiesMap.put(joinId1, user1);
		entitiesMap.put(joinId2, user2);
		entitiesMap.put(joinId3, user3);

		when(policy.getCurrentReadLevel()).thenReturn(LOCAL_QUORUM, ONE);
		List<Object> keys = Arrays.<Object> asList(joinId1, joinId2, joinId3);
		when(
				joinLoader.loadJoinEntities(UserBean.class, keys,
						joinEntityMeta, joinEntityDao)).thenReturn(entitiesMap);

		iterator = new ThriftJoinSliceIterator<Long, Integer, UserBean>(policy,
				joinEntityDao, columnFamily, propertyMeta, query, start, end,
				false, 10);
		Whitebox.setInternalState(iterator, ThriftJoinEntityLoader.class,
				joinLoader);

		when(columnsIterator.next()).thenReturn(hCol1, hCol2, hCol3);
		when(columnsIterator.hasNext()).thenReturn(true, true, true, false);

		assertThat(iterator.hasNext()).isEqualTo(true);
		HColumn<Composite, UserBean> h1 = iterator.next();

		assertThat(h1.getName().get(0, STRING_SRZ)).isEqualTo("name1");
		assertThat(h1.getValue().getName()).isEqualTo(user1.getName());

		assertThat(iterator.hasNext()).isEqualTo(true);
		HColumn<Composite, UserBean> h2 = iterator.next();

		assertThat(h2.getName().get(0, STRING_SRZ)).isEqualTo("name2");
		assertThat(h2.getValue().getName()).isEqualTo(user2.getName());

		assertThat(iterator.hasNext()).isEqualTo(true);
		HColumn<Composite, UserBean> h3 = iterator.next();

		assertThat(h3.getName().get(0, STRING_SRZ)).isEqualTo("name3");
		assertThat(h3.getValue().getName()).isEqualTo(user3.getName());

		assertThat(iterator.hasNext()).isEqualTo(false);

		verify(policy, atLeastOnce()).setCurrentReadLevel(LOCAL_QUORUM);
		verify(policy, atLeastOnce()).setCurrentReadLevel(ONE);
		verify(policy).loadConsistencyLevelForRead(columnFamily);

	}

	@Test
	public void should_reload_load_when_reaching_end_of_batch()
			throws Exception {
		PropertyMeta idMeta = PropertyMetaTestBuilder //
				.valueClass(Long.class).type(PropertyType.SIMPLE).build();

		joinEntityMeta.setIdMeta(idMeta);
		when(propertyMeta.type()).thenReturn(PropertyType.JOIN_MAP);
		when(propertyMeta.isJoin()).thenReturn(true);
		when(propertyMeta.joinMeta()).thenReturn(joinEntityMeta);
		when(propertyMeta.joinIdMeta()).thenReturn(idMeta);

		Composite start = new Composite(), //
		end = new Composite(), //
		name1 = CompositeTestBuilder.builder().values("name1").buildSimple(), //
		name2 = CompositeTestBuilder.builder().values("name2").buildSimple(), //
		name3 = CompositeTestBuilder.builder().values("name3").buildSimple();
		int count = 2;

		Long joinId1 = 11L, joinId2 = 12L, joinId3 = 13L;
		Integer ttl = 10;

		HColumn<Composite, Object> hCol1 = HColumnTestBuilder.simple(name1,
				(Object) joinId1, ttl);
		HColumn<Composite, Object> hCol2 = HColumnTestBuilder.simple(name2,
				(Object) joinId2, ttl);
		HColumn<Composite, Object> hCol3 = HColumnTestBuilder.simple(name3,
				(Object) joinId3, ttl);

		when(columnsIterator.hasNext()).thenReturn(true, true, false, true,
				false);
		when(columnsIterator.next()).thenReturn(hCol1, hCol2, hCol3);

		Map<Object, Object> entitiesMap = new HashMap<Object, Object>();
		entitiesMap.put(joinId1, user1);
		entitiesMap.put(joinId2, user2);
		entitiesMap.put(joinId3, user3);

		when(policy.getCurrentReadLevel()).thenReturn(LOCAL_QUORUM, ONE);
		when(
				joinLoader.loadJoinEntities(UserBean.class,
						Arrays.<Object> asList(joinId1, joinId2),
						joinEntityMeta, joinEntityDao)).thenReturn(entitiesMap);
		when(
				joinLoader.loadJoinEntities(UserBean.class,
						Arrays.<Object> asList(joinId3), joinEntityMeta,
						joinEntityDao)).thenReturn(entitiesMap);

		iterator = new ThriftJoinSliceIterator<Long, Integer, UserBean>(policy,
				joinEntityDao, columnFamily, propertyMeta, query, start, end,
				false, count);

		Whitebox.setInternalState(iterator, ThriftJoinEntityLoader.class,
				joinLoader);

		assertThat(iterator.hasNext()).isEqualTo(true);
		HColumn<Composite, UserBean> h1 = iterator.next();

		assertThat(h1.getName().get(0, STRING_SRZ)).isEqualTo("name1");
		assertThat(h1.getValue().getName()).isEqualTo(user1.getName());

		assertThat(iterator.hasNext()).isEqualTo(true);
		HColumn<Composite, UserBean> h2 = iterator.next();

		assertThat(h2.getName().get(0, STRING_SRZ)).isEqualTo("name2");
		assertThat(h2.getValue().getName()).isEqualTo(user2.getName());

		assertThat(iterator.hasNext()).isEqualTo(true);
		HColumn<Composite, UserBean> h3 = iterator.next();

		assertThat(h3.getName().get(0, STRING_SRZ)).isEqualTo("name3");
		assertThat(h3.getValue().getName()).isEqualTo(user3.getName());

		assertThat(iterator.hasNext()).isEqualTo(false);

		verify(policy, times(2)).loadConsistencyLevelForRead(columnFamily);
		verify(policy, times(2)).setCurrentReadLevel(LOCAL_QUORUM);
		verify(policy, times(2)).setCurrentReadLevel(ONE);
	}
}
