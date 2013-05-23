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
import info.archinnov.achilles.helper.ThriftJoinEntityHelper;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import mapping.entity.UserBean;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

import testBuilders.CompositeTestBuilder;
import testBuilders.HColumnTestBuilder;
import testBuilders.PropertyMetaTestBuilder;

/**
 * ThriftJoinSliceIteratorTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class ThriftJoinSliceIteratorTest
{
	@Mock
	private PropertyMeta<Integer, UserBean> propertyMeta;

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
	private ThriftJoinEntityHelper joinHelper;

	@Mock
	private ThriftGenericEntityDao joinEntityDao;

	private UserBean user1 = new UserBean();
	private UserBean user2 = new UserBean();
	private UserBean user3 = new UserBean();
	private ObjectMapper objectMapper = new ObjectMapper();

	private EntityMeta joinEntityMeta = new EntityMeta();

	private ThriftJoinSliceIterator<Long, Integer, UserBean> iterator;

	@Mock
	private ThriftConsistencyLevelPolicy policy;

	private String columnFamily = "cf";

	@Before
	public void setUp()
	{
		when(query.execute()).thenReturn(queryResult);
		when(queryResult.get()).thenReturn(columnSlice);
		when(columnSlice.getColumns()).thenReturn(hColumns);
		when(hColumns.iterator()).thenReturn(columnsIterator);
		when(propertyMeta.getValueClass()).thenReturn(UserBean.class);

		user1.setName("user1");
		user2.setName("user2");
		user3.setName("user3");

		when(propertyMeta.joinMeta()).thenReturn(joinEntityMeta);

		PropertyMeta<Void, Long> joinIdMeta = new PropertyMeta<Void, Long>();
		joinIdMeta.setValueClass(Long.class);
		joinIdMeta.setObjectMapper(objectMapper);
		when((PropertyMeta<Void, Long>) propertyMeta.joinIdMeta()).thenReturn(joinIdMeta);
	}

	@Test
	public void should_return_3_entities() throws Exception
	{

		PropertyMeta<Void, Long> idMeta = PropertyMetaTestBuilder //
				.valueClass(Long.class)
				.type(PropertyType.SIMPLE)
				.build();

		joinEntityMeta.setIdMeta(idMeta);
		when(propertyMeta.type()).thenReturn(PropertyType.JOIN_WIDE_MAP);
		when(propertyMeta.joinMeta()).thenReturn(joinEntityMeta);
		when((PropertyMeta<Void, Long>) propertyMeta.joinIdMeta()).thenReturn(idMeta);

		Composite start = new Composite(), //
		end = new Composite(), //
		name1 = CompositeTestBuilder.builder().values("name1").buildSimple(), //
		name2 = CompositeTestBuilder.builder().values("name2").buildSimple(), //
		name3 = CompositeTestBuilder.builder().values("name3").buildSimple();

		Long joinId1 = 11L, joinId2 = 12L, joinId3 = 13L;
		Integer ttl = 10;

		HColumn<Composite, Object> hCol1 = HColumnTestBuilder.simple(name1, (Object) joinId1, ttl);
		HColumn<Composite, Object> hCol2 = HColumnTestBuilder.simple(name2, (Object) joinId2, ttl);
		HColumn<Composite, Object> hCol3 = HColumnTestBuilder.simple(name3, (Object) joinId3, ttl);

		Map<Long, UserBean> entitiesMap = new HashMap<Long, UserBean>();
		entitiesMap.put(joinId1, user1);
		entitiesMap.put(joinId2, user2);
		entitiesMap.put(joinId3, user3);

		when(policy.getCurrentReadLevel()).thenReturn(LOCAL_QUORUM, ONE);

		List<Long> keys = Arrays.asList(joinId1, joinId2, joinId3);
		when(joinHelper.loadJoinEntities(UserBean.class, keys, joinEntityMeta, joinEntityDao))
				.thenReturn(entitiesMap);

		iterator = new ThriftJoinSliceIterator<Long, Integer, UserBean>(policy, joinEntityDao,
				columnFamily, propertyMeta, query, start, end, false, 10);
		Whitebox.setInternalState(iterator, "joinHelper", joinHelper);

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
	public void should_reload_load_when_reaching_end_of_batch() throws Exception
	{
		PropertyMeta<Void, Long> idMeta = PropertyMetaTestBuilder //
				.valueClass(Long.class)
				.type(PropertyType.SIMPLE)
				.build();

		joinEntityMeta.setIdMeta(idMeta);
		when(propertyMeta.type()).thenReturn(PropertyType.JOIN_WIDE_MAP);
		when(propertyMeta.joinMeta()).thenReturn(joinEntityMeta);
		when((PropertyMeta<Void, Long>) propertyMeta.joinIdMeta()).thenReturn(idMeta);

		Composite start = new Composite(), //
		end = new Composite(), //
		name1 = CompositeTestBuilder.builder().values("name1").buildSimple(), //
		name2 = CompositeTestBuilder.builder().values("name2").buildSimple(), //
		name3 = CompositeTestBuilder.builder().values("name3").buildSimple();
		int count = 2;

		Long joinId1 = 11L, joinId2 = 12L, joinId3 = 13L;
		Integer ttl = 10;

		HColumn<Composite, Object> hCol1 = HColumnTestBuilder.simple(name1, (Object) joinId1, ttl);
		HColumn<Composite, Object> hCol2 = HColumnTestBuilder.simple(name2, (Object) joinId2, ttl);
		HColumn<Composite, Object> hCol3 = HColumnTestBuilder.simple(name3, (Object) joinId3, ttl);

		when(columnsIterator.hasNext()).thenReturn(true, true, false, true, false);
		when(columnsIterator.next()).thenReturn(hCol1, hCol2, hCol3);

		Map<Long, UserBean> entitiesMap = new HashMap<Long, UserBean>();
		entitiesMap.put(joinId1, user1);
		entitiesMap.put(joinId2, user2);
		entitiesMap.put(joinId3, user3);

		when(policy.getCurrentReadLevel()).thenReturn(LOCAL_QUORUM, ONE);
		when(
				joinHelper.loadJoinEntities(UserBean.class, Arrays.asList(joinId1, joinId2),
						joinEntityMeta, joinEntityDao)).thenReturn(entitiesMap);
		when(
				joinHelper.loadJoinEntities(UserBean.class, Arrays.asList(joinId3), joinEntityMeta,
						joinEntityDao)).thenReturn(entitiesMap);

		iterator = new ThriftJoinSliceIterator<Long, Integer, UserBean>(policy, joinEntityDao,
				columnFamily, propertyMeta, query, start, end, false, count);

		Whitebox.setInternalState(iterator, "joinHelper", joinHelper);

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
