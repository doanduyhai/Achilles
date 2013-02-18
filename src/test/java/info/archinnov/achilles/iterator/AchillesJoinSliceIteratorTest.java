package info.archinnov.achilles.iterator;

import static info.archinnov.achilles.serializer.SerializerUtils.OBJECT_SRZ;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.JoinProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.operations.EntityLoader;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import mapping.entity.UserBean;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import testBuilders.CompositeTestBuilder;
import testBuilders.HColumnTestBuilder;
import testBuilders.PropertyMetaTestBuilder;

/**
 * JoinColumnSliceIteratorTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class AchillesJoinSliceIteratorTest
{
	@Mock
	private PropertyMeta<Integer, UserBean> propertyMeta;

	@Mock
	private SliceQuery<Long, DynamicComposite, String> query;

	@Mock
	private QueryResult<ColumnSlice<DynamicComposite, String>> queryResult;

	@Mock
	private ColumnSlice<DynamicComposite, String> columnSlice;

	@Mock
	private List<HColumn<DynamicComposite, String>> hColumns;

	@Mock
	private Iterator<HColumn<DynamicComposite, String>> columnsIterator;

	@Mock
	private EntityLoader loader;

	private UserBean user1 = new UserBean();
	private UserBean user2 = new UserBean();
	private UserBean user3 = new UserBean();

	private EntityMeta<Long> joinEntityMeta = new EntityMeta<Long>();

	AchillesJoinSliceIterator<Long, DynamicComposite, String, Integer, UserBean> iterator;

	@SuppressWarnings(
	{
			"rawtypes",
			"unchecked"
	})
	@Before
	public void setUp()
	{
		when(query.execute()).thenReturn(queryResult);
		when(queryResult.get()).thenReturn(columnSlice);
		when(columnSlice.getColumns()).thenReturn(hColumns);
		when(hColumns.iterator()).thenReturn(columnsIterator);
		when(propertyMeta.getValueClass()).thenReturn(UserBean.class);
		when(propertyMeta.getValueSerializer()).thenReturn((Serializer) OBJECT_SRZ);

		JoinProperties joinProperties = new JoinProperties();
		joinProperties.setEntityMeta(joinEntityMeta);

		user1.setName("user1");
		user2.setName("user2");
		user3.setName("user3");

		when(propertyMeta.getJoinProperties()).thenReturn(joinProperties);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_return_3_entities() throws Exception
	{

		PropertyMeta<Void, Long> idMeta = PropertyMetaTestBuilder //
				.valueClass(Long.class) //
				.build();

		joinEntityMeta.setIdMeta(idMeta);
		when(propertyMeta.type()).thenReturn(PropertyType.JOIN_WIDE_MAP);

		DynamicComposite start = new DynamicComposite(), //
		end = new DynamicComposite(), //
		name1 = CompositeTestBuilder.builder().values("name1").buildDynamic(), //
		name2 = CompositeTestBuilder.builder().values("name2").buildDynamic(), //
		name3 = CompositeTestBuilder.builder().values("name3").buildDynamic();

		Long joinId1 = 11L, joinId2 = 12L, joinId3 = 13L;
		Integer ttl = 10;

		HColumnTestBuilder.dynamic(name1, joinId1.toString(), ttl);
		HColumn<DynamicComposite, String> hCol1 = HColumnTestBuilder.dynamic(name1,
				joinId1.toString(), ttl);
		HColumn<DynamicComposite, String> hCol2 = HColumnTestBuilder.dynamic(name2,
				joinId2.toString(), ttl);
		HColumn<DynamicComposite, String> hCol3 = HColumnTestBuilder.dynamic(name3,
				joinId3.toString(), ttl);

		Map<Long, UserBean> entitiesMap = new HashMap<Long, UserBean>();
		entitiesMap.put(joinId1, user1);
		entitiesMap.put(joinId2, user2);
		entitiesMap.put(joinId3, user3);

		when(
				loader.loadJoinEntities(UserBean.class, Arrays.asList(joinId1, joinId2, joinId3),
						joinEntityMeta)).thenReturn(entitiesMap);
		iterator = new AchillesJoinSliceIterator<Long, DynamicComposite, String, Integer, UserBean>(
				propertyMeta, query, start, end, false, 10);

		iterator.loader = loader;
		when(columnsIterator.next()).thenReturn(hCol1, hCol2, hCol3);

		when(columnsIterator.hasNext()).thenReturn(true, true, true, false);

		assertThat(iterator.hasNext()).isEqualTo(true);
		HColumn<DynamicComposite, UserBean> h1 = iterator.next();

		assertThat(h1.getName()).isEqualTo(name1);
		assertThat(h1.getValue().getName()).isEqualTo(user1.getName());

		assertThat(iterator.hasNext()).isEqualTo(true);
		HColumn<DynamicComposite, UserBean> h2 = iterator.next();

		assertThat(h2.getName()).isEqualTo(name2);
		assertThat(h2.getValue().getName()).isEqualTo(user2.getName());

		assertThat(iterator.hasNext()).isEqualTo(true);
		HColumn<DynamicComposite, UserBean> h3 = iterator.next();

		assertThat(h3.getName()).isEqualTo(name3);
		assertThat(h3.getValue().getName()).isEqualTo(user3.getName());

		assertThat(iterator.hasNext()).isEqualTo(false);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_reload_load_when_reaching_end_of_batch() throws Exception
	{
		PropertyMeta<Void, Long> idMeta = PropertyMetaTestBuilder //
				.valueClass(Long.class) //
				.build();

		joinEntityMeta.setIdMeta(idMeta);
		when(propertyMeta.type()).thenReturn(PropertyType.JOIN_WIDE_MAP);

		DynamicComposite start = new DynamicComposite(), //
		end = new DynamicComposite(), //
		name1 = CompositeTestBuilder.builder().values("name1").buildDynamic(), //
		name2 = CompositeTestBuilder.builder().values("name2").buildDynamic(), //
		name3 = CompositeTestBuilder.builder().values("name3").buildDynamic();
		int count = 2;

		Long joinId1 = 11L, joinId2 = 12L, joinId3 = 13L;
		Integer ttl = 10;

		HColumn<DynamicComposite, String> hCol1 = HColumnTestBuilder.dynamic(name1,
				joinId1.toString(), ttl);
		HColumn<DynamicComposite, String> hCol2 = HColumnTestBuilder.dynamic(name2,
				joinId2.toString(), ttl);
		HColumn<DynamicComposite, String> hCol3 = HColumnTestBuilder.dynamic(name3,
				joinId3.toString(), ttl);

		when(columnsIterator.hasNext()).thenReturn(true, true, false, true, false);
		when(columnsIterator.next()).thenReturn(hCol1, hCol2, hCol3);

		Map<Long, UserBean> entitiesMap = new HashMap<Long, UserBean>();
		entitiesMap.put(joinId1, user1);
		entitiesMap.put(joinId2, user2);
		entitiesMap.put(joinId3, user3);

		when(
				loader.loadJoinEntities(UserBean.class, Arrays.asList(joinId1, joinId2),
						joinEntityMeta)).thenReturn(entitiesMap);
		when(loader.loadJoinEntities(UserBean.class, Arrays.asList(joinId3), joinEntityMeta))
				.thenReturn(entitiesMap);

		iterator = new AchillesJoinSliceIterator<Long, DynamicComposite, String, Integer, UserBean>(
				propertyMeta, query, start, end, false, count);

		iterator.loader = loader;

		assertThat(iterator.hasNext()).isEqualTo(true);
		HColumn<DynamicComposite, UserBean> h1 = iterator.next();

		assertThat(h1.getName()).isEqualTo(name1);
		assertThat(h1.getValue().getName()).isEqualTo(user1.getName());

		assertThat(iterator.hasNext()).isEqualTo(true);
		HColumn<DynamicComposite, UserBean> h2 = iterator.next();

		assertThat(h2.getName()).isEqualTo(name2);
		assertThat(h2.getValue().getName()).isEqualTo(user2.getName());

		assertThat(iterator.hasNext()).isEqualTo(true);
		HColumn<DynamicComposite, UserBean> h3 = iterator.next();

		assertThat(h3.getName()).isEqualTo(name3);
		assertThat(h3.getValue().getName()).isEqualTo(user3.getName());

		assertThat(iterator.hasNext()).isEqualTo(false);

		verify(query).setRange(name2, end, false, count);
	}
}
