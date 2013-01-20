package fr.doan.achilles.iterator;

import static fr.doan.achilles.serializer.SerializerUtils.DYNA_COMP_SRZ;
import static fr.doan.achilles.serializer.SerializerUtils.LONG_SRZ;
import static fr.doan.achilles.serializer.SerializerUtils.OBJECT_SRZ;
import static fr.doan.achilles.serializer.SerializerUtils.STRING_SRZ;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.entity.metadata.JoinProperties;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.operations.EntityLoader;

/**
 * JoinColumnSliceIteratorTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class JoinColumnSliceIteratorTest
{
	@Mock
	private PropertyMeta<Integer, UserBean> propertyMeta;

	@Mock
	private SliceQuery<Long, DynamicComposite, Long> query;

	@Mock
	private QueryResult<ColumnSlice<DynamicComposite, Long>> queryResult;

	@Mock
	private ColumnSlice<DynamicComposite, Long> columnSlice;

	@Mock
	private List<HColumn<DynamicComposite, Long>> hColumns;

	@Mock
	private Iterator<HColumn<DynamicComposite, Long>> columnsIterator;

	@Mock
	private EntityLoader loader;

	private UserBean user1 = new UserBean();
	private UserBean user2 = new UserBean();
	private UserBean user3 = new UserBean();

	private EntityMeta<Long> joinEntityMeta = new EntityMeta<Long>();

	JoinColumnSliceIterator<Long, DynamicComposite, Long, Integer, UserBean> iterator;

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
		DynamicComposite start = new DynamicComposite(), //
		end = new DynamicComposite(), //
		name1 = new DynamicComposite(), //
		name2 = new DynamicComposite(), //
		name3 = new DynamicComposite();

		name1.addComponent("name1", STRING_SRZ);
		name2.addComponent("name2", STRING_SRZ);
		name3.addComponent("name3", STRING_SRZ);

		Long joinId1 = 11L, joinId2 = 12L, joinId3 = 13L, ttl = 10L;

		HColumn<DynamicComposite, Long> hCol1 = HFactory.createColumn(name1, joinId1, ttl,
				DYNA_COMP_SRZ, LONG_SRZ);
		HColumn<DynamicComposite, Long> hCol2 = HFactory.createColumn(name2, joinId2, ttl,
				DYNA_COMP_SRZ, LONG_SRZ);
		HColumn<DynamicComposite, Long> hCol3 = HFactory.createColumn(name3, joinId3, ttl,
				DYNA_COMP_SRZ, LONG_SRZ);

		when(columnsIterator.hasNext()).thenReturn(true, true, true, false);
		when(columnsIterator.next()).thenReturn(hCol1, hCol2, hCol3);

		Map<Long, UserBean> entitiesMap = new HashMap<Long, UserBean>();
		entitiesMap.put(joinId1, user1);
		entitiesMap.put(joinId2, user2);
		entitiesMap.put(joinId3, user3);

		when(
				loader.loadJoinEntities(UserBean.class, Arrays.asList(joinId1, joinId2, joinId3),
						joinEntityMeta)).thenReturn(entitiesMap);
		iterator = new JoinColumnSliceIterator<Long, DynamicComposite, Long, Integer, UserBean>(
				propertyMeta, query, start, end, false, 10);

		ReflectionTestUtils.setField(iterator, "loader", loader);

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
		DynamicComposite start = new DynamicComposite(), //
		end = new DynamicComposite(), //
		name1 = new DynamicComposite(), //
		name2 = new DynamicComposite(), //
		name3 = new DynamicComposite();
		int count = 2;

		name1.addComponent("name1", STRING_SRZ);
		name2.addComponent("name2", STRING_SRZ);
		name3.addComponent("name3", STRING_SRZ);

		Long joinId1 = 11L, joinId2 = 12L, joinId3 = 13L, ttl = 10L;

		HColumn<DynamicComposite, Long> hCol1 = HFactory.createColumn(name1, joinId1, ttl,
				DYNA_COMP_SRZ, LONG_SRZ);
		HColumn<DynamicComposite, Long> hCol2 = HFactory.createColumn(name2, joinId2, ttl,
				DYNA_COMP_SRZ, LONG_SRZ);
		HColumn<DynamicComposite, Long> hCol3 = HFactory.createColumn(name3, joinId3, ttl,
				DYNA_COMP_SRZ, LONG_SRZ);

		when(columnsIterator.hasNext()).thenReturn(true, true, false, true, true, false);
		when(columnsIterator.next()).thenReturn(hCol1, hCol2, hCol2, hCol3);

		Map<Long, UserBean> entitiesMap = new HashMap<Long, UserBean>();
		entitiesMap.put(joinId1, user1);
		entitiesMap.put(joinId2, user2);
		entitiesMap.put(joinId3, user3);

		when(
				loader.loadJoinEntities(UserBean.class, Arrays.asList(joinId1, joinId2),
						joinEntityMeta)).thenReturn(entitiesMap);
		when(
				loader.loadJoinEntities(UserBean.class, Arrays.asList(joinId2, joinId3),
						joinEntityMeta)).thenReturn(entitiesMap);

		iterator = new JoinColumnSliceIterator<Long, DynamicComposite, Long, Integer, UserBean>(
				propertyMeta, query, start, end, false, count);

		ReflectionTestUtils.setField(iterator, "loader", loader);

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
