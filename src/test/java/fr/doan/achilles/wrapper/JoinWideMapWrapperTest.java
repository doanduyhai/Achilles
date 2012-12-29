package fr.doan.achilles.wrapper;

import static javax.persistence.CascadeType.ALL;
import static javax.persistence.CascadeType.PERSIST;
import static javax.persistence.CascadeType.REMOVE;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;

import mapping.entity.UserBean;
import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import fr.doan.achilles.composite.factory.DynamicCompositeKeyFactory;
import fr.doan.achilles.dao.GenericEntityDao;
import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.entity.metadata.JoinProperties;
import fr.doan.achilles.entity.metadata.JoinWideMapMeta;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.metadata.SimpleMeta;
import fr.doan.achilles.entity.operations.EntityLoader;
import fr.doan.achilles.entity.operations.EntityPersister;
import fr.doan.achilles.helper.CompositeHelper;

/**
 * JoinWideMapWrapperTest
 * 
 * @author DuyHai DOAN
 * 
 */
@SuppressWarnings(
{
		"unchecked",
		"rawtypes"
})
@RunWith(MockitoJUnitRunner.class)
public class JoinWideMapWrapperTest
{

	@Rule
	public ExpectedException expectedEx = ExpectedException.none();

	@InjectMocks
	private JoinWideMapWrapper<Long, Integer, UserBean> wrapper;

	@Mock
	private GenericEntityDao<Long> dao;

	@Mock
	private JoinWideMapMeta<Integer, UserBean> joinWideMapMeta;

	@Mock
	private DynamicCompositeKeyFactory keyFactory;

	@Mock
	private EntityPersister persister;

	@Mock
	private EntityLoader loader;

	@Mock
	private CompositeHelper helper;

	private Long id = 7425L;

	@Before
	public void setUp()
	{
		ReflectionTestUtils.setField(wrapper, "helper", helper);
		ReflectionTestUtils.setField(wrapper, "persister", persister);
		ReflectionTestUtils.setField(wrapper, "loader", loader);
		ReflectionTestUtils.setField(wrapper, "id", id);
	}

	@Test
	public void should_get_value() throws Exception
	{
		Long joinId = 1235L;
		int key = 4567;
		UserBean userBean = new UserBean();
		DynamicComposite comp = new DynamicComposite();

		EntityMeta<Long> joinEntityMeta = new EntityMeta<Long>();
		JoinProperties<Long> joinProperties = new JoinProperties<Long>();
		joinProperties.setEntityMeta(joinEntityMeta);

		when(joinWideMapMeta.getValueClass()).thenReturn(UserBean.class);
		when(joinWideMapMeta.getJoinProperties()).thenReturn((JoinProperties) joinProperties);

		when(keyFactory.createForInsert(joinWideMapMeta, key)).thenReturn(comp);
		when(dao.getValue(id, comp)).thenReturn(joinId);
		when(loader.loadJoinEntity(UserBean.class, joinId, joinEntityMeta)).thenReturn(userBean);

		UserBean expected = wrapper.get(key);

		assertThat(expected).isSameAs(userBean);
	}

	@Test
	public void should_insert_value_and_entity_when_insertable() throws Exception
	{

		JoinProperties<Long> joinProperties = prepareJoinProperties();
		joinProperties.setCascadeType(PERSIST);

		int key = 4567;
		UserBean userBean = new UserBean();
		long userId = 475L;
		userBean.setUserId(userId);
		DynamicComposite comp = new DynamicComposite();

		when(keyFactory.createForInsert(joinWideMapMeta, key)).thenReturn(comp);
		when(joinWideMapMeta.getJoinProperties()).thenReturn((JoinProperties) joinProperties);
		when(persister.persistOrEnsureJoinEntityExists(userBean, joinProperties))
				.thenReturn(userId);

		wrapper.insert(key, userBean);

		verify(dao).setValue(id, comp, userId);
	}

	@Test(expected = IllegalArgumentException.class)
	public void should_exception_when_trying_to_persist_null_entity() throws Exception
	{
		int key = 4567;
		wrapper.insert(key, null);
	}

	@Test
	public void should_insert_value_and_entity_with_ttl() throws Exception
	{
		JoinProperties<Long> joinProperties = prepareJoinProperties();
		joinProperties.setCascadeType(ALL);

		int key = 4567;
		UserBean userBean = new UserBean();
		long userId = 475L;
		userBean.setUserId(userId);
		DynamicComposite comp = new DynamicComposite();

		when(keyFactory.createForInsert(joinWideMapMeta, key)).thenReturn(comp);
		when(joinWideMapMeta.getJoinProperties()).thenReturn((JoinProperties) joinProperties);
		when(persister.persistOrEnsureJoinEntityExists(userBean, joinProperties))
				.thenReturn(userId);

		wrapper.insert(key, userBean, 150);

		verify(dao).setValue(id, comp, userId, 150);
	}

	@Test
	public void should_remove_value_only() throws Exception
	{
		int key = 4567;
		JoinProperties<Long> joinProperties = prepareJoinProperties();

		DynamicComposite comp = new DynamicComposite();

		when(keyFactory.createForInsert(joinWideMapMeta, key)).thenReturn(comp);
		when(joinWideMapMeta.getJoinProperties()).thenReturn((JoinProperties) joinProperties);

		wrapper.remove(key);

		verify(dao).removeColumn(id, comp);
	}

	@Test
	public void should_remove_value_and_entity() throws Exception
	{
		int key = 4567;
		long userId = 475L;
		JoinProperties<Long> joinProperties = prepareJoinProperties();
		joinProperties.setCascadeType(REMOVE);

		DynamicComposite comp = new DynamicComposite();

		when(dao.getValue(id, comp)).thenReturn(userId);
		when(keyFactory.createForInsert(joinWideMapMeta, key)).thenReturn(comp);
		when(joinWideMapMeta.getJoinProperties()).thenReturn((JoinProperties) joinProperties);

		wrapper.remove(key);

		verify(persister).removeById(userId, joinProperties.getEntityMeta());
		verify(dao).removeColumn(id, comp);
	}

	@Test
	public void should_remove_range_value_only() throws Exception
	{
		int start = 12, end = 15;
		boolean inclusiveStart = true, inclusiveEnd = false, reverse = false;

		DynamicComposite startComp = new DynamicComposite();
		DynamicComposite endComp = new DynamicComposite();

		DynamicComposite[] queryComps = new DynamicComposite[]
		{
				startComp,
				endComp
		};
		JoinProperties<Long> joinProperties = prepareJoinProperties();

		when(
				keyFactory.createForQuery(joinWideMapMeta, start, inclusiveStart, end,
						inclusiveEnd, false)).thenReturn(queryComps);
		when(joinWideMapMeta.getJoinProperties()).thenReturn((JoinProperties) joinProperties);

		wrapper.removeRange(start, inclusiveStart, end, inclusiveEnd);

		verify(helper).checkBounds(joinWideMapMeta, start, end, reverse);
		verify(dao).removeColumnRange(id, startComp, endComp);
	}

	@Test
	public void should_remove_range_value_and_entity() throws Exception
	{
		int start = 12, end = 15;
		boolean inclusiveStart = true, inclusiveEnd = false, reverse = false;
		long userId = 475L;

		DynamicComposite startComp = new DynamicComposite();
		DynamicComposite endComp = new DynamicComposite();

		DynamicComposite[] queryComps = new DynamicComposite[]
		{
				startComp,
				endComp
		};
		JoinProperties<Long> joinProperties = prepareJoinProperties();
		joinProperties.setCascadeType(ALL);

		ColumnSliceIterator<Long, DynamicComposite, Object> iterator = mock(ColumnSliceIterator.class);
		HColumn<DynamicComposite, Object> hColumn = mock(HColumn.class);

		when(
				keyFactory.createForQuery(joinWideMapMeta, start, inclusiveStart, end,
						inclusiveEnd, false)).thenReturn(queryComps);
		when(joinWideMapMeta.getJoinProperties()).thenReturn((JoinProperties) joinProperties);

		when(dao.getColumnsIterator(id, startComp, endComp, false)).thenReturn(iterator);
		when(iterator.hasNext()).thenReturn(true, false);
		when(iterator.next()).thenReturn(hColumn);
		when(hColumn.getValue()).thenReturn(userId);
		wrapper.removeRange(start, inclusiveStart, end, inclusiveEnd);

		verify(helper).checkBounds(joinWideMapMeta, start, end, reverse);
		verify(persister).removeById(userId, joinProperties.getEntityMeta());
		verify(dao).removeColumnRange(id, startComp, endComp);
	}

	private JoinProperties<Long> prepareJoinProperties() throws Exception
	{
		EntityMeta<Long> joinEntityMeta = new EntityMeta<Long>();
		joinEntityMeta.setCanonicalClassName("canonicalClassName");

		Method idGetter = UserBean.class.getDeclaredMethod("getUserId");
		PropertyMeta<Void, Long> idMeta = new SimpleMeta<Long>();
		idMeta.setGetter(idGetter);
		joinEntityMeta.setIdMeta(idMeta);
		JoinProperties<Long> joinProperties = new JoinProperties<Long>();
		joinProperties.setEntityMeta(joinEntityMeta);

		return joinProperties;
	}
}
