package info.archinnov.achilles.wrapper;

import static javax.persistence.CascadeType.ALL;
import static javax.persistence.CascadeType.PERSIST;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import info.archinnov.achilles.composite.factory.DynamicCompositeKeyFactory;
import info.archinnov.achilles.dao.GenericDynamicCompositeDao;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.JoinProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.operations.EntityLoader;
import info.archinnov.achilles.entity.operations.EntityPersister;
import info.archinnov.achilles.helper.CompositeHelper;
import info.archinnov.achilles.wrapper.JoinWideMapWrapper;

import java.lang.reflect.Method;

import mapping.entity.UserBean;
import me.prettyprint.hector.api.beans.DynamicComposite;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;


/**
 * JoinWideMapWrapperTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class JoinWideMapWrapperTest
{

	@Rule
	public ExpectedException expectedEx = ExpectedException.none();

	@InjectMocks
	private JoinWideMapWrapper<Long, Integer, UserBean> wrapper;

	@Mock
	private GenericDynamicCompositeDao<Long> dao;

	@Mock
	private PropertyMeta<Integer, UserBean> joinWideMapMeta;

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
		JoinProperties joinProperties = new JoinProperties();
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

		JoinProperties joinProperties = prepareJoinProperties();
		joinProperties.addCascadeType(PERSIST);

		int key = 4567;
		UserBean userBean = new UserBean();
		long userId = 475L;
		userBean.setUserId(userId);
		DynamicComposite comp = new DynamicComposite();

		when(keyFactory.createForInsert(joinWideMapMeta, key)).thenReturn(comp);
		when(joinWideMapMeta.getJoinProperties()).thenReturn((JoinProperties) joinProperties);
		when(persister.cascadePersistOrEnsureExists(userBean, joinProperties)).thenReturn(userId);

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
		JoinProperties joinProperties = prepareJoinProperties();
		joinProperties.addCascadeType(ALL);

		int key = 4567;
		UserBean userBean = new UserBean();
		long userId = 475L;
		userBean.setUserId(userId);
		DynamicComposite comp = new DynamicComposite();

		when(keyFactory.createForInsert(joinWideMapMeta, key)).thenReturn(comp);
		when(joinWideMapMeta.getJoinProperties()).thenReturn((JoinProperties) joinProperties);
		when(persister.cascadePersistOrEnsureExists(userBean, joinProperties)).thenReturn(userId);

		wrapper.insert(key, userBean, 150);

		verify(dao).setValue(id, comp, userId, 150);
	}

	private JoinProperties prepareJoinProperties() throws Exception
	{
		EntityMeta<Long> joinEntityMeta = new EntityMeta<Long>();
		joinEntityMeta.setClassName("canonicalClassName");

		Method idGetter = UserBean.class.getDeclaredMethod("getUserId");
		PropertyMeta<Void, Long> idMeta = new PropertyMeta<Void, Long>();
		idMeta.setType(PropertyType.SIMPLE);
		idMeta.setGetter(idGetter);
		joinEntityMeta.setIdMeta(idMeta);
		JoinProperties joinProperties = new JoinProperties();
		joinProperties.setEntityMeta(joinEntityMeta);

		return joinProperties;
	}
}
