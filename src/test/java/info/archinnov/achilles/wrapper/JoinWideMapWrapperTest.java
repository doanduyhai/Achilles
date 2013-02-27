package info.archinnov.achilles.wrapper;

import static info.archinnov.achilles.entity.metadata.PropertyType.SIMPLE;
import static javax.persistence.CascadeType.ALL;
import static javax.persistence.CascadeType.PERSIST;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.composite.factory.DynamicCompositeKeyFactory;
import info.archinnov.achilles.dao.GenericDynamicCompositeDao;
import info.archinnov.achilles.entity.EntityHelper;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.JoinProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.operations.EntityLoader;
import info.archinnov.achilles.entity.operations.EntityPersister;
import info.archinnov.achilles.helper.CompositeHelper;
import info.archinnov.achilles.proxy.interceptor.AchillesInterceptor;

import java.lang.reflect.Method;

import mapping.entity.UserBean;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.mutation.Mutator;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import testBuilders.PropertyMetaTestBuilder;

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
	private GenericDynamicCompositeDao<Long> entityDao;

	@Mock
	private GenericDynamicCompositeDao<Long> joinDao;

	@Mock
	private PropertyMeta<Integer, UserBean> propertyMeta;

	@Mock
	private DynamicCompositeKeyFactory keyFactory;

	@Mock
	private EntityPersister persister;

	@Mock
	private EntityLoader loader;

	@Mock
	private CompositeHelper compositeHelper;

	@Mock
	private EntityHelper entityHelper;

	@Mock
	private AchillesInterceptor interceptor;

	@Mock
	private Mutator<Long> mutator;

	@Mock
	private Mutator<Long> joinMutator;

	private Long id = 7425L;

	@Before
	public void setUp()
	{
		wrapper.setId(id);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_get_value() throws Exception
	{
		Long joinId = 1235L;
		int key = 4567;
		UserBean userBean = new UserBean();
		DynamicComposite comp = new DynamicComposite();

		EntityMeta<Long> joinEntityMeta = new EntityMeta<Long>();
		PropertyMeta<Void, Long> joinIdMeta = PropertyMetaTestBuilder.valueClass(Long.class)
				.type(SIMPLE).build();
		joinEntityMeta.setIdMeta(joinIdMeta);

		when((PropertyMeta<Void, Long>) propertyMeta.joinIdMeta()).thenReturn(joinIdMeta);

		when(propertyMeta.getValueClass()).thenReturn(UserBean.class);
		when(keyFactory.createForInsert(propertyMeta, key)).thenReturn(comp);
		when(entityDao.getValue(id, comp)).thenReturn(joinId.toString());
		when(loader.load(UserBean.class, joinId, joinEntityMeta)).thenReturn(userBean);
		when((EntityMeta<Long>) propertyMeta.joinMeta()).thenReturn(joinEntityMeta);
		when(entityHelper.buildProxy(userBean, joinEntityMeta)).thenReturn(userBean);
		UserBean expected = wrapper.get(key);

		assertThat(expected).isSameAs(userBean);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_insert_value_and_entity_when_insertable() throws Exception
	{

		JoinProperties joinProperties = prepareJoinProperties();
		joinProperties.addCascadeType(PERSIST);
		PropertyMeta<Void, Long> joinIdMeta = mock(PropertyMeta.class);

		int key = 4567;
		UserBean userBean = new UserBean();
		Long userId = 475L;
		userBean.setUserId(userId);
		DynamicComposite comp = new DynamicComposite();

		when(keyFactory.createForInsert(propertyMeta, key)).thenReturn(comp);
		when(propertyMeta.getJoinProperties()).thenReturn((JoinProperties) joinProperties);
		when(joinDao.buildMutator()).thenReturn(joinMutator);
		when(persister.cascadePersistOrEnsureExists(userBean, joinProperties)).thenReturn(userId);
		when((PropertyMeta<Void, Long>) propertyMeta.joinIdMeta()).thenReturn(joinIdMeta);
		when(joinIdMeta.writeValueToString(userId)).thenReturn(userId.toString());
		when(interceptor.isBatchMode()).thenReturn(false);

		wrapper.insert(key, userBean);

		verify(entityDao).setValue(id, comp, userId.toString());
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Test
	public void should_insert_value_in_batch_mode_and_entity_when_insertable() throws Exception
	{

		JoinProperties joinProperties = prepareJoinProperties();
		joinProperties.addCascadeType(PERSIST);
		PropertyMeta<Void, Long> joinIdMeta = mock(PropertyMeta.class);

		int key = 4567;
		UserBean userBean = new UserBean();
		Long userId = 475L;
		userBean.setUserId(userId);
		DynamicComposite comp = new DynamicComposite();

		when(keyFactory.createForInsert(propertyMeta, key)).thenReturn(comp);
		when(propertyMeta.getJoinProperties()).thenReturn((JoinProperties) joinProperties);
		when(propertyMeta.getPropertyName()).thenReturn("joinProperty");
		when(interceptor.getMutatorForProperty("joinProperty")).thenReturn((Mutator) joinMutator);
		when(persister.cascadePersistOrEnsureExists(userBean, joinProperties, joinMutator))
				.thenReturn(userId);

		when((PropertyMeta<Void, Long>) propertyMeta.joinIdMeta()).thenReturn(joinIdMeta);
		when(joinIdMeta.writeValueToString(userId)).thenReturn(userId.toString());
		when(interceptor.isBatchMode()).thenReturn(true);
		when(interceptor.getMutator()).thenReturn((Mutator) mutator);
		wrapper.insert(key, userBean);

		verify(entityDao).setValueBatch(id, comp, userId.toString(), mutator);
	}

	@Test(expected = IllegalArgumentException.class)
	public void should_exception_when_trying_to_persist_null_entity() throws Exception
	{
		int key = 4567;
		wrapper.insert(key, null);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_insert_value_and_entity_with_ttl() throws Exception
	{
		JoinProperties joinProperties = prepareJoinProperties();
		joinProperties.addCascadeType(ALL);
		PropertyMeta<Void, Long> joinIdMeta = mock(PropertyMeta.class);

		int key = 4567;
		UserBean userBean = new UserBean();
		Long userId = 475L;
		userBean.setUserId(userId);
		DynamicComposite comp = new DynamicComposite();

		when(keyFactory.createForInsert(propertyMeta, key)).thenReturn(comp);
		when(propertyMeta.getJoinProperties()).thenReturn((JoinProperties) joinProperties);
		when(joinDao.buildMutator()).thenReturn(joinMutator);
		when(persister.cascadePersistOrEnsureExists(userBean, joinProperties)).thenReturn(userId);
		when((PropertyMeta<Void, Long>) propertyMeta.joinIdMeta()).thenReturn(joinIdMeta);
		when(joinIdMeta.writeValueToString(userId)).thenReturn(userId.toString());
		when(interceptor.isBatchMode()).thenReturn(false);
		wrapper.insert(key, userBean, 150);

		verify(entityDao).setValue(id, comp, userId.toString(), 150);
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Test
	public void should_insert_value_in_batch_mode_and_entity_with_ttl() throws Exception
	{
		JoinProperties joinProperties = prepareJoinProperties();
		joinProperties.addCascadeType(ALL);
		PropertyMeta<Void, Long> joinIdMeta = mock(PropertyMeta.class);

		int key = 4567;
		UserBean userBean = new UserBean();
		Long userId = 475L;
		userBean.setUserId(userId);
		DynamicComposite comp = new DynamicComposite();

		when(keyFactory.createForInsert(propertyMeta, key)).thenReturn(comp);
		when(propertyMeta.getJoinProperties()).thenReturn((JoinProperties) joinProperties);
		when(propertyMeta.getPropertyName()).thenReturn("joinProperty");
		when(interceptor.getMutatorForProperty("joinProperty")).thenReturn((Mutator) joinMutator);
		when(persister.cascadePersistOrEnsureExists(userBean, joinProperties, joinMutator))
				.thenReturn(userId);
		when((PropertyMeta<Void, Long>) propertyMeta.joinIdMeta()).thenReturn(joinIdMeta);
		when(joinIdMeta.writeValueToString(userId)).thenReturn(userId.toString());
		when(interceptor.isBatchMode()).thenReturn(true);
		when(interceptor.getMutator()).thenReturn((Mutator) mutator);
		wrapper.insert(key, userBean, 150);

		verify(entityDao).setValueBatch(id, comp, userId.toString(), 150, mutator);
	}

	private JoinProperties prepareJoinProperties() throws Exception
	{
		EntityMeta<Long> joinEntityMeta = new EntityMeta<Long>();
		joinEntityMeta.setClassName("canonicalClassName");
		joinEntityMeta.setEntityDao(joinDao);

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
