package fr.doan.achilles.entity.operations;

import static fr.doan.achilles.entity.metadata.PropertyType.JOIN_SIMPLE;
import static fr.doan.achilles.entity.metadata.PropertyType.LAZY_LIST;
import static fr.doan.achilles.entity.metadata.PropertyType.LAZY_MAP;
import static fr.doan.achilles.entity.metadata.PropertyType.LAZY_SET;
import static fr.doan.achilles.entity.metadata.PropertyType.LAZY_SIMPLE;
import static fr.doan.achilles.entity.metadata.PropertyType.LIST;
import static fr.doan.achilles.entity.metadata.PropertyType.MAP;
import static fr.doan.achilles.entity.metadata.PropertyType.SET;
import static fr.doan.achilles.entity.metadata.PropertyType.SIMPLE;
import static javax.persistence.CascadeType.PERSIST;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.EQUAL;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.GREATER_THAN_EQUAL;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import mapping.entity.CompleteBean;
import mapping.entity.UserBean;
import me.prettyprint.cassandra.model.ExecutingKeyspace;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.mutation.Mutator;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import parser.entity.WideRowBean;

import com.google.common.collect.Sets;

import fr.doan.achilles.composite.factory.DynamicCompositeKeyFactory;
import fr.doan.achilles.dao.GenericCompositeDao;
import fr.doan.achilles.dao.GenericDynamicCompositeDao;
import fr.doan.achilles.entity.EntityHelper;
import fr.doan.achilles.entity.manager.CompleteBeanTestBuilder;
import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.entity.metadata.ExternalWideMapProperties;
import fr.doan.achilles.entity.metadata.JoinProperties;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.metadata.PropertyType;
import fr.doan.achilles.exception.BeanMappingException;
import fr.doan.achilles.holder.KeyValueHolder;
import fr.doan.achilles.serializer.SerializerUtils;

/**
 * EntityPersisterTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class EntityPersisterTest
{

	@InjectMocks
	private EntityPersister persister;

	@Mock
	private EntityHelper helper;

	@Mock
	private EntityLoader loader;

	@Mock
	private GenericDynamicCompositeDao<Long> dao;

	@Mock
	private EntityMeta<Long> entityMeta;

	@Mock
	private PropertyMeta<?, String> propertyMeta;

	@Mock
	private PropertyMeta<Void, String> listMeta;

	@Mock
	private PropertyMeta<Void, String> setMeta;

	@Mock
	private PropertyMeta<Integer, String> mapMeta;

	@Mock
	private ExecutingKeyspace keyspace;

	@Mock
	private DynamicCompositeKeyFactory keyFactory;

	@Mock
	private Mutator<Long> mutator;

	private Method anyMethod;

	private CompleteBean entity;

	private UserBean userBean;

	private Long id = 7856L;

	private Long joinId = 32548L;

	@Before
	public void setUp() throws Exception
	{

		anyMethod = this.getClass().getDeclaredMethod("setUp");
		entity = CompleteBeanTestBuilder.builder().id(id).name("name").age(52L)
				.addFriends("foo", "bar").addFollowers("George", "Paul").addPreference(1, "FR")
				.buid();

		userBean = new UserBean();
		userBean.setUserId(joinId);
		entity.setUser(userBean);

		PropertyMeta<Void, Long> idMeta = new PropertyMeta<Void, Long>();
		idMeta.setType(SIMPLE);

		when(entityMeta.getIdMeta()).thenReturn(idMeta);
		when(entityMeta.isWideRow()).thenReturn(false);
		when(entityMeta.getClassName()).thenReturn(CompleteBean.class.getCanonicalName());
		when(entityMeta.getEntityDao()).thenReturn(dao);

		Map<String, PropertyMeta<?, ?>> propertyMetaMap = new HashMap<String, PropertyMeta<?, ?>>();
		propertyMetaMap.put("name", propertyMeta);
		when(entityMeta.getPropertyMetas()).thenReturn(propertyMetaMap);

		when(dao.buildMutator()).thenReturn(mutator);
		when(helper.getKey(entity, idMeta)).thenReturn(id);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_batch_persist_serialVersionUID() throws Exception
	{
		Map<String, PropertyMeta<?, ?>> propertyMetaMap = new HashMap<String, PropertyMeta<?, ?>>();
		when(entityMeta.getPropertyMetas()).thenReturn(propertyMetaMap);

		ArgumentCaptor<DynamicComposite> compositeCaptor = ArgumentCaptor
				.forClass(DynamicComposite.class);

		when(helper.findSerialVersionUID(entity.getClass())).thenReturn(151L);
		doNothing().when(dao)
				.insertColumn(eq(id), compositeCaptor.capture(), eq(151L), eq(mutator));
		persister.persist(entity, entityMeta);

		verify(helper).findSerialVersionUID(entity.getClass());
		DynamicComposite captured = compositeCaptor.getValue();

		assertThat(captured.getComponent(0).getValue(SerializerUtils.BYTE_SRZ)).isEqualTo(
				PropertyType.SERIAL_VERSION_UID.flag());
		assertThat(captured.getComponent(1).getValue(SerializerUtils.STRING_SRZ)).isEqualTo(
				PropertyType.SERIAL_VERSION_UID.name());
		assertThat(captured.getComponent(2).getValue(SerializerUtils.INT_SRZ)).isEqualTo(0);
	}

	@Test(expected = BeanMappingException.class)
	public void should_exception_when_serialVersionUID_not_found_while_persisting_entity()
			throws Exception
	{
		Map<String, PropertyMeta<?, ?>> propertyMetaMap = new HashMap<String, PropertyMeta<?, ?>>();
		when(entityMeta.getPropertyMetas()).thenReturn(propertyMetaMap);

		when(helper.findSerialVersionUID(entity.getClass())).thenReturn(null);

		persister.persist(entity, entityMeta);
	}

	@Test
	public void should_batch_simple_property() throws Exception
	{

		when(propertyMeta.getPropertyName()).thenReturn("name");
		when(propertyMeta.type()).thenReturn(SIMPLE);

		DynamicComposite composite = new DynamicComposite();
		when(keyFactory.createForBatchInsert(propertyMeta, 0)).thenReturn(composite);
		when(propertyMeta.getGetter()).thenReturn(anyMethod);
		when(helper.getValueFromField(entity, anyMethod)).thenReturn("testValue");

		persister.persist(entity, entityMeta);

		verify(dao).insertColumn(id, composite, "testValue", mutator);
	}

	@Test
	public void should_persist_simple_property() throws Exception
	{
		when(propertyMeta.getPropertyName()).thenReturn("name");
		when(propertyMeta.type()).thenReturn(SIMPLE);
		DynamicComposite composite = new DynamicComposite();
		when(keyFactory.createForBatchInsert(propertyMeta, 0)).thenReturn(composite);
		when(propertyMeta.getGetter()).thenReturn(anyMethod);
		when(helper.getValueFromField(entity, anyMethod)).thenReturn("testValue");

		persister.persistSimpleProperty(entity, 1L, dao, propertyMeta);

		verify(dao).insertColumn(1L, composite, "testValue", null);
	}

	@Test
	public void should_batch_list_property() throws Exception
	{
		when(propertyMeta.getGetter()).thenReturn(anyMethod);
		when(propertyMeta.type()).thenReturn(LAZY_LIST);
		when(helper.getValueFromField(entity, anyMethod)).thenReturn(Arrays.asList("foo", "bar"));
		when(propertyMeta.getPropertyName()).thenReturn("friends");

		DynamicComposite composite = new DynamicComposite();
		when(keyFactory.createForBatchInsert(propertyMeta, 0)).thenReturn(composite);
		when(keyFactory.createForBatchInsert(propertyMeta, 1)).thenReturn(composite);

		persister.persist(entity, entityMeta);

		verify(dao).insertColumn(id, composite, "foo", mutator);
		verify(dao).insertColumn(id, composite, "bar", mutator);
	}

	@Test
	public void should_persist_list_property() throws Exception
	{
		when(dao.buildMutator()).thenReturn(mutator);
		when(propertyMeta.type()).thenReturn(LAZY_LIST);
		when(propertyMeta.getGetter()).thenReturn(anyMethod);
		when(helper.getValueFromField(entity, anyMethod)).thenReturn(Arrays.asList("foo", "bar"));
		when(propertyMeta.getPropertyName()).thenReturn("friends");

		DynamicComposite composite = new DynamicComposite();
		when(keyFactory.createForBatchInsert(propertyMeta, 0)).thenReturn(composite);
		when(keyFactory.createForBatchInsert(propertyMeta, 1)).thenReturn(composite);

		persister.persistListProperty(entity, id, dao, propertyMeta);

		verify(dao).insertColumn(id, composite, "foo", mutator);
		verify(dao).insertColumn(id, composite, "bar", mutator);
	}

	@Test
	public void should_batch_set_property() throws Exception
	{
		when(propertyMeta.getGetter()).thenReturn(anyMethod);
		when(propertyMeta.type()).thenReturn(SET);
		when(helper.getValueFromField(entity, anyMethod)).thenReturn(
				Sets.newHashSet("George", "Paul"));
		when(propertyMeta.getPropertyName()).thenReturn("followers");

		DynamicComposite composite = new DynamicComposite();
		when(keyFactory.createForBatchInsert(propertyMeta, "George".hashCode())).thenReturn(
				composite);
		when(keyFactory.createForBatchInsert(propertyMeta, "Paul".hashCode()))
				.thenReturn(composite);

		persister.persist(entity, entityMeta);

		verify(dao).insertColumn(id, composite, "George", mutator);
		verify(dao).insertColumn(id, composite, "Paul", mutator);
	}

	@Test
	public void should_persist_set_property() throws Exception
	{
		when(dao.buildMutator()).thenReturn(mutator);
		when(propertyMeta.getGetter()).thenReturn(anyMethod);
		when(propertyMeta.type()).thenReturn(SET);
		when(helper.getValueFromField(entity, anyMethod)).thenReturn(
				Sets.newHashSet("George", "Paul"));
		when(propertyMeta.getPropertyName()).thenReturn("followers");

		DynamicComposite composite = new DynamicComposite();
		when(keyFactory.createForBatchInsert(propertyMeta, "George".hashCode())).thenReturn(
				composite);
		when(keyFactory.createForBatchInsert(propertyMeta, "Paul".hashCode()))
				.thenReturn(composite);

		persister.persistSetProperty(entity, id, dao, propertyMeta);

		verify(dao).insertColumn(id, composite, "George", mutator);
		verify(dao).insertColumn(id, composite, "Paul", mutator);
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Test
	public void should_batch_map_property() throws Exception
	{
		when(propertyMeta.getGetter()).thenReturn(anyMethod);
		when(propertyMeta.type()).thenReturn(LAZY_MAP);
		Map<Integer, String> map = new HashMap<Integer, String>();
		map.put(1, "FR");
		map.put(2, "Paris");
		map.put(3, "75014");

		when(helper.getValueFromField(entity, anyMethod)).thenReturn(map);
		when(propertyMeta.getPropertyName()).thenReturn("preferences");

		DynamicComposite composite = new DynamicComposite();
		when(keyFactory.createForBatchInsert(propertyMeta, 1)).thenReturn(composite);
		when(keyFactory.createForBatchInsert(propertyMeta, 2)).thenReturn(composite);
		when(keyFactory.createForBatchInsert(propertyMeta, 3)).thenReturn(composite);

		persister.persist(entity, entityMeta);

		ArgumentCaptor<KeyValueHolder> keyValueHolderCaptor = ArgumentCaptor
				.forClass(KeyValueHolder.class);

		verify(dao, times(3)).insertColumn(eq(id), eq(composite), keyValueHolderCaptor.capture(),
				eq(mutator));

		assertThat(keyValueHolderCaptor.getAllValues()).hasSize(3);
		KeyValueHolder<Integer, String> holder1 = keyValueHolderCaptor.getAllValues().get(0);
		KeyValueHolder<Integer, String> holder2 = keyValueHolderCaptor.getAllValues().get(1);
		KeyValueHolder<Integer, String> holder3 = keyValueHolderCaptor.getAllValues().get(2);

		assertThat(holder1.getKey()).isEqualTo(1);
		assertThat(holder1.getValue()).isEqualTo("FR");

		assertThat(holder2.getKey()).isEqualTo(2);
		assertThat(holder2.getValue()).isEqualTo("Paris");

		assertThat(holder3.getKey()).isEqualTo(3);
		assertThat(holder3.getValue()).isEqualTo("75014");
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Test
	public void should_persist_map_property() throws Exception
	{
		when(dao.buildMutator()).thenReturn(mutator);
		when(propertyMeta.type()).thenReturn(LAZY_MAP);
		when(propertyMeta.getGetter()).thenReturn(anyMethod);
		Map<Integer, String> map = new HashMap<Integer, String>();
		map.put(1, "FR");
		map.put(2, "Paris");
		map.put(3, "75014");

		when(helper.getValueFromField(entity, anyMethod)).thenReturn(map);
		when(propertyMeta.getPropertyName()).thenReturn("preferences");

		DynamicComposite composite = new DynamicComposite();
		when(keyFactory.createForBatchInsert(propertyMeta, 1)).thenReturn(composite);
		when(keyFactory.createForBatchInsert(propertyMeta, 2)).thenReturn(composite);
		when(keyFactory.createForBatchInsert(propertyMeta, 3)).thenReturn(composite);

		persister.persistMapProperty(entity, id, dao, propertyMeta);

		ArgumentCaptor<KeyValueHolder> keyValueHolderCaptor = ArgumentCaptor
				.forClass(KeyValueHolder.class);

		verify(dao, times(3)).insertColumn(eq(id), eq(composite), keyValueHolderCaptor.capture(),
				eq(mutator));

		assertThat(keyValueHolderCaptor.getAllValues()).hasSize(3);
		KeyValueHolder<Integer, String> holder1 = keyValueHolderCaptor.getAllValues().get(0);
		KeyValueHolder<Integer, String> holder2 = keyValueHolderCaptor.getAllValues().get(1);
		KeyValueHolder<Integer, String> holder3 = keyValueHolderCaptor.getAllValues().get(2);

		assertThat(holder1.getKey()).isEqualTo(1);
		assertThat(holder1.getValue()).isEqualTo("FR");

		assertThat(holder2.getKey()).isEqualTo(2);
		assertThat(holder2.getValue()).isEqualTo("Paris");

		assertThat(holder3.getKey()).isEqualTo(3);
		assertThat(holder3.getValue()).isEqualTo("75014");
	}

	@SuppressWarnings(
	{
		"unchecked"
	})
	@Test
	public void should_batch_join_entity_when_cascade_persist() throws Exception
	{
		JoinProperties joinProperties = prepareJoinProperties();

		Method userGetter = CompleteBean.class.getDeclaredMethod("getUser");

		when(propertyMeta.getPropertyName()).thenReturn("name");
		when(propertyMeta.type()).thenReturn(JOIN_SIMPLE);

		when(propertyMeta.getJoinProperties()).thenReturn((JoinProperties) joinProperties);
		when(propertyMeta.getGetter()).thenReturn(userGetter);

		when(helper.getKey(userBean, joinProperties.getEntityMeta().getIdMeta()))
				.thenReturn(joinId);

		DynamicComposite composite = new DynamicComposite();
		when(keyFactory.createForBatchInsert(propertyMeta, 0)).thenReturn(composite);
		when(helper.getValueFromField(entity, userGetter)).thenReturn(userBean);

		persister.persist(entity, entityMeta);

		verify(dao).insertColumn(id, composite, joinId, mutator);
	}

	@SuppressWarnings(
	{
		"unchecked"
	})
	@Test
	public void should_not_batch_join_entity_when_no_cascade() throws Exception
	{
		JoinProperties joinProperties = prepareJoinProperties();
		joinProperties.getCascadeTypes().clear();

		Method userGetter = CompleteBean.class.getDeclaredMethod("getUser");

		when(propertyMeta.getPropertyName()).thenReturn("name");
		when(propertyMeta.type()).thenReturn(JOIN_SIMPLE);

		when(propertyMeta.getJoinProperties()).thenReturn((JoinProperties) joinProperties);
		when(propertyMeta.getGetter()).thenReturn(userGetter);

		when(helper.getKey(userBean, joinProperties.getEntityMeta().getIdMeta()))
				.thenReturn(joinId);

		when(loader.load(UserBean.class, joinId, joinProperties.getEntityMeta())).thenReturn(
				userBean);

		DynamicComposite composite = new DynamicComposite();
		when(keyFactory.createForBatchInsert(propertyMeta, 0)).thenReturn(composite);
		when(helper.getValueFromField(entity, userGetter)).thenReturn(userBean);

		persister.persist(entity, entityMeta);

		verify(dao).insertColumn(id, composite, joinId, mutator);
	}

	@SuppressWarnings(
	{
		"unchecked"
	})
	@Test
	public void should_persist_join_entity_when_cascade_persist() throws Exception
	{
		JoinProperties joinProperties = prepareJoinProperties();

		Method userGetter = CompleteBean.class.getDeclaredMethod("getUser");

		when(propertyMeta.getPropertyName()).thenReturn("name");
		when(propertyMeta.type()).thenReturn(JOIN_SIMPLE);

		when(propertyMeta.getJoinProperties()).thenReturn((JoinProperties) joinProperties);
		when(propertyMeta.getGetter()).thenReturn(userGetter);

		when(helper.getKey(userBean, joinProperties.getEntityMeta().getIdMeta()))
				.thenReturn(joinId);

		DynamicComposite composite = new DynamicComposite();
		when(keyFactory.createForBatchInsert(propertyMeta, 0)).thenReturn(composite);
		when(helper.getValueFromField(entity, userGetter)).thenReturn(userBean);

		persister.persistProperty(entity, id, dao, propertyMeta);

		verify(dao).insertColumn(id, composite, joinId, null);
	}

	@Test
	public void should_persist_simple_property_into_object() throws Exception
	{
		when(propertyMeta.type()).thenReturn(SIMPLE);
		EntityPersister spy = spy(persister);
		spy.persistProperty(entity, 1L, dao, propertyMeta);
		verify(spy).persistSimpleProperty(entity, 1L, dao, propertyMeta);

	}

	@Test
	public void should_persist_simple_lazy_property_into_object() throws Exception
	{
		when(propertyMeta.type()).thenReturn(LAZY_SIMPLE);
		EntityPersister spy = spy(persister);
		spy.persistProperty(entity, 1L, dao, propertyMeta);

		verify(spy).persistSimpleProperty(entity, 1L, dao, propertyMeta);
	}

	@Test
	public void should_persist_list_property_into_object() throws Exception
	{
		when(listMeta.type()).thenReturn(LIST);
		EntityPersister spy = spy(persister);

		when(dao.buildMutator()).thenReturn(mutator);
		spy.persistProperty(entity, 1L, dao, listMeta);

		verify(spy).persistListProperty(entity, 1L, dao, listMeta);

	}

	@Test
	public void should_persist_list_lazy_property_into_object() throws Exception
	{
		when(listMeta.type()).thenReturn(LAZY_LIST);
		EntityPersister spy = spy(persister);

		when(dao.buildMutator()).thenReturn(mutator);
		spy.persistProperty(entity, 1L, dao, listMeta);

		verify(spy).persistListProperty(entity, 1L, dao, listMeta);
	}

	@Test
	public void should_persist_set_property_into_object() throws Exception
	{
		when(setMeta.type()).thenReturn(SET);
		EntityPersister spy = spy(persister);

		when(dao.buildMutator()).thenReturn(mutator);
		spy.persistProperty(entity, 1L, dao, setMeta);

		verify(spy).persistSetProperty(entity, 1L, dao, setMeta);
	}

	@Test
	public void should_persist_set_lazy_property_into_object() throws Exception
	{
		when(setMeta.type()).thenReturn(LAZY_SET);
		EntityPersister spy = spy(persister);

		when(dao.buildMutator()).thenReturn(mutator);
		spy.persistProperty(entity, 1L, dao, setMeta);

		verify(spy).persistSetProperty(entity, 1L, dao, setMeta);
	}

	@Test
	public void should_persist_map_property_into_object() throws Exception
	{
		when(mapMeta.type()).thenReturn(MAP);
		EntityPersister spy = spy(persister);

		when(dao.buildMutator()).thenReturn(mutator);
		spy.persistProperty(entity, 1L, dao, mapMeta);

		verify(spy).persistMapProperty(entity, 1L, dao, mapMeta);

	}

	@Test
	public void should_persist_map_lazy_property_into_object() throws Exception
	{
		when(mapMeta.type()).thenReturn(LAZY_MAP);
		EntityPersister spy = spy(persister);

		when(dao.buildMutator()).thenReturn(mutator);
		spy.persistProperty(entity, 1L, dao, mapMeta);

		verify(spy).persistMapProperty(entity, 1L, dao, mapMeta);
	}

	@Test
	public void should_remove_property() throws Exception
	{
		when(propertyMeta.getPropertyName()).thenReturn("name");
		when(propertyMeta.type()).thenReturn(MAP);

		DynamicComposite start = new DynamicComposite();
		DynamicComposite end = new DynamicComposite();

		when(keyFactory.createBaseForQuery(propertyMeta, EQUAL)).thenReturn(start);
		when(keyFactory.createBaseForQuery(propertyMeta, GREATER_THAN_EQUAL)).thenReturn(end);

		persister.removeProperty(1L, dao, propertyMeta);

		verify(dao).removeColumnRange(1L, start, end);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_persist_widerow() throws Exception
	{
		EntityMeta<Long> entityMeta = mock(EntityMeta.class);
		when(entityMeta.isWideRow()).thenReturn(true);
		persister.persist(new WideRowBean(), entityMeta);

		verifyZeroInteractions(helper);

	}

	@Test
	public void should_remove_entity() throws Exception
	{
		PropertyMeta<Void, Long> idMeta = new PropertyMeta<Void, Long>();
		idMeta.setType(SIMPLE);

		Long idValue = 7856L;
		EntityMeta<Long> entityMeta = new EntityMeta<Long>();
		entityMeta.setIdMeta(idMeta);
		entityMeta.setEntityDao(dao);
		entityMeta.setPropertyMetas(new HashMap<String, PropertyMeta<?, ?>>());

		CompleteBean bean = CompleteBeanTestBuilder.builder().buid();

		when(helper.getKey(bean, idMeta)).thenReturn(idValue);

		persister.remove(bean, entityMeta);

		verify(dao).removeRow(idValue);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_remove_entity_and_external_wide_map_row() throws Exception
	{
		Long idValue = 7856L;
		EntityMeta<Long> entityMeta = new EntityMeta<Long>();
		entityMeta.setEntityDao(dao);
		HashMap<String, PropertyMeta<?, ?>> propertyMetas = new HashMap<String, PropertyMeta<?, ?>>();
		PropertyMeta<Integer, String> propertyMeta = new PropertyMeta<Integer, String>();
		ExternalWideMapProperties<Long> externalWideMapProperties = new ExternalWideMapProperties<Long>();
		GenericCompositeDao<Long, String> externalWideMapDao = mock(GenericCompositeDao.class);

		externalWideMapProperties.setExternalWideMapDao(externalWideMapDao);
		propertyMeta.setExternalWideMapProperties(externalWideMapProperties);
		propertyMetas.put("externalWideMap", propertyMeta);

		entityMeta.setPropertyMetas(propertyMetas);

		persister.removeById(idValue, entityMeta);

		verify(dao).removeRow(idValue);
		verify(externalWideMapDao).removeRow(idValue);
	}

	@Test
	public void should_remove_entity_by_id() throws Exception
	{
		Long idValue = 7856L;
		EntityMeta<Long> entityMeta = new EntityMeta<Long>();
		entityMeta.setEntityDao(dao);
		entityMeta.setPropertyMetas(new HashMap<String, PropertyMeta<?, ?>>());

		persister.removeById(idValue, entityMeta);

		verify(dao).removeRow(idValue);
	}

	private JoinProperties prepareJoinProperties() throws Exception
	{
		Method userIdGetter = UserBean.class.getDeclaredMethod("getUserId");
		PropertyMeta<Void, Long> joinIdMeta = new PropertyMeta<Void, Long>();
		joinIdMeta.setType(JOIN_SIMPLE);

		joinIdMeta.setGetter(userIdGetter);

		EntityMeta<Long> joinEntityMeta = new EntityMeta<Long>();
		joinEntityMeta.setIdMeta(joinIdMeta);
		joinEntityMeta.setEntityDao(dao);
		joinEntityMeta.setPropertyMetas(new HashMap<String, PropertyMeta<?, ?>>());

		JoinProperties joinProperties = new JoinProperties();
		joinProperties.setEntityMeta(joinEntityMeta);
		joinProperties.addCascadeType(PERSIST);

		return joinProperties;
	}
}
