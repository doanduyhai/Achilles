package info.archinnov.achilles.entity.operations;

import static info.archinnov.achilles.entity.metadata.PropertyType.EXTERNAL_WIDE_MAP;
import static info.archinnov.achilles.entity.metadata.PropertyType.JOIN_SIMPLE;
import static info.archinnov.achilles.entity.metadata.PropertyType.LAZY_LIST;
import static info.archinnov.achilles.entity.metadata.PropertyType.LAZY_MAP;
import static info.archinnov.achilles.entity.metadata.PropertyType.MAP;
import static info.archinnov.achilles.entity.metadata.PropertyType.SET;
import static info.archinnov.achilles.entity.metadata.PropertyType.SIMPLE;
import static javax.persistence.CascadeType.PERSIST;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.EQUAL;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.GREATER_THAN_EQUAL;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.composite.factory.CompositeKeyFactory;
import info.archinnov.achilles.composite.factory.DynamicCompositeKeyFactory;
import info.archinnov.achilles.dao.CounterDao;
import info.archinnov.achilles.dao.GenericCompositeDao;
import info.archinnov.achilles.dao.GenericDynamicCompositeDao;
import info.archinnov.achilles.entity.EntityHelper;
import info.archinnov.achilles.entity.manager.CompleteBeanTestBuilder;
import info.archinnov.achilles.entity.metadata.CounterProperties;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.ExternalWideMapProperties;
import info.archinnov.achilles.entity.metadata.JoinProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.type.KeyValue;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.exception.BeanMappingException;
import info.archinnov.achilles.serializer.SerializerUtils;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mapping.entity.CompleteBean;
import mapping.entity.UserBean;
import me.prettyprint.cassandra.model.ExecutingKeyspace;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.mutation.Mutator;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import parser.entity.BeanWithSimpleCounter;
import parser.entity.ColumnFamilyBean;
import testBuilders.PropertyMetaTestBuilder;

import com.google.common.collect.Sets;

/**
 * EntityPersisterTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class EntityPersisterTest
{

	@Rule
	public ExpectedException exception = ExpectedException.none();

	@InjectMocks
	private EntityPersister persister;

	@Mock
	private EntityHelper helper;

	@Mock
	private EntityLoader loader;

	@Mock
	private GenericDynamicCompositeDao<Long> entityDao;

	@Mock
	private GenericCompositeDao<Long, String> columnFamilyDao;

	@Mock
	private CounterDao counterDao;

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
	private DynamicCompositeKeyFactory dynamicCompositeKeyFactory;

	@Mock
	private CompositeKeyFactory compositeKeyFactory;

	@Mock
	private Mutator<Long> mutator;

	@Mock
	private Mutator<Long> joinMutator;

	@Mock
	private Mutator<Composite> counterMutator;

	@Captor
	ArgumentCaptor<Mutator<Long>> mutatorCaptor;

	private ObjectMapper objectMapper = new ObjectMapper();

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
		when(entityMeta.isColumnFamilyDirectMapping()).thenReturn(false);
		when(entityMeta.getClassName()).thenReturn(CompleteBean.class.getCanonicalName());
		when(entityMeta.getEntityDao()).thenReturn(entityDao);

		Map<String, PropertyMeta<?, ?>> propertyMetaMap = new HashMap<String, PropertyMeta<?, ?>>();
		propertyMetaMap.put("name", propertyMeta);
		when(entityMeta.getPropertyMetas()).thenReturn(propertyMetaMap);

		when(entityDao.buildMutator()).thenReturn(mutator);
		when(helper.getKey(entity, idMeta)).thenReturn(id);
		when(helper.findSerialVersionUID(CompleteBean.class)).thenReturn(1L);
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
		doNothing().when(entityDao).insertColumnBatch(eq(id), compositeCaptor.capture(), eq("151"),
				eq(mutator));
		persister.persist(entity, entityMeta);

		verify(helper).findSerialVersionUID(entity.getClass());
		DynamicComposite captured = compositeCaptor.getValue();

		assertThat(captured.getComponent(0).getValue(SerializerUtils.BYTE_SRZ)).isEqualTo(
				PropertyType.SERIAL_VERSION_UID.flag());
		assertThat(captured.getComponent(1).getValue(SerializerUtils.STRING_SRZ)).isEqualTo(
				PropertyType.SERIAL_VERSION_UID.name());
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
		when(dynamicCompositeKeyFactory.createForBatchInsertSingleValue(propertyMeta)).thenReturn(
				composite);
		when(propertyMeta.getGetter()).thenReturn(anyMethod);

		when(helper.getValueFromField(entity, anyMethod)).thenReturn("testValue");
		when(propertyMeta.writeValueToString("testValue")).thenReturn("testValue");

		persister.persist(entity, entityMeta);

		verify(entityDao).insertColumnBatch(eq(id), any(DynamicComposite.class), eq("1"),
				eq(mutator));
		verify(entityDao).insertColumnBatch(id, composite, "testValue", mutator);
		verify(mutator).execute();
	}

	@Test
	public void should_batch_list_property() throws Exception
	{
		when(propertyMeta.getGetter()).thenReturn(anyMethod);
		when(propertyMeta.type()).thenReturn(LAZY_LIST);
		when(helper.getValueFromField(entity, anyMethod)).thenReturn(Arrays.asList("foo", "bar"));
		when(propertyMeta.getPropertyName()).thenReturn("friends");

		DynamicComposite composite = new DynamicComposite();
		when(dynamicCompositeKeyFactory.createForBatchInsertMultiValue(propertyMeta, 0))
				.thenReturn(composite);
		when(dynamicCompositeKeyFactory.createForBatchInsertMultiValue(propertyMeta, 1))
				.thenReturn(composite);

		when(propertyMeta.writeValueToString("foo")).thenReturn("foo");
		when(propertyMeta.writeValueToString("bar")).thenReturn("bar");

		persister.persist(entity, entityMeta);

		verify(entityDao).insertColumnBatch(id, composite, "foo", mutator);
		verify(entityDao).insertColumnBatch(id, composite, "bar", mutator);
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
		when(
				dynamicCompositeKeyFactory.createForBatchInsertMultiValue(propertyMeta,
						"George".hashCode())).thenReturn(composite);
		when(
				dynamicCompositeKeyFactory.createForBatchInsertMultiValue(propertyMeta,
						"Paul".hashCode())).thenReturn(composite);

		when(propertyMeta.writeValueToString("George")).thenReturn("George");
		when(propertyMeta.writeValueToString("Paul")).thenReturn("Paul");

		persister.persist(entity, entityMeta);

		verify(entityDao).insertColumnBatch(id, composite, "George", mutator);
		verify(entityDao).insertColumnBatch(id, composite, "Paul", mutator);
	}

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
		when(dynamicCompositeKeyFactory.createForBatchInsertMultiValue(propertyMeta, 1))
				.thenReturn(composite);
		when(dynamicCompositeKeyFactory.createForBatchInsertMultiValue(propertyMeta, 2))
				.thenReturn(composite);
		when(dynamicCompositeKeyFactory.createForBatchInsertMultiValue(propertyMeta, 3))
				.thenReturn(composite);

		when(propertyMeta.writeValueToString(any(KeyValue.class))) //
				.thenReturn(writeString(new KeyValue<Integer, String>(1, "FR")), //
						writeString(new KeyValue<Integer, String>(2, "Paris")), //
						writeString(new KeyValue<Integer, String>(3, "75014")));

		persister.persist(entity, entityMeta);

		ArgumentCaptor<String> keyValueHolderCaptor = ArgumentCaptor.forClass(String.class);

		verify(entityDao, times(3)).insertColumnBatch(eq(id), eq(composite),
				keyValueHolderCaptor.capture(), eq(mutator));

		assertThat(keyValueHolderCaptor.getAllValues()).hasSize(3);

		List<String> keyValues = keyValueHolderCaptor.getAllValues();

		KeyValue<Integer, String> holder1 = readKeyValue(keyValues.get(0));
		KeyValue<Integer, String> holder2 = readKeyValue(keyValues.get(1));
		KeyValue<Integer, String> holder3 = readKeyValue(keyValues.get(2));

		assertThat(holder1.getKey()).isEqualTo(1);
		assertThat(holder1.getValue()).isEqualTo("FR");

		assertThat(holder2.getKey()).isEqualTo(2);
		assertThat(holder2.getValue()).isEqualTo("Paris");

		assertThat(holder3.getKey()).isEqualTo(3);
		assertThat(holder3.getValue()).isEqualTo("75014");
	}

	@Test
	public void should_batch_persist_simple_counter() throws Exception
	{

		BeanWithSimpleCounter bean = new BeanWithSimpleCounter();
		Method getter = BeanWithSimpleCounter.class.getDeclaredMethod("getCounter");

		PropertyMeta<Void, Long> idMeta = PropertyMetaTestBuilder.valueClass(Long.class).build();

		PropertyMeta<Void, Long> counterMeta = PropertyMetaTestBuilder//
				.of(BeanWithSimpleCounter.class, Void.class, Long.class) //
				.field("counter") //
				.accesors() //
				.counterDao(counterDao) //
				.fqcn("fqcn") //
				.counterIdMeta(idMeta)//
				.type(PropertyType.COUNTER) //
				.build();

		Composite keyComp = new Composite();
		DynamicComposite comp = new DynamicComposite();
		when(compositeKeyFactory.createKeyForCounter("fqcn", 11L, idMeta)).thenReturn(keyComp);
		when(dynamicCompositeKeyFactory.createForBatchInsertSingleValue(counterMeta)).thenReturn(
				comp);

		EntityMeta<Long> entityMeta = new EntityMeta<Long>();
		entityMeta.setIdMeta(idMeta);
		entityMeta.setEntityDao(entityDao);
		entityMeta.setHasCounter(true);
		entityMeta.setCounterDao(counterDao);

		Map<String, PropertyMeta<?, ?>> map = new HashMap<String, PropertyMeta<?, ?>>();
		map.put("counter", counterMeta);

		entityMeta.setPropertyMetas(map);

		when(helper.getValueFromField(bean, getter)).thenReturn(150L);
		when(counterDao.buildMutator()).thenReturn(counterMutator);

		when(helper.getKey(bean, idMeta)).thenReturn(11L);
		when(helper.findSerialVersionUID(bean.getClass())).thenReturn(11L);

		persister.persist(bean, entityMeta, mutator);

		verify(helper).getValueFromField(bean, getter);
		verify(counterDao).insertCounter(keyComp, comp, 150L, counterMutator);
		verify(counterMutator).execute();

		assertThat(EntityPersister.counterMutatorTL.get()).isNull();
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void should_batch_join_entity_when_cascade_persist() throws Exception
	{
		JoinProperties joinProperties = prepareJoinProperties();

		Method userGetter = CompleteBean.class.getDeclaredMethod("getUser");

		when(propertyMeta.getPropertyName()).thenReturn("name");
		when(propertyMeta.type()).thenReturn(JOIN_SIMPLE);

		when(propertyMeta.getJoinProperties()).thenReturn(joinProperties);
		when((PropertyMeta) propertyMeta.joinIdMeta()).thenReturn(
				joinProperties.getEntityMeta().getIdMeta());
		when(propertyMeta.getGetter()).thenReturn(userGetter);

		when((Long) helper.getKey(userBean, joinProperties.getEntityMeta().getIdMeta()))
				.thenReturn(joinId);
		when(helper.unproxy(userBean)).thenReturn(userBean);
		DynamicComposite composite = new DynamicComposite();
		when(dynamicCompositeKeyFactory.createForBatchInsertSingleValue(propertyMeta)).thenReturn(
				composite);
		when(helper.getValueFromField(entity, userGetter)).thenReturn(userBean);

		persister.persist(entity, entityMeta);

		verify(entityDao).insertColumnBatch(id, composite, joinId.toString(), mutator);
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Test
	public void should_not_batch_join_entity_when_no_cascade() throws Exception
	{
		JoinProperties joinProperties = prepareJoinProperties();
		joinProperties.getCascadeTypes().clear();

		Method userGetter = CompleteBean.class.getDeclaredMethod("getUser");

		when(propertyMeta.getPropertyName()).thenReturn("name");
		when(propertyMeta.type()).thenReturn(JOIN_SIMPLE);

		when(propertyMeta.getJoinProperties()).thenReturn(joinProperties);
		when((PropertyMeta) propertyMeta.joinIdMeta()).thenReturn(
				joinProperties.getEntityMeta().getIdMeta());
		when(propertyMeta.getGetter()).thenReturn(userGetter);

		when((Long) helper.getKey(userBean, joinProperties.getEntityMeta().getIdMeta()))
				.thenReturn(joinId);

		when(loader.load(UserBean.class, joinId, (EntityMeta<Long>) joinProperties.getEntityMeta()))
				.thenReturn(userBean);

		DynamicComposite composite = new DynamicComposite();
		when(dynamicCompositeKeyFactory.createForBatchInsertSingleValue(propertyMeta)).thenReturn(
				composite);
		when(helper.getValueFromField(entity, userGetter)).thenReturn(userBean);

		persister.persist(entity, entityMeta);

		verify(entityDao).insertColumnBatch(id, composite, joinId.toString(), mutator);
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void should_persist_join_entity_when_cascade_persist() throws Exception
	{
		JoinProperties joinProperties = prepareJoinProperties();

		Method userGetter = CompleteBean.class.getDeclaredMethod("getUser");

		when(propertyMeta.getPropertyName()).thenReturn("name");
		when(propertyMeta.type()).thenReturn(JOIN_SIMPLE);

		when(propertyMeta.getJoinProperties()).thenReturn(joinProperties);
		when((PropertyMeta) propertyMeta.joinIdMeta()).thenReturn(
				joinProperties.getEntityMeta().getIdMeta());
		when(propertyMeta.getGetter()).thenReturn(userGetter);

		when(entityDao.buildMutator()).thenReturn(mutator);

		when((Long) helper.getKey(userBean, joinProperties.getEntityMeta().getIdMeta()))
				.thenReturn(joinId);

		DynamicComposite composite = new DynamicComposite();
		when(dynamicCompositeKeyFactory.createForBatchInsertSingleValue(propertyMeta)).thenReturn(
				composite);
		when(helper.getValueFromField(entity, userGetter)).thenReturn(userBean);
		when(helper.unproxy(userBean)).thenReturn(userBean);
		persister.persistProperty(entity, id, entityDao, propertyMeta, mutator);

		verify(entityDao, atLeastOnce()).insertColumnBatch(id, composite, joinId.toString(),
				mutator);
		verify(entityDao, atLeastOnce()).insertColumnBatch(eq(joinId), any(DynamicComposite.class),
				eq("0"), eq(mutator));
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void should_cascade_persist() throws Exception
	{
		JoinProperties joinProperties = prepareJoinProperties();
		when((PropertyMeta) propertyMeta.joinIdMeta()).thenReturn(
				joinProperties.getEntityMeta().getIdMeta());
		when(entityDao.buildMutator()).thenReturn(joinMutator);

		when((Long) helper.getKey(userBean, joinProperties.getEntityMeta().getIdMeta()))
				.thenReturn(joinId);
		when(helper.unproxy(userBean)).thenReturn(userBean);
		persister.cascadePersistOrEnsureExists(userBean, joinProperties);

		verify(joinMutator).execute();
	}

	@Test
	public void should_check_for_join_entity_when_persisting() throws Exception
	{
		JoinProperties joinProperties = prepareJoinProperties();
		joinProperties.getCascadeTypes().clear();

		when((Long) helper.getKey(userBean, joinProperties.getEntityMeta().getIdMeta()))
				.thenReturn(joinId);
		when(loader.loadVersionSerialUID(joinId, entityDao)).thenReturn(123L);

		persister.cascadePersistOrEnsureExists(userBean, joinProperties);

	}

	@Test
	public void should_exception_when_no_join_entity_found_when_persisting() throws Exception
	{
		JoinProperties joinProperties = prepareJoinProperties();
		joinProperties.getCascadeTypes().clear();
		when(loader.loadVersionSerialUID(joinId, entityDao)).thenReturn(null);

		when((Long) helper.getKey(userBean, joinProperties.getEntityMeta().getIdMeta()))
				.thenReturn(joinId);

		exception.expect(AchillesException.class);
		exception
				.expectMessage("The entity '"
						+ UserBean.class.getCanonicalName()
						+ "' with id '"
						+ joinId
						+ "' cannot be found. Maybe you should persist it first or enable CascadeType.PERSIST/CascadeType.ALL");

		persister.cascadePersistOrEnsureExists(userBean, joinProperties);

	}

	@Test
	public void should_remove_property() throws Exception
	{
		when(propertyMeta.getPropertyName()).thenReturn("name");
		when(propertyMeta.type()).thenReturn(MAP);

		DynamicComposite start = new DynamicComposite();
		DynamicComposite end = new DynamicComposite();

		when(dynamicCompositeKeyFactory.createBaseForQuery(propertyMeta, EQUAL)).thenReturn(start);
		when(dynamicCompositeKeyFactory.createBaseForQuery(propertyMeta, GREATER_THAN_EQUAL))
				.thenReturn(end);

		persister.removeProperty(1L, entityDao, propertyMeta);

		verify(entityDao).removeColumnRange(1L, start, end);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_persist_column_family() throws Exception
	{
		EntityMeta<Long> entityMeta = mock(EntityMeta.class);
		when(entityMeta.isColumnFamilyDirectMapping()).thenReturn(true);
		persister.persist(new ColumnFamilyBean(), entityMeta);

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
		entityMeta.setEntityDao(entityDao);
		entityMeta.setPropertyMetas(new HashMap<String, PropertyMeta<?, ?>>());

		CompleteBean bean = CompleteBeanTestBuilder.builder().buid();

		when(helper.getKey(bean, idMeta)).thenReturn(idValue);

		persister.remove(bean, entityMeta);

		verify(entityDao).removeRow(idValue);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_remove_entity_and_external_wide_map_row() throws Exception
	{
		Long idValue = 7856L;
		EntityMeta<Long> entityMeta = new EntityMeta<Long>();
		entityMeta.setEntityDao(entityDao);
		HashMap<String, PropertyMeta<?, ?>> propertyMetas = new HashMap<String, PropertyMeta<?, ?>>();
		PropertyMeta<Integer, String> propertyMeta = new PropertyMeta<Integer, String>();
		propertyMeta.setType(EXTERNAL_WIDE_MAP);
		ExternalWideMapProperties<Long> externalWideMapProperties = new ExternalWideMapProperties<Long>();
		GenericCompositeDao<Long, String> externalWideMapDao = mock(GenericCompositeDao.class);

		externalWideMapProperties.setExternalWideMapDao(externalWideMapDao);
		propertyMeta.setExternalWideMapProperties(externalWideMapProperties);
		propertyMetas.put("externalWideMap", propertyMeta);

		entityMeta.setPropertyMetas(propertyMetas);

		persister.removeById(idValue, entityMeta);

		verify(entityDao).removeRow(idValue);
		verify(externalWideMapDao).removeRow(idValue);
	}

	@Test
	public void should_remove_entity_by_id() throws Exception
	{
		Long idValue = 7856L;
		EntityMeta<Long> entityMeta = new EntityMeta<Long>();
		entityMeta.setEntityDao(entityDao);
		entityMeta.setPropertyMetas(new HashMap<String, PropertyMeta<?, ?>>());

		persister.removeById(idValue, entityMeta);

		verify(entityDao).removeRow(idValue);
	}

	@Test
	public void should_remove_entity_and_counter() throws Exception
	{
		Long idValue = 7856L;
		EntityMeta<Long> entityMeta = new EntityMeta<Long>();
		entityMeta.setEntityDao(entityDao);
		HashMap<String, PropertyMeta<?, ?>> propertyMetas = new HashMap<String, PropertyMeta<?, ?>>();
		PropertyMeta<Integer, Long> propertyMeta = new PropertyMeta<Integer, Long>();
		PropertyMeta<Void, Long> idMeta = PropertyMetaTestBuilder.valueClass(Long.class).build();
		CounterProperties counterProperties = new CounterProperties("fqcn", counterDao, idMeta);
		propertyMeta.setCounterProperties(counterProperties);
		propertyMeta.setType(PropertyType.COUNTER);

		propertyMetas.put("counter", propertyMeta);
		entityMeta.setPropertyMetas(propertyMetas);

		Composite keyComp = new Composite();
		when(compositeKeyFactory.createKeyForCounter("fqcn", idValue, idMeta)).thenReturn(keyComp);

		DynamicComposite comp = new DynamicComposite();
		when(dynamicCompositeKeyFactory.createForBatchInsertSingleValue(propertyMeta)).thenReturn(
				comp);

		persister.removeById(idValue, entityMeta);

		verify(counterDao).removeCounter(keyComp, comp);
	}

	@Test
	public void should_remove_row_from_direct_cf_mapping() throws Exception
	{
		EntityMeta<Long> entityMeta = new EntityMeta<Long>();
		entityMeta.setColumnFamilyDirectMapping(true);
		entityMeta.setColumnFamilyDao(columnFamilyDao);
		entityMeta.setPropertyMetas(new HashMap<String, PropertyMeta<?, ?>>());

		persister.removeById(id, entityMeta);

		verify(columnFamilyDao).removeRow(id);
	}

	private JoinProperties prepareJoinProperties() throws Exception
	{
		Method userIdGetter = UserBean.class.getDeclaredMethod("getUserId");
		PropertyMeta<Void, Long> joinIdMeta = new PropertyMeta<Void, Long>();
		joinIdMeta.setType(JOIN_SIMPLE);
		joinIdMeta.setGetter(userIdGetter);
		joinIdMeta.setObjectMapper(objectMapper);

		EntityMeta<Long> joinEntityMeta = new EntityMeta<Long>();
		joinEntityMeta.setIdMeta(joinIdMeta);
		joinEntityMeta.setEntityDao(entityDao);
		joinEntityMeta.setClassName(UserBean.class.getCanonicalName());
		joinEntityMeta.setPropertyMetas(new HashMap<String, PropertyMeta<?, ?>>());

		JoinProperties joinProperties = new JoinProperties();
		joinProperties.setEntityMeta(joinEntityMeta);
		joinProperties.addCascadeType(PERSIST);

		return joinProperties;
	}

	@SuppressWarnings("unchecked")
	private KeyValue<Integer, String> readKeyValue(String value) throws Exception
	{
		return objectMapper.readValue(value, KeyValue.class);
	}

	private String writeString(Object value) throws Exception
	{
		return objectMapper.writeValueAsString(value);
	}
}
