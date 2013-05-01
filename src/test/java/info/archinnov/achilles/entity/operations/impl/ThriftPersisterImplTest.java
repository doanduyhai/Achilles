package info.archinnov.achilles.entity.operations.impl;

import static info.archinnov.achilles.entity.metadata.PropertyType.*;
import static info.archinnov.achilles.entity.type.ConsistencyLevel.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.composite.factory.CompositeFactory;
import info.archinnov.achilles.consistency.AchillesConfigurableConsistencyLevelPolicy;
import info.archinnov.achilles.dao.CounterDao;
import info.archinnov.achilles.dao.GenericColumnFamilyDao;
import info.archinnov.achilles.dao.GenericEntityDao;
import info.archinnov.achilles.dao.Pair;
import info.archinnov.achilles.entity.EntityIntrospector;
import info.archinnov.achilles.entity.context.ImmediateFlushContext;
import info.archinnov.achilles.entity.context.PersistenceContext;
import info.archinnov.achilles.entity.context.PersistenceContextTestBuilder;
import info.archinnov.achilles.entity.manager.CompleteBeanTestBuilder;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.operations.EntityPersister;
import info.archinnov.achilles.entity.operations.EntityProxifier;
import info.archinnov.achilles.entity.type.ConsistencyLevel;
import info.archinnov.achilles.entity.type.Counter;
import info.archinnov.achilles.entity.type.KeyValue;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.serializer.SerializerUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import mapping.entity.CompleteBean;
import mapping.entity.UserBean;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.mutation.Mutator;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import testBuilders.PropertyMetaTestBuilder;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * ThriftPersisterImplTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class ThriftPersisterImplTest
{
	@Rule
	public ExpectedException exception = ExpectedException.none();

	@InjectMocks
	private ThriftPersisterImpl thriftPersister;

	@Mock
	private EntityPersister persister;

	@Mock
	private EntityIntrospector introspector;

	@Mock
	private EntityProxifier proxifier;

	@Mock
	private GenericEntityDao<Long> entityDao;

	@Mock
	private GenericColumnFamilyDao<Long, String> columnFamilyDao;

	@Mock
	private EntityMeta<Long> entityMeta;

	@Mock
	private CompositeFactory compositeFactory;

	@Mock
	private CounterDao counterDao;

	@Mock
	private Mutator<Long> mutator;

	@Mock
	private Mutator<Long> cfMutator;

	@Mock
	private Mutator<Composite> counterMutator;

	@Mock
	private AchillesConfigurableConsistencyLevelPolicy policy;

	@Mock
	private Map<String, GenericEntityDao<?>> entityDaosMap;

	@Mock
	private Map<String, GenericColumnFamilyDao<?, ?>> columnFamilyDaosMap;

	@Mock
	private ImmediateFlushContext immediateFlushContext;

	@Captor
	ArgumentCaptor<Composite> compositeCaptor;

	private ObjectMapper objectMapper = new ObjectMapper();

	private CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().buid();

	private PersistenceContext<Long> context;

	@SuppressWarnings(
	{
		"rawtypes"
	})
	@Before
	public void setUp()
	{
		context = PersistenceContextTestBuilder
				.context(entityMeta, counterDao, policy, CompleteBean.class, entity.getId())
				.entity(entity) //
				.immediateFlushContext(immediateFlushContext) //
				.entityDao(entityDao) //
				.columnFamilyDao(columnFamilyDao) //
				.columnFamilyDaosMap(columnFamilyDaosMap) //
				.entityDaosMap(entityDaosMap) //
				.build();
		when(entityMeta.getColumnFamilyName()).thenReturn("cf");
		when((Mutator) immediateFlushContext.getEntityMutator("cf")).thenReturn(mutator);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_batch_persist_serialVersionUID() throws Exception
	{
		when(introspector.findSerialVersionUID(entity.getClass())).thenReturn(151L);

		context = PersistenceContextTestBuilder
				.context(entityMeta, counterDao, policy, CompleteBean.class, entity.getId())
				.entity(entity) //
				.immediateFlushContext(immediateFlushContext).entityDao(entityDao) //
				.build();

		thriftPersister.batchPersistVersionSerialUID(context);

		verify(entityDao).insertColumnBatch(eq(entity.getId()), compositeCaptor.capture(),
				eq("151"), eq(mutator));

		Composite captured = compositeCaptor.getValue();

		assertThat(captured.getComponent(0).getValue(SerializerUtils.BYTE_SRZ)).isEqualTo(
				PropertyType.SERIAL_VERSION_UID.flag());
		assertThat(captured.getComponent(1).getValue(SerializerUtils.STRING_SRZ)).isEqualTo(
				PropertyType.SERIAL_VERSION_UID.name());
	}

	@Test
	public void should_exception_when_serialVersionUID_not_found_while_persisting_entity()
			throws Exception
	{
		when(introspector.findSerialVersionUID(entity.getClass())).thenReturn(null);

		exception.expect(AchillesException.class);
		exception.expectMessage("Cannot find 'serialVersionUID' field for entity class '"
				+ CompleteBean.class.getCanonicalName() + "'");
		thriftPersister.batchPersistVersionSerialUID(context);
	}

	@Test
	public void should_batch_simple_property() throws Exception
	{

		PropertyMeta<Void, String> propertyMeta = PropertyMetaTestBuilder //
				.completeBean(Void.class, String.class) //
				.field("name") //
				.accesors() //
				.build();

		Composite comp = new Composite();
		when(compositeFactory.createForBatchInsertSingleValue(propertyMeta)).thenReturn(comp);

		when(introspector.getValueFromField(entity, propertyMeta.getGetter())).thenReturn(
				"testValue");

		thriftPersister.batchPersistSimpleProperty(context, propertyMeta);

		verify(entityDao).insertColumnBatch(entity.getId(), comp, "testValue", mutator);

	}

	@Test
	public void should_batch_list_property() throws Exception
	{
		PropertyMeta<Void, String> propertyMeta = PropertyMetaTestBuilder //
				.completeBean(Void.class, String.class) //
				.field("friends") //
				.accesors() //
				.build();

		Composite comp1 = new Composite();
		Composite comp2 = new Composite();
		when(compositeFactory.createForBatchInsertMultiValue(propertyMeta, 0)).thenReturn(comp1);
		when(compositeFactory.createForBatchInsertMultiValue(propertyMeta, 1)).thenReturn(comp2);

		thriftPersister.batchPersistList(Arrays.asList("foo", "bar"), context, propertyMeta);

		InOrder inOrder = inOrder(entityDao);
		inOrder.verify(entityDao).insertColumnBatch(entity.getId(), comp1, "foo", mutator);
		inOrder.verify(entityDao).insertColumnBatch(entity.getId(), comp2, "bar", mutator);

	}

	@Test
	public void should_batch_set_property() throws Exception
	{
		PropertyMeta<Void, String> propertyMeta = PropertyMetaTestBuilder //
				.completeBean(Void.class, String.class) //
				.field("followers") //
				.accesors() //
				.build();

		Composite comp1 = new Composite();
		Composite comp2 = new Composite();
		when(compositeFactory.createForBatchInsertMultiValue(propertyMeta, "John".hashCode()))
				.thenReturn(comp1);
		when(compositeFactory.createForBatchInsertMultiValue(propertyMeta, "Helen".hashCode()))
				.thenReturn(comp2);

		Set<String> followers = ImmutableSet.of("John", "Helen");
		thriftPersister.batchPersistSet(followers, context, propertyMeta);

		InOrder inOrder = inOrder(entityDao);
		inOrder.verify(entityDao).insertColumnBatch(entity.getId(), comp1, "John", mutator);
		inOrder.verify(entityDao).insertColumnBatch(entity.getId(), comp2, "Helen", mutator);

	}

	@Test
	public void should_batch_map_property() throws Exception
	{
		PropertyMeta<Integer, String> propertyMeta = PropertyMetaTestBuilder //
				.completeBean(Integer.class, String.class) //
				.field("preferences") //
				.type(MAP) //
				.accesors() //
				.build();

		Map<Integer, String> map = new HashMap<Integer, String>();
		map.put(1, "FR");
		map.put(2, "Paris");
		map.put(3, "75014");

		Composite comp1 = new Composite();
		Composite comp2 = new Composite();
		Composite comp3 = new Composite();
		when(compositeFactory.createForBatchInsertMultiValue(propertyMeta, 1)).thenReturn(comp1);
		when(compositeFactory.createForBatchInsertMultiValue(propertyMeta, 2)).thenReturn(comp2);
		when(compositeFactory.createForBatchInsertMultiValue(propertyMeta, 3)).thenReturn(comp3);

		thriftPersister.batchPersistMap(map, context, propertyMeta);

		ArgumentCaptor<String> keyValueHolderCaptor = ArgumentCaptor.forClass(String.class);

		verify(entityDao, times(3)).insertColumnBatch(eq(entity.getId()), any(Composite.class),
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

	@SuppressWarnings(
	{
			"rawtypes",
			"unchecked"
	})
	@Test
	public void should_batch_persist_join_entity() throws Exception
	{
		Long joinId = 154654L;
		PropertyMeta<Void, Long> joinIdMeta = PropertyMetaTestBuilder //
				.of(UserBean.class, Void.class, Long.class) //
				.field("userId")//
				.type(SIMPLE) //
				.accesors() //
				.build();

		EntityMeta<Long> joinMeta = new EntityMeta<Long>();
		joinMeta.setIdMeta(joinIdMeta);

		PropertyMeta<Void, UserBean> propertyMeta = PropertyMetaTestBuilder //
				.completeBean(Void.class, UserBean.class) //
				.field("user") //
				.type(JOIN_SIMPLE) //
				.joinMeta(joinMeta)//
				.accesors() //
				.build();

		UserBean user = new UserBean();
		user.setUserId(joinId);

		when(introspector.getKey(user, joinIdMeta)).thenReturn(joinId);
		Composite comp = new Composite();
		when(compositeFactory.createForBatchInsertSingleValue(propertyMeta)).thenReturn(comp);

		when(proxifier.unproxy(user)).thenReturn(user);

		thriftPersister.batchPersistJoinEntity(context, propertyMeta, user, persister);

		verify(entityDao).insertColumnBatch(entity.getId(), comp, joinId.toString(), mutator);

		ArgumentCaptor<PersistenceContext> contextCaptor = ArgumentCaptor
				.forClass(PersistenceContext.class);
		verify(persister).cascadePersistOrEnsureExists(contextCaptor.capture(), eq(user),
				eq(propertyMeta.getJoinProperties()));
		assertThat(contextCaptor.getValue().getEntity()).isSameAs(user);
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Test
	public void should_batch_persist_join_collection() throws Exception
	{
		Long joinId1 = 54351L, joinId2 = 4653L;
		PropertyMeta<Void, Long> joinIdMeta = PropertyMetaTestBuilder //
				.of(UserBean.class, Void.class, Long.class) //
				.field("userId")//
				.type(SIMPLE) //
				.accesors() //
				.build();

		EntityMeta<Long> joinMeta = new EntityMeta<Long>();
		joinMeta.setIdMeta(joinIdMeta);

		PropertyMeta<Void, UserBean> propertyMeta = PropertyMetaTestBuilder //
				.completeBean(Void.class, UserBean.class) //
				.field("user") //
				.joinMeta(joinMeta)//
				.build();

		UserBean user1 = new UserBean(), user2 = new UserBean();
		user1.setUserId(joinId1);
		user2.setUserId(joinId2);

		Composite comp1 = new Composite();
		Composite comp2 = new Composite();
		when(compositeFactory.createForBatchInsertMultiValue(propertyMeta, 0)).thenReturn(comp1);
		when(compositeFactory.createForBatchInsertMultiValue(propertyMeta, 1)).thenReturn(comp2);
		when(introspector.getValueFromField(user1, joinIdMeta.getGetter())).thenReturn(joinId1);
		when(introspector.getValueFromField(user2, joinIdMeta.getGetter())).thenReturn(joinId2);

		when(proxifier.unproxy(user1)).thenReturn(user1);
		when(proxifier.unproxy(user2)).thenReturn(user2);

		thriftPersister.batchPersistJoinCollection(context, propertyMeta,
				Arrays.asList(user1, user2), persister);

		verify(entityDao).insertColumnBatch(entity.getId(), comp1, joinId1.toString(), mutator);
		verify(entityDao).insertColumnBatch(entity.getId(), comp2, joinId2.toString(), mutator);

		ArgumentCaptor<PersistenceContext> contextCaptor = ArgumentCaptor
				.forClass(PersistenceContext.class);
		verify(persister).cascadePersistOrEnsureExists(contextCaptor.capture(), eq(user1),
				eq(propertyMeta.getJoinProperties()));
		verify(persister).cascadePersistOrEnsureExists(contextCaptor.capture(), eq(user2),
				eq(propertyMeta.getJoinProperties()));

		List<PersistenceContext> contextes = contextCaptor.getAllValues();

		assertThat(contextes.get(0).getEntity()).isSameAs(user1);
		assertThat(contextes.get(1).getEntity()).isSameAs(user2);

	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Test
	public void should_batch_persist_join_map() throws Exception
	{
		Long joinId1 = 54351L, joinId2 = 4653L;
		PropertyMeta<Void, Long> joinIdMeta = PropertyMetaTestBuilder //
				.of(UserBean.class, Void.class, Long.class) //
				.field("userId")//
				.type(SIMPLE) //
				.accesors() //
				.build();

		EntityMeta<Long> joinMeta = new EntityMeta<Long>();
		joinMeta.setIdMeta(joinIdMeta);

		PropertyMeta<Integer, UserBean> propertyMeta = PropertyMetaTestBuilder //
				.completeBean(Integer.class, UserBean.class) //
				.joinMeta(joinMeta)//
				.build();

		UserBean user1 = new UserBean(), user2 = new UserBean();
		user1.setUserId(joinId1);
		user2.setUserId(joinId2);

		Map<Integer, UserBean> joinMap = ImmutableMap.of(1, user1, 2, user2);
		KeyValue<Integer, String> kv1 = new KeyValue<Integer, String>(1, joinId1.toString());
		KeyValue<Integer, String> kv2 = new KeyValue<Integer, String>(2, joinId2.toString());

		Composite comp1 = new Composite();
		Composite comp2 = new Composite();

		when(compositeFactory.createForBatchInsertMultiValue(propertyMeta, 1)).thenReturn(comp1);
		when(compositeFactory.createForBatchInsertMultiValue(propertyMeta, 2)).thenReturn(comp2);
		when(introspector.getValueFromField(user1, joinIdMeta.getGetter())).thenReturn(joinId1);
		when(introspector.getValueFromField(user2, joinIdMeta.getGetter())).thenReturn(joinId2);

		when(proxifier.unproxy(user1)).thenReturn(user1);
		when(proxifier.unproxy(user2)).thenReturn(user2);

		thriftPersister.batchPersistJoinMap(context, propertyMeta, joinMap, persister);

		verify(entityDao).insertColumnBatch(entity.getId(), comp1, writeString(kv1), mutator);
		verify(entityDao).insertColumnBatch(entity.getId(), comp2, writeString(kv2), mutator);

		ArgumentCaptor<PersistenceContext> contextCaptor = ArgumentCaptor
				.forClass(PersistenceContext.class);

		verify(persister).cascadePersistOrEnsureExists(contextCaptor.capture(), eq(user1),
				eq(propertyMeta.getJoinProperties()));
		verify(persister).cascadePersistOrEnsureExists(contextCaptor.capture(), eq(user2),
				eq(propertyMeta.getJoinProperties()));

		List<PersistenceContext> contextes = contextCaptor.getAllValues();

		assertThat(contextes.get(0).getEntity()).isSameAs(user1);
		assertThat(contextes.get(1).getEntity()).isSameAs(user2);
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void should_remove_direct_cf_mapping() throws Exception
	{
		when(entityMeta.isColumnFamilyDirectMapping()).thenReturn(true);
		when((Mutator) immediateFlushContext.getColumnFamilyMutator("cf")).thenReturn(mutator);
		thriftPersister.remove(context);
		verify(columnFamilyDao).removeRowBatch(entity.getId(), mutator);
	}

	@SuppressWarnings(
	{
			"rawtypes",
			"unchecked"
	})
	@Test
	public void should_remove_entity_having_external_wide_map() throws Exception
	{
		when(entityMeta.isColumnFamilyDirectMapping()).thenReturn(false);
		PropertyMeta<UUID, String> propertyMeta = PropertyMetaTestBuilder //
				.completeBean(UUID.class, String.class) //
				.field("geoPositions")//
				.type(PropertyType.WIDE_MAP) //
				.externalCf("external_cf") //
				.idSerializer(SerializerUtils.LONG_SRZ) //
				.accesors() //
				.build();

		Map<String, PropertyMeta<UUID, String>> propertyMetas = ImmutableMap.of("geoPositions",
				propertyMeta);
		when((Map) entityMeta.getPropertyMetas()).thenReturn(propertyMetas);
		when((GenericColumnFamilyDao<Long, String>) columnFamilyDaosMap.get("external_cf"))
				.thenReturn(columnFamilyDao);
		when((Mutator) immediateFlushContext.getColumnFamilyMutator("external_cf")).thenReturn(
				cfMutator);

		thriftPersister.remove(context);
		verify(entityDao).removeRowBatch(entity.getId(), mutator);
		verify(columnFamilyDao).removeRowBatch(entity.getId(), cfMutator);
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void should_remove_entity_having_simple_counter() throws Exception
	{
		String fqcn = CompleteBean.class.getCanonicalName();

		PropertyMeta<Void, Long> counterIdMeta = PropertyMetaTestBuilder //
				.completeBean(Void.class, Long.class) //
				.field("id") //
				.accesors() //
				.build();
		PropertyMeta<Void, Counter> propertyMeta = PropertyMetaTestBuilder //
				.completeBean(Void.class, Counter.class) //
				.field("count") //
				.type(PropertyType.COUNTER) //
				.accesors() //
				.counterIdMeta(counterIdMeta) //
				.fqcn(fqcn) //
				.consistencyLevels(new Pair<ConsistencyLevel, ConsistencyLevel>(ONE, ALL)) //
				.build();
		when(entityMeta.isColumnFamilyDirectMapping()).thenReturn(false);
		Map<String, PropertyMeta<Void, Counter>> propertyMetas = ImmutableMap.of("geoPositions",
				propertyMeta);
		when((Map) entityMeta.getPropertyMetas()).thenReturn(propertyMetas);

		Composite keyComp = new Composite();
		Composite comp = new Composite();
		when(compositeFactory.createKeyForCounter(fqcn, entity.getId(), counterIdMeta)).thenReturn(
				keyComp);
		when(compositeFactory.createForBatchInsertSingleCounter(propertyMeta)).thenReturn(comp);
		when(immediateFlushContext.getCounterMutator()).thenReturn(counterMutator);

		thriftPersister.remove(context);

		verify(counterDao).removeCounterBatch(keyComp, comp, counterMutator);

	}

	@SuppressWarnings("rawtypes")
	@Test
	public void should_remove_entity_having_widemap_counter() throws Exception
	{
		String fqcn = CompleteBean.class.getCanonicalName();

		PropertyMeta<Void, Long> counterIdMeta = PropertyMetaTestBuilder //
				.completeBean(Void.class, Long.class) //
				.field("id") //
				.accesors() //
				.build();
		PropertyMeta<String, Counter> propertyMeta = PropertyMetaTestBuilder //
				.completeBean(String.class, Counter.class) //
				.field("popularTopics") //
				.type(PropertyType.COUNTER_WIDE_MAP) //
				.accesors() //
				.counterIdMeta(counterIdMeta) //
				.fqcn(fqcn) //
				.consistencyLevels(new Pair<ConsistencyLevel, ConsistencyLevel>(ONE, ALL)) //
				.build();

		when(entityMeta.isColumnFamilyDirectMapping()).thenReturn(false);
		Map<String, PropertyMeta<String, Counter>> propertyMetas = ImmutableMap.of("geoPositions",
				propertyMeta);
		when((Map) entityMeta.getPropertyMetas()).thenReturn(propertyMetas);

		Composite keyComp = new Composite();
		when(compositeFactory.createKeyForCounter(fqcn, entity.getId(), counterIdMeta)).thenReturn(
				keyComp);
		when(immediateFlushContext.getCounterMutator()).thenReturn(counterMutator);

		thriftPersister.remove(context);

		verify(counterDao).removeCounterRowBatch(keyComp, counterMutator);

	}

	@Test
	public void should_batch_remove_property() throws Exception
	{
		PropertyMeta<Void, String> propertyMeta = PropertyMetaTestBuilder //
				.completeBean(Void.class, String.class) //
				.field("name") //
				.type(PropertyType.SIMPLE) //
				.accesors() //
				.build();

		Composite start = new Composite(), end = new Composite();
		when(compositeFactory.createBaseForQuery(propertyMeta, ComponentEquality.EQUAL))
				.thenReturn(start);
		when(
				compositeFactory.createBaseForQuery(propertyMeta,
						ComponentEquality.GREATER_THAN_EQUAL)).thenReturn(end);

		thriftPersister.removePropertyBatch(context, propertyMeta);

		verify(entityDao).removeColumnRangeBatch(entity.getId(), start, end, mutator);
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
