package fr.doan.achilles.entity.operations;

import static fr.doan.achilles.entity.metadata.PropertyType.LAZY_LIST;
import static fr.doan.achilles.entity.metadata.PropertyType.LAZY_MAP;
import static fr.doan.achilles.entity.metadata.PropertyType.LAZY_SET;
import static fr.doan.achilles.entity.metadata.PropertyType.LAZY_SIMPLE;
import static fr.doan.achilles.entity.metadata.PropertyType.LIST;
import static fr.doan.achilles.entity.metadata.PropertyType.MAP;
import static fr.doan.achilles.entity.metadata.PropertyType.SET;
import static fr.doan.achilles.entity.metadata.PropertyType.SIMPLE;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.EQUAL;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.GREATER_THAN_EQUAL;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import mapping.entity.CompleteBean;
import me.prettyprint.cassandra.model.ExecutingKeyspace;
import me.prettyprint.cassandra.model.MutatorImpl;
import me.prettyprint.hector.api.beans.DynamicComposite;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.collect.Sets;

import fr.doan.achilles.composite.factory.DynamicCompositeKeyFactory;
import fr.doan.achilles.dao.GenericEntityDao;
import fr.doan.achilles.entity.EntityHelper;
import fr.doan.achilles.entity.manager.CompleteBeanTestBuilder;
import fr.doan.achilles.entity.metadata.ListMeta;
import fr.doan.achilles.entity.metadata.MapMeta;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.metadata.SetMeta;
import fr.doan.achilles.holder.KeyValueHolder;

@RunWith(MockitoJUnitRunner.class)
public class EntityPersisterTest
{

	@InjectMocks
	private EntityPersister persister = new EntityPersister();

	@Mock
	private EntityHelper helper;

	@Mock
	private GenericEntityDao<Long> dao;

	@Mock
	private PropertyMeta<?, String> propertyMeta;

	@Mock
	private ListMeta<String> listMeta;

	@Mock
	private SetMeta<String> setMeta;

	@Mock
	private MapMeta<Integer, String> mapMeta;

	@Mock
	private ExecutingKeyspace keyspace;

	@Mock
	private DynamicCompositeKeyFactory keyFactory;

	private MutatorImpl<Long> mutator = new MutatorImpl<Long>(keyspace);

	private Method method;

	private CompleteBean entity;

	@Before
	public void setUp() throws Exception
	{
		method = this.getClass().getDeclaredMethod("setUp", (Class<?>[]) null);
		entity = CompleteBeanTestBuilder.builder().id(1L).name("name").age(52L)
				.addFriends("foo", "bar").addFollowers("George", "Paul").addPreference(1, "FR")
				.buid();
	}

	@Test
	public void should_batch_simple_property() throws Exception
	{
		when(propertyMeta.getPropertyName()).thenReturn("name");
		when(propertyMeta.propertyType()).thenReturn(SIMPLE);
		DynamicComposite composite = new DynamicComposite();
		when(keyFactory.createForInsert("name", SIMPLE, 0)).thenReturn(composite);
		when(propertyMeta.getGetter()).thenReturn(method);
		when(helper.getValueFromField(entity, method)).thenReturn("testValue");

		ReflectionTestUtils.invokeMethod(persister, "batchSimpleProperty", entity, 1L, dao,
				propertyMeta, mutator);

		verify(dao).insertColumn(1L, composite, "testValue", mutator);
	}

	@Test
	public void should_persist_simple_property() throws Exception
	{
		when(propertyMeta.getPropertyName()).thenReturn("name");
		when(propertyMeta.propertyType()).thenReturn(SIMPLE);
		DynamicComposite composite = new DynamicComposite();
		when(keyFactory.createForInsert("name", SIMPLE, 0)).thenReturn(composite);
		when(propertyMeta.getGetter()).thenReturn(method);
		when(helper.getValueFromField(entity, method)).thenReturn("testValue");

		ReflectionTestUtils.invokeMethod(persister, "persistSimpleProperty", entity, 1L, dao,
				propertyMeta);

		verify(dao).insertColumn(1L, composite, "testValue", null);
	}

	@Test
	public void should_batch_list_property() throws Exception
	{
		when(propertyMeta.getGetter()).thenReturn(method);
		when(propertyMeta.propertyType()).thenReturn(LAZY_LIST);
		when(helper.getValueFromField(entity, method)).thenReturn(Arrays.asList("foo", "bar"));
		when(propertyMeta.getPropertyName()).thenReturn("friends");

		DynamicComposite composite = new DynamicComposite();
		when(keyFactory.createForInsert("friends", LAZY_LIST, 0)).thenReturn(composite);
		when(keyFactory.createForInsert("friends", LAZY_LIST, 1)).thenReturn(composite);

		ReflectionTestUtils.invokeMethod(persister, "batchListProperty", entity, 1L, dao,
				propertyMeta, mutator);

		verify(dao).insertColumn(1L, composite, "foo", mutator);
		verify(dao).insertColumn(1L, composite, "bar", mutator);
	}

	@Test
	public void should_persist_list_property() throws Exception
	{
		when(dao.buildMutator()).thenReturn(mutator);
		when(propertyMeta.propertyType()).thenReturn(LAZY_LIST);
		when(propertyMeta.getGetter()).thenReturn(method);
		when(helper.getValueFromField(entity, method)).thenReturn(Arrays.asList("foo", "bar"));
		when(propertyMeta.getPropertyName()).thenReturn("friends");

		DynamicComposite composite = new DynamicComposite();
		when(keyFactory.createForInsert("friends", LAZY_LIST, 0)).thenReturn(composite);
		when(keyFactory.createForInsert("friends", LAZY_LIST, 1)).thenReturn(composite);

		ReflectionTestUtils.invokeMethod(persister, "persistListProperty", entity, 1L, dao,
				propertyMeta);

		verify(dao).insertColumn(1L, composite, "foo", mutator);
		verify(dao).insertColumn(1L, composite, "bar", mutator);
	}

	@Test
	public void should_batch_set_property() throws Exception
	{
		when(propertyMeta.getGetter()).thenReturn(method);
		when(propertyMeta.propertyType()).thenReturn(SET);
		when(helper.getValueFromField(entity, method))
				.thenReturn(Sets.newHashSet("George", "Paul"));
		when(propertyMeta.getPropertyName()).thenReturn("followers");

		DynamicComposite composite = new DynamicComposite();
		when(keyFactory.createForInsert("followers", SET, "George".hashCode())).thenReturn(
				composite);
		when(keyFactory.createForInsert("followers", SET, "Paul".hashCode()))
				.thenReturn(composite);

		ReflectionTestUtils.invokeMethod(persister, "batchSetProperty", entity, 1L, dao,
				propertyMeta, mutator);

		verify(dao).insertColumn(1L, composite, "George", mutator);
		verify(dao).insertColumn(1L, composite, "Paul", mutator);
	}

	@Test
	public void should_persist_set_property() throws Exception
	{
		when(dao.buildMutator()).thenReturn(mutator);
		when(propertyMeta.getGetter()).thenReturn(method);
		when(propertyMeta.propertyType()).thenReturn(SET);
		when(helper.getValueFromField(entity, method))
				.thenReturn(Sets.newHashSet("George", "Paul"));
		when(propertyMeta.getPropertyName()).thenReturn("followers");

		DynamicComposite composite = new DynamicComposite();
		when(keyFactory.createForInsert("followers", SET, "George".hashCode())).thenReturn(
				composite);
		when(keyFactory.createForInsert("followers", SET, "Paul".hashCode()))
				.thenReturn(composite);

		ReflectionTestUtils.invokeMethod(persister, "persistSetProperty", entity, 1L, dao,
				propertyMeta);

		verify(dao).insertColumn(1L, composite, "George", mutator);
		verify(dao).insertColumn(1L, composite, "Paul", mutator);
	}

	@Test
	public void should_batch_map_property() throws Exception
	{
		when(propertyMeta.getGetter()).thenReturn(method);
		when(propertyMeta.propertyType()).thenReturn(LAZY_MAP);
		Map<Integer, String> map = new HashMap<Integer, String>();
		map.put(1, "FR");
		map.put(2, "Paris");
		map.put(3, "75014");

		when(helper.getValueFromField(entity, method)).thenReturn(map);
		when(propertyMeta.getPropertyName()).thenReturn("preferences");

		DynamicComposite composite = new DynamicComposite();
		when(keyFactory.createForInsert("preferences", LAZY_MAP, 1)).thenReturn(composite);
		when(keyFactory.createForInsert("preferences", LAZY_MAP, 2)).thenReturn(composite);
		when(keyFactory.createForInsert("preferences", LAZY_MAP, 3)).thenReturn(composite);

		ReflectionTestUtils.invokeMethod(persister, "batchMapProperty", entity, 1L, dao,
				propertyMeta, mutator);

		ArgumentCaptor<KeyValueHolder> keyValueHolderCaptor = ArgumentCaptor
				.forClass(KeyValueHolder.class);

		verify(dao, times(3)).insertColumn(eq(1L), eq(composite), keyValueHolderCaptor.capture(),
				eq(mutator));

		assertThat(keyValueHolderCaptor.getAllValues()).hasSize(3);
		KeyValueHolder holder1 = keyValueHolderCaptor.getAllValues().get(0);
		KeyValueHolder holder2 = keyValueHolderCaptor.getAllValues().get(1);
		KeyValueHolder holder3 = keyValueHolderCaptor.getAllValues().get(2);

		assertThat(holder1.getKey()).isEqualTo(1);
		assertThat(holder1.getValue()).isEqualTo("FR");

		assertThat(holder2.getKey()).isEqualTo(2);
		assertThat(holder2.getValue()).isEqualTo("Paris");

		assertThat(holder3.getKey()).isEqualTo(3);
		assertThat(holder3.getValue()).isEqualTo("75014");
	}

	@Test
	public void should_persist_map_property() throws Exception
	{
		when(dao.buildMutator()).thenReturn(mutator);
		when(propertyMeta.propertyType()).thenReturn(LAZY_MAP);
		when(propertyMeta.getGetter()).thenReturn(method);
		Map<Integer, String> map = new HashMap<Integer, String>();
		map.put(1, "FR");
		map.put(2, "Paris");
		map.put(3, "75014");

		when(helper.getValueFromField(entity, method)).thenReturn(map);
		when(propertyMeta.getPropertyName()).thenReturn("preferences");

		DynamicComposite composite = new DynamicComposite();
		when(keyFactory.createForInsert("preferences", LAZY_MAP, 1)).thenReturn(composite);
		when(keyFactory.createForInsert("preferences", LAZY_MAP, 2)).thenReturn(composite);
		when(keyFactory.createForInsert("preferences", LAZY_MAP, 3)).thenReturn(composite);

		ReflectionTestUtils.invokeMethod(persister, "persistMapProperty", entity, 1L, dao,
				propertyMeta);

		ArgumentCaptor<KeyValueHolder> keyValueHolderCaptor = ArgumentCaptor
				.forClass(KeyValueHolder.class);

		verify(dao, times(3)).insertColumn(eq(1L), eq(composite), keyValueHolderCaptor.capture(),
				eq(mutator));

		assertThat(keyValueHolderCaptor.getAllValues()).hasSize(3);
		KeyValueHolder holder1 = keyValueHolderCaptor.getAllValues().get(0);
		KeyValueHolder holder2 = keyValueHolderCaptor.getAllValues().get(1);
		KeyValueHolder holder3 = keyValueHolderCaptor.getAllValues().get(2);

		assertThat(holder1.getKey()).isEqualTo(1);
		assertThat(holder1.getValue()).isEqualTo("FR");

		assertThat(holder2.getKey()).isEqualTo(2);
		assertThat(holder2.getValue()).isEqualTo("Paris");

		assertThat(holder3.getKey()).isEqualTo(3);
		assertThat(holder3.getValue()).isEqualTo("75014");
	}

	@Test
	public void should_persist_simple_property_into_object() throws Exception
	{
		when(propertyMeta.propertyType()).thenReturn(SIMPLE);
		EntityPersister spy = spy(persister);
		spy.persistProperty(entity, 1L, dao, propertyMeta);
		verify(spy).persistSimpleProperty(entity, 1L, dao, propertyMeta);

	}

	@Test
	public void should_persist_simple_lazy_property_into_object() throws Exception
	{
		when(propertyMeta.propertyType()).thenReturn(LAZY_SIMPLE);
		EntityPersister spy = spy(persister);
		spy.persistProperty(entity, 1L, dao, propertyMeta);

		verify(spy).persistSimpleProperty(entity, 1L, dao, propertyMeta);
	}

	@Test
	public void should_persist_list_property_into_object() throws Exception
	{
		when(listMeta.propertyType()).thenReturn(LIST);
		EntityPersister spy = spy(persister);

		when(dao.buildMutator()).thenReturn(mutator);
		spy.persistProperty(entity, 1L, dao, listMeta);

		verify(spy).persistListProperty(entity, 1L, dao, listMeta);

	}

	@Test
	public void should_persist_list_lazy_property_into_object() throws Exception
	{
		when(listMeta.propertyType()).thenReturn(LAZY_LIST);
		EntityPersister spy = spy(persister);

		when(dao.buildMutator()).thenReturn(mutator);
		spy.persistProperty(entity, 1L, dao, listMeta);

		verify(spy).persistListProperty(entity, 1L, dao, listMeta);
	}

	@Test
	public void should_persist_set_property_into_object() throws Exception
	{
		when(setMeta.propertyType()).thenReturn(SET);
		EntityPersister spy = spy(persister);

		when(dao.buildMutator()).thenReturn(mutator);
		spy.persistProperty(entity, 1L, dao, setMeta);

		verify(spy).persistSetProperty(entity, 1L, dao, setMeta);
	}

	@Test
	public void should_persist_set_lazy_property_into_object() throws Exception
	{
		when(setMeta.propertyType()).thenReturn(LAZY_SET);
		EntityPersister spy = spy(persister);

		when(dao.buildMutator()).thenReturn(mutator);
		spy.persistProperty(entity, 1L, dao, setMeta);

		verify(spy).persistSetProperty(entity, 1L, dao, setMeta);
	}

	@Test
	public void should_persist_map_property_into_object() throws Exception
	{
		when(mapMeta.propertyType()).thenReturn(MAP);
		EntityPersister spy = spy(persister);

		when(dao.buildMutator()).thenReturn(mutator);
		spy.persistProperty(entity, 1L, dao, mapMeta);

		verify(spy).persistMapProperty(entity, 1L, dao, mapMeta);

	}

	@Test
	public void should_persist_map_lazy_property_into_object() throws Exception
	{
		when(mapMeta.propertyType()).thenReturn(LAZY_MAP);
		EntityPersister spy = spy(persister);

		when(dao.buildMutator()).thenReturn(mutator);
		spy.persistProperty(entity, 1L, dao, mapMeta);

		verify(spy).persistMapProperty(entity, 1L, dao, mapMeta);
	}

	@Test
	public void should_remove_property() throws Exception
	{
		when(propertyMeta.getPropertyName()).thenReturn("name");
		when(propertyMeta.propertyType()).thenReturn(MAP);

		DynamicComposite start = new DynamicComposite();
		DynamicComposite end = new DynamicComposite();

		when(keyFactory.createBaseForQuery("name", MAP, EQUAL)).thenReturn(start);
		when(keyFactory.createBaseForQuery("name", MAP, GREATER_THAN_EQUAL)).thenReturn(end);

		persister.removeProperty(1L, dao, propertyMeta);

		verify(dao).removeColumnRange(1L, start, end);
	}
}
