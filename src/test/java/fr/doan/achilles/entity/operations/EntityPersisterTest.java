package fr.doan.achilles.entity.operations;

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
import me.prettyprint.hector.api.beans.Composite;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.collect.Sets;

import fr.doan.achilles.dao.GenericDao;
import fr.doan.achilles.entity.EntityPropertyHelper;
import fr.doan.achilles.entity.manager.CompleteBeanTestBuilder;
import fr.doan.achilles.entity.metadata.ListPropertyMeta;
import fr.doan.achilles.entity.metadata.MapPropertyMeta;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.metadata.PropertyType;
import fr.doan.achilles.entity.metadata.SetPropertyMeta;
import fr.doan.achilles.entity.operations.EntityPersister;
import fr.doan.achilles.holder.KeyValueHolder;

@RunWith(MockitoJUnitRunner.class)
public class EntityPersisterTest
{

	@InjectMocks
	private EntityPersister persister = new EntityPersister();

	@Mock
	private EntityPropertyHelper helper;

	@Mock
	private GenericDao<Long> dao;

	@Mock
	private PropertyMeta<String> propertyMeta;

	@Mock
	private ListPropertyMeta<String> listPropertyMeta;

	@Mock
	private SetPropertyMeta<String> setPropertyMeta;

	@Mock
	private MapPropertyMeta<String> mapPropertyMeta;

	@Mock
	private ExecutingKeyspace keyspace;

	private MutatorImpl<Long> mutator = new MutatorImpl<Long>(keyspace);

	private Method method;

	private CompleteBean entity;

	@Before
	public void setUp() throws Exception
	{
		method = this.getClass().getDeclaredMethod("setUp", (Class<?>[]) null);
		entity = CompleteBeanTestBuilder.builder().id(1L).name("name").age(52L).addFriends("foo", "bar").addFollowers("George", "Paul")
				.addPreference(1, "FR").buid();
	}

	@Test
	public void should_batch_simple_property() throws Exception
	{
		when(propertyMeta.getPropertyName()).thenReturn("name");
		when(propertyMeta.propertyType()).thenReturn(PropertyType.SIMPLE);
		Composite composite = new Composite();
		when(dao.buildCompositeForProperty("name", PropertyType.SIMPLE, 0)).thenReturn(composite);
		when(propertyMeta.getGetter()).thenReturn(method);
		when(helper.getValueFromField(entity, method)).thenReturn("testValue");

		ReflectionTestUtils.invokeMethod(persister, "batchSimpleProperty", entity, 1L, dao, propertyMeta, mutator);

		verify(dao).insertColumn(1L, composite, "testValue", mutator);
	}

	@Test
	public void should_persist_simple_property() throws Exception
	{
		when(propertyMeta.getPropertyName()).thenReturn("name");
		when(propertyMeta.propertyType()).thenReturn(PropertyType.SIMPLE);
		Composite composite = new Composite();
		when(dao.buildCompositeForProperty("name", PropertyType.SIMPLE, 0)).thenReturn(composite);
		when(propertyMeta.getGetter()).thenReturn(method);
		when(helper.getValueFromField(entity, method)).thenReturn("testValue");

		ReflectionTestUtils.invokeMethod(persister, "persistSimpleProperty", entity, 1L, dao, propertyMeta);

		verify(dao).insertColumn(1L, composite, "testValue", null);
	}

	@Test
	public void should_batch_list_property() throws Exception
	{
		when(propertyMeta.getGetter()).thenReturn(method);
		when(propertyMeta.propertyType()).thenReturn(PropertyType.LAZY_LIST);
		when(helper.getValueFromField(entity, method)).thenReturn(Arrays.asList("foo", "bar"));
		when(propertyMeta.getPropertyName()).thenReturn("friends");

		Composite composite = new Composite();
		when(dao.buildCompositeForProperty("friends", PropertyType.LAZY_LIST, 0)).thenReturn(composite);
		when(dao.buildCompositeForProperty("friends", PropertyType.LAZY_LIST, 1)).thenReturn(composite);

		ReflectionTestUtils.invokeMethod(persister, "batchListProperty", entity, 1L, dao, propertyMeta, mutator);

		verify(dao).insertColumn(1L, composite, "foo", mutator);
		verify(dao).insertColumn(1L, composite, "bar", mutator);
	}

	@Test
	public void should_persist_list_property() throws Exception
	{
		when(dao.buildMutator()).thenReturn(mutator);
		when(propertyMeta.propertyType()).thenReturn(PropertyType.LAZY_LIST);
		when(propertyMeta.getGetter()).thenReturn(method);
		when(helper.getValueFromField(entity, method)).thenReturn(Arrays.asList("foo", "bar"));
		when(propertyMeta.getPropertyName()).thenReturn("friends");

		Composite composite = new Composite();
		when(dao.buildCompositeForProperty("friends", PropertyType.LAZY_LIST, 0)).thenReturn(composite);
		when(dao.buildCompositeForProperty("friends", PropertyType.LAZY_LIST, 1)).thenReturn(composite);

		ReflectionTestUtils.invokeMethod(persister, "persistListProperty", entity, 1L, dao, propertyMeta);

		verify(dao).insertColumn(1L, composite, "foo", mutator);
		verify(dao).insertColumn(1L, composite, "bar", mutator);
	}

	@Test
	public void should_batch_set_property() throws Exception
	{
		when(propertyMeta.getGetter()).thenReturn(method);
		when(propertyMeta.propertyType()).thenReturn(PropertyType.SET);
		when(helper.getValueFromField(entity, method)).thenReturn(Sets.newHashSet("George", "Paul"));
		when(propertyMeta.getPropertyName()).thenReturn("followers");

		Composite composite = new Composite();
		when(dao.buildCompositeForProperty("followers", PropertyType.SET, "George".hashCode())).thenReturn(composite);
		when(dao.buildCompositeForProperty("followers", PropertyType.SET, "Paul".hashCode())).thenReturn(composite);

		ReflectionTestUtils.invokeMethod(persister, "batchSetProperty", entity, 1L, dao, propertyMeta, mutator);

		verify(dao).insertColumn(1L, composite, "George", mutator);
		verify(dao).insertColumn(1L, composite, "Paul", mutator);
	}

	@Test
	public void should_persist_set_property() throws Exception
	{
		when(dao.buildMutator()).thenReturn(mutator);
		when(propertyMeta.getGetter()).thenReturn(method);
		when(propertyMeta.propertyType()).thenReturn(PropertyType.SET);
		when(helper.getValueFromField(entity, method)).thenReturn(Sets.newHashSet("George", "Paul"));
		when(propertyMeta.getPropertyName()).thenReturn("followers");

		Composite composite = new Composite();
		when(dao.buildCompositeForProperty("followers", PropertyType.SET, "George".hashCode())).thenReturn(composite);
		when(dao.buildCompositeForProperty("followers", PropertyType.SET, "Paul".hashCode())).thenReturn(composite);

		ReflectionTestUtils.invokeMethod(persister, "persistSetProperty", entity, 1L, dao, propertyMeta);

		verify(dao).insertColumn(1L, composite, "George", mutator);
		verify(dao).insertColumn(1L, composite, "Paul", mutator);
	}

	@Test
	public void should_batch_map_property() throws Exception
	{
		when(propertyMeta.getGetter()).thenReturn(method);
		when(propertyMeta.propertyType()).thenReturn(PropertyType.LAZY_MAP);
		Map<Integer, String> map = new HashMap<Integer, String>();
		map.put(1, "FR");
		map.put(2, "Paris");
		map.put(3, "75014");

		when(helper.getValueFromField(entity, method)).thenReturn(map);
		when(propertyMeta.getPropertyName()).thenReturn("preferences");

		Composite composite = new Composite();
		when(dao.buildCompositeForProperty("preferences", PropertyType.LAZY_MAP, 1)).thenReturn(composite);
		when(dao.buildCompositeForProperty("preferences", PropertyType.LAZY_MAP, 2)).thenReturn(composite);
		when(dao.buildCompositeForProperty("preferences", PropertyType.LAZY_MAP, 3)).thenReturn(composite);

		ReflectionTestUtils.invokeMethod(persister, "batchMapProperty", entity, 1L, dao, propertyMeta, mutator);

		ArgumentCaptor<KeyValueHolder> keyValueHolderCaptor = ArgumentCaptor.forClass(KeyValueHolder.class);

		verify(dao, times(3)).insertColumn(eq(1L), eq(composite), keyValueHolderCaptor.capture(), eq(mutator));

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
		when(propertyMeta.propertyType()).thenReturn(PropertyType.LAZY_MAP);
		when(propertyMeta.getGetter()).thenReturn(method);
		Map<Integer, String> map = new HashMap<Integer, String>();
		map.put(1, "FR");
		map.put(2, "Paris");
		map.put(3, "75014");

		when(helper.getValueFromField(entity, method)).thenReturn(map);
		when(propertyMeta.getPropertyName()).thenReturn("preferences");

		Composite composite = new Composite();
		when(dao.buildCompositeForProperty("preferences", PropertyType.LAZY_MAP, 1)).thenReturn(composite);
		when(dao.buildCompositeForProperty("preferences", PropertyType.LAZY_MAP, 2)).thenReturn(composite);
		when(dao.buildCompositeForProperty("preferences", PropertyType.LAZY_MAP, 3)).thenReturn(composite);

		ReflectionTestUtils.invokeMethod(persister, "persistMapProperty", entity, 1L, dao, propertyMeta);

		ArgumentCaptor<KeyValueHolder> keyValueHolderCaptor = ArgumentCaptor.forClass(KeyValueHolder.class);

		verify(dao, times(3)).insertColumn(eq(1L), eq(composite), keyValueHolderCaptor.capture(), eq(mutator));

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
		when(propertyMeta.propertyType()).thenReturn(PropertyType.SIMPLE);
		EntityPersister spy = spy(persister);
		spy.persistProperty(entity, 1L, dao, propertyMeta);
		verify(spy).persistSimpleProperty(entity, 1L, dao, propertyMeta);

	}

	@Test
	public void should_persist_simple_lazy_property_into_object() throws Exception
	{
		when(propertyMeta.propertyType()).thenReturn(PropertyType.LAZY_SIMPLE);
		EntityPersister spy = spy(persister);
		spy.persistProperty(entity, 1L, dao, propertyMeta);

		verify(spy).persistSimpleProperty(entity, 1L, dao, propertyMeta);
	}

	@Test
	public void should_persist_list_property_into_object() throws Exception
	{
		when(listPropertyMeta.propertyType()).thenReturn(PropertyType.LIST);
		EntityPersister spy = spy(persister);

		when(dao.buildMutator()).thenReturn(mutator);
		spy.persistProperty(entity, 1L, dao, listPropertyMeta);

		verify(spy).persistListProperty(entity, 1L, dao, listPropertyMeta);

	}

	@Test
	public void should_persist_list_lazy_property_into_object() throws Exception
	{
		when(listPropertyMeta.propertyType()).thenReturn(PropertyType.LAZY_LIST);
		EntityPersister spy = spy(persister);

		when(dao.buildMutator()).thenReturn(mutator);
		spy.persistProperty(entity, 1L, dao, listPropertyMeta);

		verify(spy).persistListProperty(entity, 1L, dao, listPropertyMeta);
	}

	@Test
	public void should_persist_set_property_into_object() throws Exception
	{
		when(setPropertyMeta.propertyType()).thenReturn(PropertyType.SET);
		EntityPersister spy = spy(persister);

		when(dao.buildMutator()).thenReturn(mutator);
		spy.persistProperty(entity, 1L, dao, setPropertyMeta);

		verify(spy).persistSetProperty(entity, 1L, dao, setPropertyMeta);
	}

	@Test
	public void should_persist_set_lazy_property_into_object() throws Exception
	{
		when(setPropertyMeta.propertyType()).thenReturn(PropertyType.LAZY_SET);
		EntityPersister spy = spy(persister);

		when(dao.buildMutator()).thenReturn(mutator);
		spy.persistProperty(entity, 1L, dao, setPropertyMeta);

		verify(spy).persistSetProperty(entity, 1L, dao, setPropertyMeta);
	}

	@Test
	public void should_persist_map_property_into_object() throws Exception
	{
		when(mapPropertyMeta.propertyType()).thenReturn(PropertyType.MAP);
		EntityPersister spy = spy(persister);

		when(dao.buildMutator()).thenReturn(mutator);
		spy.persistProperty(entity, 1L, dao, mapPropertyMeta);

		verify(spy).persistMapProperty(entity, 1L, dao, mapPropertyMeta);

	}

	@Test
	public void should_persist_map_lazy_property_into_object() throws Exception
	{
		when(mapPropertyMeta.propertyType()).thenReturn(PropertyType.LAZY_MAP);
		EntityPersister spy = spy(persister);

		when(dao.buildMutator()).thenReturn(mutator);
		spy.persistProperty(entity, 1L, dao, mapPropertyMeta);

		verify(spy).persistMapProperty(entity, 1L, dao, mapPropertyMeta);
	}
}
