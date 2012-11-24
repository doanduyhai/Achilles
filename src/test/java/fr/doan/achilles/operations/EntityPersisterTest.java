package fr.doan.achilles.operations;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

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

import parser.entity.Bean;

import com.google.common.collect.Sets;

import fr.doan.achilles.bean.BeanPropertyHelper;
import fr.doan.achilles.dao.GenericDao;
import fr.doan.achilles.holder.KeyValueHolder;
import fr.doan.achilles.metadata.PropertyMeta;
import fr.doan.achilles.metadata.PropertyType;

@RunWith(MockitoJUnitRunner.class)
public class EntityPersisterTest
{

	@InjectMocks
	private EntityPersister persister = new EntityPersister();

	@Mock
	private BeanPropertyHelper helper;

	@Mock
	private GenericDao<Long> dao;

	@Mock
	private PropertyMeta<?> propertyMeta;
	@Mock
	private ExecutingKeyspace keyspace;

	private MutatorImpl<Long> mutator = new MutatorImpl<Long>(keyspace);

	private Method method;

	private Bean entity = new Bean();

	@Before
	public void setUp() throws Exception
	{
		method = this.getClass().getDeclaredMethod("setUp", (Class<?>[]) null);
	}

	@Test
	public void should_batch_simple_property() throws Exception
	{
		when(propertyMeta.getPropertyName()).thenReturn("name");
		Composite composite = new Composite();
		when(dao.buildCompositeForProperty("name", PropertyType.SIMPLE, 0)).thenReturn(composite);
		when(propertyMeta.getGetter()).thenReturn(method);
		when(helper.getValueFromField(entity, method)).thenReturn("testValue");

		ReflectionTestUtils.invokeMethod(persister, "batchSimpleProperty", entity, 1L, dao, mutator, propertyMeta);

		verify(dao).insertColumnBatch(1L, composite, "testValue", mutator);
	}

	@Test
	public void should_persist_simple_property() throws Exception
	{
		when(propertyMeta.getPropertyName()).thenReturn("name");
		Composite composite = new Composite();
		when(dao.buildCompositeForProperty("name", PropertyType.SIMPLE, 0)).thenReturn(composite);
		when(propertyMeta.getGetter()).thenReturn(method);
		when(helper.getValueFromField(entity, method)).thenReturn("testValue");

		ReflectionTestUtils.invokeMethod(persister, "persistSimpleProperty", entity, 1L, dao, propertyMeta);

		verify(dao).insertColumn(1L, composite, "testValue");
	}

	@Test
	public void should_batch_list_property() throws Exception
	{
		when(propertyMeta.getGetter()).thenReturn(method);
		when(helper.getValueFromField(entity, method)).thenReturn(Arrays.asList("foo", "bar"));
		when(propertyMeta.getPropertyName()).thenReturn("friends");

		Composite composite = new Composite();
		when(dao.buildCompositeForProperty("friends", PropertyType.LIST, 0)).thenReturn(composite);
		when(dao.buildCompositeForProperty("friends", PropertyType.LIST, 1)).thenReturn(composite);

		ReflectionTestUtils.invokeMethod(persister, "batchListProperty", entity, 1L, dao, mutator, propertyMeta);

		verify(dao).insertColumnBatch(1L, composite, "foo", mutator);
		verify(dao).insertColumnBatch(1L, composite, "bar", mutator);
	}

	@Test
	public void should_persist_list_property() throws Exception
	{
		when(dao.buildMutator()).thenReturn(mutator);
		when(propertyMeta.getGetter()).thenReturn(method);
		when(helper.getValueFromField(entity, method)).thenReturn(Arrays.asList("foo", "bar"));
		when(propertyMeta.getPropertyName()).thenReturn("friends");

		Composite composite = new Composite();
		when(dao.buildCompositeForProperty("friends", PropertyType.LIST, 0)).thenReturn(composite);
		when(dao.buildCompositeForProperty("friends", PropertyType.LIST, 1)).thenReturn(composite);

		ReflectionTestUtils.invokeMethod(persister, "persistListProperty", entity, 1L, dao, propertyMeta);

		verify(dao).insertColumnBatch(1L, composite, "foo", mutator);
		verify(dao).insertColumnBatch(1L, composite, "bar", mutator);
	}

	@Test
	public void should_batch_set_property() throws Exception
	{
		when(propertyMeta.getGetter()).thenReturn(method);
		when(helper.getValueFromField(entity, method)).thenReturn(Sets.newHashSet("George", "Paul"));
		when(propertyMeta.getPropertyName()).thenReturn("followers");

		Composite composite = new Composite();
		when(dao.buildCompositeForProperty("followers", PropertyType.SET, "George".hashCode())).thenReturn(composite);
		when(dao.buildCompositeForProperty("followers", PropertyType.SET, "Paul".hashCode())).thenReturn(composite);

		ReflectionTestUtils.invokeMethod(persister, "batchSetProperty", entity, 1L, dao, mutator, propertyMeta);

		verify(dao).insertColumnBatch(1L, composite, "George", mutator);
		verify(dao).insertColumnBatch(1L, composite, "Paul", mutator);
	}

	@Test
	public void should_persist_set_property() throws Exception
	{
		when(dao.buildMutator()).thenReturn(mutator);
		when(propertyMeta.getGetter()).thenReturn(method);
		when(helper.getValueFromField(entity, method)).thenReturn(Sets.newHashSet("George", "Paul"));
		when(propertyMeta.getPropertyName()).thenReturn("followers");

		Composite composite = new Composite();
		when(dao.buildCompositeForProperty("followers", PropertyType.SET, "George".hashCode())).thenReturn(composite);
		when(dao.buildCompositeForProperty("followers", PropertyType.SET, "Paul".hashCode())).thenReturn(composite);

		ReflectionTestUtils.invokeMethod(persister, "persistSetProperty", entity, 1L, dao, propertyMeta);

		verify(dao).insertColumnBatch(1L, composite, "George", mutator);
		verify(dao).insertColumnBatch(1L, composite, "Paul", mutator);
	}

	@Test
	public void should_batch_map_property() throws Exception
	{
		when(propertyMeta.getGetter()).thenReturn(method);
		Map<Integer, String> map = new HashMap<Integer, String>();
		map.put(1, "FR");
		map.put(2, "Paris");
		map.put(3, "75014");

		when(helper.getValueFromField(entity, method)).thenReturn(map);
		when(propertyMeta.getPropertyName()).thenReturn("preferences");

		Composite composite = new Composite();
		when(dao.buildCompositeForProperty("preferences", PropertyType.MAP, 1)).thenReturn(composite);
		when(dao.buildCompositeForProperty("preferences", PropertyType.MAP, 2)).thenReturn(composite);
		when(dao.buildCompositeForProperty("preferences", PropertyType.MAP, 3)).thenReturn(composite);

		ReflectionTestUtils.invokeMethod(persister, "batchMapProperty", entity, 1L, dao, mutator, propertyMeta);

		ArgumentCaptor<KeyValueHolder> keyValueHolderCaptor = ArgumentCaptor.forClass(KeyValueHolder.class);

		verify(dao, times(3)).insertColumnBatch(eq(1L), eq(composite), keyValueHolderCaptor.capture(), eq(mutator));

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
		when(propertyMeta.getGetter()).thenReturn(method);
		Map<Integer, String> map = new HashMap<Integer, String>();
		map.put(1, "FR");
		map.put(2, "Paris");
		map.put(3, "75014");

		when(helper.getValueFromField(entity, method)).thenReturn(map);
		when(propertyMeta.getPropertyName()).thenReturn("preferences");

		Composite composite = new Composite();
		when(dao.buildCompositeForProperty("preferences", PropertyType.MAP, 1)).thenReturn(composite);
		when(dao.buildCompositeForProperty("preferences", PropertyType.MAP, 2)).thenReturn(composite);
		when(dao.buildCompositeForProperty("preferences", PropertyType.MAP, 3)).thenReturn(composite);

		ReflectionTestUtils.invokeMethod(persister, "persistMapProperty", entity, 1L, dao, propertyMeta);

		ArgumentCaptor<KeyValueHolder> keyValueHolderCaptor = ArgumentCaptor.forClass(KeyValueHolder.class);

		verify(dao, times(3)).insertColumnBatch(eq(1L), eq(composite), keyValueHolderCaptor.capture(), eq(mutator));

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
}
