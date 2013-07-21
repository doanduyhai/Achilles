package info.archinnov.achilles.entity;

import static info.archinnov.achilles.entity.metadata.PropertyType.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.test.builders.EntityMetaTestBuilder;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.test.parser.entity.BeanWithClusteredId;
import info.archinnov.achilles.test.parser.entity.CompoundKey;
import info.archinnov.achilles.type.KeyValue;
import info.archinnov.achilles.type.Pair;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import me.prettyprint.hector.api.beans.Composite;

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

/**
 * ThriftEntityMapperTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class ThriftEntityMapperTest
{
	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	@InjectMocks
	private ThriftEntityMapper mapper;

	@Mock
	private ReflectionInvoker invoker;

	@Mock
	private ThriftPersistenceContext context;

	private ObjectMapper objectMapper = new ObjectMapper();

	@Captor
	ArgumentCaptor<Long> idCaptor;

	@Captor
	ArgumentCaptor<String> simpleCaptor;

	@Captor
	ArgumentCaptor<List<String>> listCaptor;

	@Captor
	ArgumentCaptor<Set<String>> setCaptor;

	@Captor
	ArgumentCaptor<Map<Integer, String>> mapCaptor;

	private EntityMeta entityMeta;

	private PropertyMeta<Void, Long> idMeta;

	@Before
	public void setUp() throws Exception
	{
		idMeta = PropertyMetaTestBuilder //
				.of(CompleteBean.class, Void.class, Long.class)
				.field("id")
				.build();
	}

	@Test
	public void should_map_columns_to_bean() throws Exception
	{

		CompleteBean entity = new CompleteBean();

		PropertyMeta<Void, String> namePropertyMeta = PropertyMetaTestBuilder //
				.of(CompleteBean.class, Void.class, String.class)
				.field("name")
				.type(SIMPLE)
				.mapper(objectMapper)
				.accessors()
				.build();

		PropertyMeta<Void, String> listPropertyMeta = PropertyMetaTestBuilder //
				.of(CompleteBean.class, Void.class, String.class)
				.field("friends")
				.type(LIST)
				.mapper(objectMapper)
				.accessors()
				.build();

		PropertyMeta<Void, String> setPropertyMeta = PropertyMetaTestBuilder //
				.of(CompleteBean.class, Void.class, String.class)
				.field("followers")
				.type(SET)
				.mapper(objectMapper)
				.accessors()
				.build();

		PropertyMeta<Integer, String> mapPropertyMeta = PropertyMetaTestBuilder //
				.of(CompleteBean.class, Integer.class, String.class)
				.field("preferences")
				.type(MAP)
				.mapper(objectMapper)
				.accessors()
				.build();

		entityMeta = EntityMetaTestBuilder.builder(idMeta) //
				.addPropertyMeta(namePropertyMeta)
				.addPropertyMeta(listPropertyMeta)
				.addPropertyMeta(setPropertyMeta)
				.addPropertyMeta(mapPropertyMeta)
				.build();

		List<Pair<Composite, String>> columns = new ArrayList<Pair<Composite, String>>();

		columns.add(new Pair<Composite, String>(buildSimplePropertyComposite("name"), "name"));

		columns.add(new Pair<Composite, String>(buildListPropertyComposite("friends"), "foo"));
		columns.add(new Pair<Composite, String>(buildListPropertyComposite("friends"), "bar"));

		columns.add(new Pair<Composite, String>(buildSetPropertyComposite("followers"), "George"));
		columns.add(new Pair<Composite, String>(buildSetPropertyComposite("followers"), "Paul"));

		columns.add(new Pair<Composite, String>(buildMapPropertyComposite("preferences"),
				writeToString(new KeyValue<Integer, String>(1, "FR"))));
		columns.add(new Pair<Composite, String>(buildMapPropertyComposite("preferences"),
				writeToString(new KeyValue<Integer, String>(2, "Paris"))));
		columns.add(new Pair<Composite, String>(buildMapPropertyComposite("preferences"),
				writeToString(new KeyValue<Integer, String>(3, "75014"))));

		doNothing().when(invoker).setValueToField(eq(entity), eq(idMeta.getSetter()),
				idCaptor.capture());
		doNothing().when(invoker).setValueToField(eq(entity), eq(namePropertyMeta.getSetter()),
				simpleCaptor.capture());
		doNothing().when(invoker).setValueToField(eq(entity), eq(setPropertyMeta.getSetter()),
				setCaptor.capture());
		doNothing().when(invoker).setValueToField(eq(entity), eq(listPropertyMeta.getSetter()),
				listCaptor.capture());
		doNothing().when(invoker).setValueToField(eq(entity), eq(mapPropertyMeta.getSetter()),
				mapCaptor.capture());

		mapper.setEagerPropertiesToEntity(2L, columns, entityMeta, entity);

		assertThat(idCaptor.getValue()).isEqualTo(2L);
		assertThat(simpleCaptor.getValue()).isEqualTo("name");

		assertThat(listCaptor.getValue()).hasSize(2);
		assertThat(listCaptor.getValue()).contains("foo", "bar");

		assertThat(setCaptor.getValue()).hasSize(2);
		assertThat(setCaptor.getValue()).contains("George", "Paul");

		assertThat(mapCaptor.getValue()).hasSize(3);
		assertThat(mapCaptor.getValue().get(1)).isEqualTo("FR");
		assertThat(mapCaptor.getValue().get(2)).isEqualTo("Paris");
		assertThat(mapCaptor.getValue().get(3)).isEqualTo("75014");
	}

	@Test
	public void should_do_nothing_for_unmapped_property() throws Exception
	{
		CompleteBean entity = new CompleteBean();

		PropertyMeta<Void, String> namePropertyMeta = PropertyMetaTestBuilder //
				.of(CompleteBean.class, Void.class, String.class)
				.field("name")
				.type(SIMPLE)
				.mapper(objectMapper)
				.accessors()
				.build();

		entityMeta = EntityMetaTestBuilder.builder(idMeta) //
				.addPropertyMeta(namePropertyMeta)
				.build();

		List<Pair<Composite, String>> columns = new ArrayList<Pair<Composite, String>>();

		columns.add(new Pair<Composite, String>(buildSimplePropertyComposite("name"), "name"));
		columns.add(new Pair<Composite, String>(buildSimplePropertyComposite("unmapped"),
				"unmapped property"));

		doNothing().when(invoker).setValueToField(eq(entity), eq(namePropertyMeta.getSetter()),
				simpleCaptor.capture());

		mapper.setEagerPropertiesToEntity(2L, columns, entityMeta, entity);

		assertThat(simpleCaptor.getValue()).isEqualTo("name");

	}

	@Test
	public void should_not_set_lazy_or_proxy_property_type() throws Exception
	{
		CompleteBean entity = new CompleteBean();

		PropertyMeta<Void, String> lazyNamePropertyMeta = PropertyMetaTestBuilder //
				.of(CompleteBean.class, Void.class, String.class)
				.field("name")
				.type(LAZY_SIMPLE)
				.mapper(objectMapper)
				.accessors()
				.build();

		entityMeta = EntityMetaTestBuilder.builder(idMeta) //
				.addPropertyMeta(lazyNamePropertyMeta)
				.build();

		List<Pair<Composite, String>> columns = new ArrayList<Pair<Composite, String>>();

		columns.add(new Pair<Composite, String>(buildSimplePropertyComposite("name"), "name"));

		mapper.setEagerPropertiesToEntity(2L, columns, entityMeta, entity);

		verify(invoker, never()).setValueToField(entity, lazyNamePropertyMeta.getSetter(), "name");

	}

	@Test
	public void should_init_clustered_entity() throws Exception
	{
		BeanWithClusteredId entity = new BeanWithClusteredId();
		CompoundKey compoundKey = new CompoundKey();
		String clusteredValue = "clusteredValue";

		Method idSetter = BeanWithClusteredId.class.getDeclaredMethod("setId", CompoundKey.class);
		Method nameSetter = BeanWithClusteredId.class.getDeclaredMethod("setName", String.class);

		PropertyMeta idMeta = PropertyMetaTestBuilder
				.valueClass(CompoundKey.class)
				.build();
		idMeta.setSetter(idSetter);

		when(invoker.instanciate(BeanWithClusteredId.class)).thenReturn(entity);
		BeanWithClusteredId actual = mapper.initClusteredEntity(BeanWithClusteredId.class,
				idMeta, compoundKey);

		assertThat(actual).isSameAs(entity);

		verify(invoker).setValueToField(entity, idSetter, compoundKey);
	}

	@Test
	public void should_create_clustered_entity_with_value() throws Exception
	{
		BeanWithClusteredId entity = new BeanWithClusteredId();
		CompoundKey compoundKey = new CompoundKey();
		String clusteredValue = "clusteredValue";

		Method idSetter = BeanWithClusteredId.class.getDeclaredMethod("setId", CompoundKey.class);
		Method nameSetter = BeanWithClusteredId.class.getDeclaredMethod("setName", String.class);

		PropertyMeta idMeta = PropertyMetaTestBuilder
				.valueClass(CompoundKey.class)
				.build();
		idMeta.setSetter(idSetter);

		PropertyMeta pm = PropertyMetaTestBuilder //
				.valueClass(String.class)
				.type(SIMPLE)
				.build();
		pm.setSetter(nameSetter);

		when(context.getIdMeta()).thenReturn(idMeta);
		when(context.getFirstMeta()).thenReturn(pm);

		when(invoker.instanciate(BeanWithClusteredId.class)).thenReturn(entity);
		BeanWithClusteredId actual = mapper.createClusteredEntityWithValue(
				BeanWithClusteredId.class,
				idMeta, pm, compoundKey, clusteredValue);

		assertThat(actual).isSameAs(entity);

		verify(invoker).setValueToField(entity, idSetter, compoundKey);
		verify(invoker).setValueToField(entity, nameSetter, clusteredValue);
	}

	private void initMetaForClusteredEntity(Method idSetter, Method nameSetter,
			PropertyType type)
			throws Exception
	{
		PropertyMeta idMeta = PropertyMetaTestBuilder
				.valueClass(CompoundKey.class)
				.build();
		idMeta.setSetter(idSetter);

		PropertyMeta pm = PropertyMetaTestBuilder //
				.valueClass(String.class)
				.type(type)
				.build();
		pm.setSetter(nameSetter);

		when(context.getIdMeta()).thenReturn(idMeta);
		when(context.getFirstMeta()).thenReturn(pm);

	}

	private Composite buildSimplePropertyComposite(String propertyName)
	{
		Composite comp = new Composite();
		comp.add(0, SIMPLE.flag());
		comp.add(1, propertyName);
		return comp;
	}

	private Composite buildListPropertyComposite(String propertyName)
	{
		Composite comp = new Composite();
		comp.add(0, LIST.flag());
		comp.add(1, propertyName);
		return comp;
	}

	private Composite buildSetPropertyComposite(String propertyName)
	{
		Composite comp = new Composite();
		comp.add(0, SET.flag());
		comp.add(1, propertyName);
		return comp;
	}

	private Composite buildMapPropertyComposite(String propertyName)
	{
		Composite comp = new Composite();
		comp.add(0, MAP.flag());
		comp.add(1, propertyName);
		return comp;
	}

	private String writeToString(Object object) throws Exception
	{
		return objectMapper.writeValueAsString(object);
	}
}
