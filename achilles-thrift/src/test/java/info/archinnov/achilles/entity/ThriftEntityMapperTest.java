package info.archinnov.achilles.entity;

import static info.archinnov.achilles.entity.metadata.PropertyType.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.proxy.AchillesMethodInvoker;
import info.archinnov.achilles.type.KeyValue;
import info.archinnov.achilles.type.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import mapping.entity.CompleteBean;
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

import testBuilders.EntityMetaTestBuilder;
import testBuilders.PropertyMetaTestBuilder;

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
	private AchillesMethodInvoker invoker;

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
	public void should_exception_when_serialVersionUID_changes() throws Exception
	{
		CompleteBean entity = new CompleteBean();
		List<Pair<Composite, String>> columns = new ArrayList<Pair<Composite, String>>();

		columns.add(new Pair<Composite, String>(buildSimplePropertyComposite(SERIAL_VERSION_UID
				.name()), "123"));

		expectedException.expect(IllegalStateException.class);
		expectedException
				.expectMessage("Saved serialVersionUID does not match current serialVersionUID for entity '"
						+ CompleteBean.class.getCanonicalName() + "'");

		entityMeta = EntityMetaTestBuilder.builder(idMeta) //
				.serialVersionUID(2L)
				//
				.classname(CompleteBean.class.getCanonicalName())
				//
				.build();
		mapper.setEagerPropertiesToEntity(2L, columns, entityMeta, entity);
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
