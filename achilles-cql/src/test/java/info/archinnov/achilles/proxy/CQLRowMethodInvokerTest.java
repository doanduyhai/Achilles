package info.archinnov.achilles.proxy;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.exception.AchillesException;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.datastax.driver.core.Row;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

/**
 * CQLResultMethodInvokerTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class CQLRowMethodInvokerTest
{
	@Rule
	public ExpectedException exception = ExpectedException.none();

	private CQLRowMethodInvoker invoker = new CQLRowMethodInvoker();

	@Mock
	private Row row;

	@Mock(answer = Answers.RETURNS_DEEP_STUBS)
	private PropertyMeta<Integer, String> pm;

	@Before
	public void setUp()
	{
		when(pm.getPropertyName()).thenReturn("property");
		when(pm.getKeyClass()).thenReturn(Integer.class);
		when(pm.getValueClass()).thenReturn(String.class);
		when(row.isNull("property")).thenReturn(false);
	}

	@Test
	public void should_get_list_value_from_row() throws Exception
	{
		when(pm.type()).thenReturn(PropertyType.LIST);

		List<String> list = Arrays.asList("value");
		when(row.getList("property", String.class)).thenReturn(list);

		Object actual = invoker.invokeOnRowForEagerFields(row, pm);

		assertThat(actual).isSameAs(list);
	}

	@Test
	public void should_get_set_value_from_row() throws Exception
	{
		when(pm.type()).thenReturn(PropertyType.SET);

		Set<String> set = Sets.newHashSet("value");
		when(row.getSet("property", String.class)).thenReturn(set);

		Object actual = invoker.invokeOnRowForEagerFields(row, pm);

		assertThat(actual).isSameAs(set);
	}

	@Test
	public void should_get_map_value_from_row() throws Exception
	{
		when(pm.type()).thenReturn(PropertyType.MAP);
		when((Class<String>) pm.getJoinProperties().getEntityMeta().getIdMeta().getValueClass())
				.thenReturn(String.class);
		Map<Integer, String> map = ImmutableMap.of(11, "value");
		when(row.getMap("property", Integer.class, String.class)).thenReturn(map);

		Object actual = invoker.invokeOnRowForEagerFields(row, pm);

		assertThat(actual).isSameAs(map);
	}

	@Test
	public void should_get_simple_value_from_row() throws Exception
	{
		when(pm.type()).thenReturn(PropertyType.SIMPLE);

		when(row.getString("property")).thenReturn("value");

		Object actual = invoker.invokeOnRowForEagerFields(row, pm);

		assertThat(actual).isEqualTo("value");
	}

	@Test
	public void should_return_null_when_no_value() throws Exception
	{
		when(pm.type()).thenReturn(PropertyType.SIMPLE);
		when(row.isNull("property")).thenReturn(true);

		assertThat(invoker.invokeOnRowForEagerFields(row, pm)).isNull();
	}

	@Test
	public void should_return_null_when_not_eager_property_type() throws Exception
	{
		when(pm.type()).thenReturn(PropertyType.LAZY_SIMPLE);
		assertThat(invoker.invokeOnRowForEagerFields(row, pm)).isNull();
	}

	@Test
	public void should_exception_when_invoking_getter_from_row() throws Exception
	{
		when(pm.type()).thenReturn(PropertyType.SIMPLE);

		when(row.getString("property")).thenThrow(new RuntimeException(""));

		exception.expect(AchillesException.class);
		exception.expectMessage("Cannot retrieve property 'property' from CQL Row");

		invoker.invokeOnRowForEagerFields(row, pm);
	}

	@Test
	public void should_exception_when_invoking_list_getter_from_row() throws Exception
	{
		when(pm.type()).thenReturn(PropertyType.LIST);

		when(row.getList("property", String.class)).thenThrow(new RuntimeException(""));

		exception.expect(AchillesException.class);
		exception.expectMessage("Cannot retrieve list property 'property' from CQL Row");

		invoker.invokeOnRowForList(row, "property", String.class);
	}

	@Test
	public void should_exception_when_invoking_set_getter_from_row() throws Exception
	{
		when(pm.type()).thenReturn(PropertyType.SET);

		when(row.getSet("property", String.class)).thenThrow(new RuntimeException(""));

		exception.expect(AchillesException.class);
		exception.expectMessage("Cannot retrieve set property 'property' from CQL Row");

		invoker.invokeOnRowForSet(row, "property", String.class);
	}

	@Test
	public void should_exception_when_invoking_map_getter_from_row() throws Exception
	{
		when(pm.type()).thenReturn(PropertyType.MAP);
		when((Class<String>) pm.getJoinProperties().getEntityMeta().getIdMeta().getValueClass())
				.thenReturn(String.class);
		when(row.getMap("property", Integer.class, String.class)).thenThrow(
				new RuntimeException(""));

		exception.expect(AchillesException.class);
		exception.expectMessage("Cannot retrieve map property 'property' from CQL Row");

		invoker.invokeOnRowForMap(row, "property", Integer.class, String.class);
	}
}
