package info.archinnov.achilles.entity;

import static org.mockito.Mockito.*;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.proxy.MethodInvoker;
import info.archinnov.achilles.proxy.CQLRowMethodInvoker;

import java.util.Arrays;
import java.util.List;

import mapping.entity.CompleteBean;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import testBuilders.CompleteBeanTestBuilder;
import testBuilders.PropertyMetaTestBuilder;

import com.datastax.driver.core.Row;

/**
 * CQLEntityMapperTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class CQLEntityMapperTest
{

	@InjectMocks
	private CQLEntityMapper mapper;

	@Mock
	private MethodInvoker invoker;

	@Mock
	private CQLRowMethodInvoker cqlRowInvoker;

	@Mock
	private Row row;

	@Mock
	private EntityMeta entityMeta;

	private CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().buid();

	@Test
	public void should_set_eager_properties_to_entity() throws Exception
	{
		PropertyMeta<?, ?> pm = PropertyMetaTestBuilder
				.completeBean(Void.class, String.class)
				.field("name")
				.accessors()
				.type(PropertyType.SIMPLE)
				.build();

		List<PropertyMeta<?, ?>> eagerMetas = Arrays.<PropertyMeta<?, ?>> asList(pm);

		when(entityMeta.getEagerMetas()).thenReturn(eagerMetas);

		when(row.isNull("name")).thenReturn(false);
		when(cqlRowInvoker.invokeOnRowForEagerFields(row, pm)).thenReturn("value");

		mapper.setEagerPropertiesToEntity(row, entityMeta, entity);

		verify(invoker).setValueToField(entity, pm.getSetter(), "value");
	}

	@Test
	public void should_set_null_to_entity_when_no_value_from_row() throws Exception
	{
		PropertyMeta<?, ?> pm = PropertyMetaTestBuilder
				.completeBean(Void.class, String.class)
				.field("name")
				.accessors()
				.type(PropertyType.SIMPLE)
				.build();

		List<PropertyMeta<?, ?>> eagerMetas = Arrays.<PropertyMeta<?, ?>> asList(pm);

		when(entityMeta.getEagerMetas()).thenReturn(eagerMetas);

		when(row.isNull("name")).thenReturn(true);

		mapper.setEagerPropertiesToEntity(row, entityMeta, entity);

		verifyZeroInteractions(cqlRowInvoker, invoker);
	}

	@Test
	public void should_do_nothing_when_null_row() throws Exception
	{
		PropertyMeta<?, ?> pm = PropertyMetaTestBuilder
				.completeBean(Void.class, String.class)
				.field("name")
				.accessors()
				.type(PropertyType.SIMPLE)
				.build();

		mapper.setPropertyToEntity((Row) null, pm, entity);

		verifyZeroInteractions(cqlRowInvoker, invoker);
	}

	@Test
	public void should_set_property_to_entity() throws Exception
	{
		PropertyMeta<?, ?> pm = PropertyMetaTestBuilder
				.completeBean(Void.class, String.class)
				.field("name")
				.accessors()
				.type(PropertyType.SIMPLE)
				.build();

		mapper.setJoinValueToEntity("name", pm, entity);

		verify(invoker).setValueToField(entity, pm.getSetter(), "name");
	}

}
