package info.archinnov.achilles.entity.operations;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.exception.AchillesException;
import integration.tests.entity.CompleteBean;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.ImmutableMap;

/**
 * EntityInitializerTest
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntityInitializerTest
{
	@Rule
	public ExpectedException exception = ExpectedException.none();

	private EntityInitializer initializer = new EntityInitializer();

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Test
	public void should_initialize_entity() throws Exception
	{
		final List<String> calledMethods = new ArrayList<String>();
		CompleteBean bean = new CompleteBean()
		{
			private static final long serialVersionUID = 1L;

			public Long getId()
			{
				calledMethods.add("getId");
				return 10L;
			}
		};

		PropertyMeta<Void, Long> propertyMeta = new PropertyMeta<Void, Long>();
		propertyMeta.setType(PropertyType.LAZY_SIMPLE);
		propertyMeta.setGetter(bean.getClass().getDeclaredMethod("getId"));
		EntityMeta<Long> entityMeta = new EntityMeta<Long>();
		entityMeta.setPropertyMetas((Map) ImmutableMap.of("id", propertyMeta));

		initializer.initializeEntity(bean, entityMeta);

		assertThat(calledMethods).containsExactly("getId");
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Test
	public void should_not_initialize_entity_if_not_lazy() throws Exception
	{
		final List<String> calledMethods = new ArrayList<String>();
		CompleteBean bean = new CompleteBean()
		{
			private static final long serialVersionUID = 1L;

			public Long getId()
			{
				calledMethods.add("getId");
				return 10L;
			}
		};

		PropertyMeta<Void, Long> propertyMeta = new PropertyMeta<Void, Long>();
		propertyMeta.setType(PropertyType.SIMPLE);
		propertyMeta.setGetter(bean.getClass().getDeclaredMethod("getId"));
		EntityMeta<Long> entityMeta = new EntityMeta<Long>();
		entityMeta.setPropertyMetas((Map) ImmutableMap.of("id", propertyMeta));

		initializer.initializeEntity(bean, entityMeta);

		assertThat(calledMethods).isEmpty();
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Test
	public void should_not_initialize_entity_if_widemap() throws Exception
	{
		final List<String> calledMethods = new ArrayList<String>();
		CompleteBean bean = new CompleteBean()
		{
			private static final long serialVersionUID = 1L;

			public Long getId()
			{
				calledMethods.add("getId");
				return 10L;
			}
		};

		PropertyMeta<Void, Long> propertyMeta = new PropertyMeta<Void, Long>();
		propertyMeta.setType(PropertyType.WIDE_MAP);
		EntityMeta<Long> entityMeta = new EntityMeta<Long>();
		entityMeta.setPropertyMetas((Map) ImmutableMap.of("id", propertyMeta));

		initializer.initializeEntity(bean, entityMeta);

		assertThat(calledMethods).isEmpty();
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Test
	public void should_throw_exception_when_error_initializing() throws Exception
	{
		CompleteBean bean = new CompleteBean()
		{
			private static final long serialVersionUID = 1L;

			public Long getId()
			{
				throw new RuntimeException();
			}
		};

		PropertyMeta<Void, Long> propertyMeta = new PropertyMeta<Void, Long>();
		propertyMeta.setType(PropertyType.LAZY_SIMPLE);
		EntityMeta<Long> entityMeta = new EntityMeta<Long>();
		entityMeta.setPropertyMetas((Map) ImmutableMap.of("id", propertyMeta));

		exception.expect(AchillesException.class);
		initializer.initializeEntity(bean, entityMeta);
	}
}
