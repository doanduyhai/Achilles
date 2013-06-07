package info.archinnov.achilles.entity.operations;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.exception.AchillesException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mapping.entity.CompleteBean;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * AchillesEntityInitializerTest
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntityInitializerTest
{
	@Rule
	public ExpectedException exception = ExpectedException.none();

	private EntityInitializer initializer = new EntityInitializer();

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
		EntityMeta entityMeta = new EntityMeta();
		entityMeta.setPropertyMetas(buildPropertyMetaMaps("id", propertyMeta));

		initializer.initializeEntity(bean, entityMeta);

		assertThat(calledMethods).containsExactly("getId");
	}

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
		EntityMeta entityMeta = new EntityMeta();
		entityMeta.setPropertyMetas(buildPropertyMetaMaps("id", propertyMeta));

		initializer.initializeEntity(bean, entityMeta);

		assertThat(calledMethods).isEmpty();
	}

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
		EntityMeta entityMeta = new EntityMeta();
		entityMeta.setPropertyMetas(buildPropertyMetaMaps("id", propertyMeta));

		initializer.initializeEntity(bean, entityMeta);

		assertThat(calledMethods).isEmpty();
	}

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
		EntityMeta entityMeta = new EntityMeta();
		entityMeta.setPropertyMetas(buildPropertyMetaMaps("id", propertyMeta));

		exception.expect(AchillesException.class);
		initializer.initializeEntity(bean, entityMeta);
	}

	private Map<String, PropertyMeta<?, ?>> buildPropertyMetaMaps(String prop,
			PropertyMeta<?, ?> propertyMeta)
	{
		Map<String, PropertyMeta<?, ?>> map = new HashMap<String, PropertyMeta<?, ?>>();
		map.put(prop, propertyMeta);
		return map;
	}
}
