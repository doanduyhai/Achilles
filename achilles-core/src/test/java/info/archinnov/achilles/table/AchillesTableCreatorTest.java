package info.archinnov.achilles.table;

import static info.archinnov.achilles.entity.metadata.EntityMetaBuilder.entityMetaBuilder;
import static info.archinnov.achilles.entity.metadata.PropertyType.SIMPLE;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.context.AchillesConfigurationContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.table.AchillesTableCreator;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import testBuilders.PropertyMetaTestBuilder;

/**
 * AchillesTableCreatorTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class AchillesTableCreatorTest
{

	@Mock
	private AchillesTableCreator creator;

	private Map<Class<?>, EntityMeta> entityMetaMap;

	private EntityMeta entityMeta;

	private PropertyMeta<Void, String> simplePropertyMeta;

	private PropertyMeta<Void, Long> idMeta;

	private AchillesConfigurationContext configContext = new AchillesConfigurationContext();

	@Before
	public void setUp() throws Exception
	{
		idMeta = PropertyMetaTestBuilder
				.noClass(Void.class, Long.class)
				.type(SIMPLE)
				.field("id")
				.build();
		configContext.setForceColumnFamilyCreation(true);
	}

	@Test
	public void should_validate_or_create_for_wide_map() throws Exception
	{
		PropertyMeta<Integer, String> wideMapMeta = PropertyMetaTestBuilder //
				.noClass(Integer.class, String.class)
				.field("externalWideMap")
				.externalTable("externalCF")
				.type(PropertyType.WIDE_MAP)
				.build();
		prepareData(wideMapMeta);
		idMeta.setValueClass(Long.class);

		doCallRealMethod().when(creator).validateOrCreateColumnFamilies(entityMetaMap,
				configContext, false);

		creator.validateOrCreateColumnFamilies(entityMetaMap, configContext, false);

		verify(creator).validateOrCreateCFForWideMap(wideMapMeta, Long.class, true, "externalCF",
				"TestBean");
	}

	@Test
	public void should_validate_or_create_for_entity() throws Exception
	{
		prepareData();

		doCallRealMethod().when(creator).validateOrCreateColumnFamilies(entityMetaMap,
				configContext, false);
		creator.validateOrCreateColumnFamilies(entityMetaMap, configContext, false);
		verify(creator).validateOrCreateCFForEntity(entityMeta, true);
	}

	@Test
	public void should_validate_or_create_for_counter() throws Exception
	{
		HashMap<Class<?>, EntityMeta> metaMap = new HashMap<Class<?>, EntityMeta>();
		doCallRealMethod().when(creator).validateOrCreateColumnFamilies(metaMap, configContext,
				true);
		creator.validateOrCreateColumnFamilies(metaMap, configContext, true);
		verify(creator).validateOrCreateCFForCounter(true);
	}

	private void prepareData(PropertyMeta<?, ?>... extraPropertyMetas) throws Exception
	{
		Map<String, PropertyMeta<?, ?>> propertyMetas = new HashMap<String, PropertyMeta<?, ?>>();

		for (PropertyMeta<?, ?> propertyMeta : extraPropertyMetas)
		{
			propertyMetas.put(propertyMeta.getPropertyName(), propertyMeta);
		}

		simplePropertyMeta = PropertyMetaTestBuilder
				.noClass(Void.class, String.class)
				.type(SIMPLE)
				.field("name")
				.build();

		propertyMetas.put("name", simplePropertyMeta);

		entityMeta = entityMetaBuilder(idMeta)
				.className("TestBean")
				.columnFamilyName("testCF")
				.serialVersionUID(1L)
				.propertyMetas(propertyMetas)
				.build();

		entityMetaMap = new HashMap<Class<?>, EntityMeta>();
		entityMetaMap.put(this.getClass(), entityMeta);
	}
}
