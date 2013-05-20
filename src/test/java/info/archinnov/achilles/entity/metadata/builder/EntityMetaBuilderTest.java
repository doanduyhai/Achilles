package info.archinnov.achilles.entity.metadata.builder;

import static info.archinnov.achilles.entity.metadata.PropertyType.SIMPLE;
import static info.archinnov.achilles.entity.metadata.builder.EntityMetaBuilder.entityMetaBuilder;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import me.prettyprint.cassandra.model.ExecutingKeyspace;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import parser.entity.Bean;

/**
 * EntityMetaBuilderTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class EntityMetaBuilderTest
{

	@Mock
	private ExecutingKeyspace keyspace;

	@Mock
	private ThriftGenericEntityDao dao;

	@Mock
	private PropertyMeta<Void, Long> idMeta;

	@Test
	public void should_build_meta() throws Exception
	{

		Map<String, PropertyMeta<?, ?>> propertyMetas = new HashMap<String, PropertyMeta<?, ?>>();
		PropertyMeta<Void, String> simpleMeta = new PropertyMeta<Void, String>();
		simpleMeta.setType(SIMPLE);

		Method getter = Bean.class.getDeclaredMethod("getName", (Class<?>[]) null);
		simpleMeta.setGetter(getter);

		Method setter = Bean.class.getDeclaredMethod("setName", String.class);
		simpleMeta.setSetter(setter);

		propertyMetas.put("name", simpleMeta);

		when(idMeta.getValueClass()).thenReturn(Long.class);

		EntityMeta meta = entityMetaBuilder(idMeta)
				.className("Bean")
				.serialVersionUID(1L)
				.columnFamilyName("cfName")
				.propertyMetas(propertyMetas)
				.build();

		assertThat(meta.getClassName()).isEqualTo("Bean");
		assertThat(meta.getTableName()).isEqualTo("cfName");
		assertThat((PropertyMeta<Void, Long>) meta.getIdMeta()).isSameAs(idMeta);
		assertThat((Class<Long>) meta.getIdClass()).isEqualTo(Long.class);
		assertThat(meta.getPropertyMetas()).containsKey("name");
		assertThat(meta.getPropertyMetas()).containsValue(simpleMeta);

		assertThat(meta.getGetterMetas()).hasSize(1);
		assertThat(meta.getGetterMetas().containsKey(getter));
		assertThat(meta.getGetterMetas().get(getter)).isSameAs((PropertyMeta) simpleMeta);

		assertThat(meta.getSetterMetas()).hasSize(1);
		assertThat(meta.getSetterMetas().containsKey(setter));
		assertThat(meta.getSetterMetas().get(setter)).isSameAs((PropertyMeta) simpleMeta);

	}

	@Test
	public void should_build_meta_with_column_family_name() throws Exception
	{

		Map<String, PropertyMeta<?, ?>> propertyMetas = new HashMap<String, PropertyMeta<?, ?>>();
		PropertyMeta<Void, String> simpleMeta = new PropertyMeta<Void, String>();
		simpleMeta.setType(SIMPLE);
		propertyMetas.put("name", simpleMeta);

		when(idMeta.getValueClass()).thenReturn(Long.class);

		EntityMeta meta = entityMetaBuilder(idMeta)
				.className("Bean")
				.serialVersionUID(1L)
				.propertyMetas(propertyMetas)
				.columnFamilyName("toto")
				.build();

		assertThat(meta.getClassName()).isEqualTo("Bean");
		assertThat(meta.getTableName()).isEqualTo("toto");
	}

	@Test
	public void should_build_meta_for_wide_row() throws Exception
	{

		Map<String, PropertyMeta<?, ?>> propertyMetas = new HashMap<String, PropertyMeta<?, ?>>();
		PropertyMeta<Integer, String> wideMapMeta = new PropertyMeta<Integer, String>();
		wideMapMeta.setValueClass(String.class);
		wideMapMeta.setType(PropertyType.WIDE_MAP);
		propertyMetas.put("name", wideMapMeta);

		when(idMeta.getValueClass()).thenReturn(Long.class);

		EntityMeta meta = entityMetaBuilder(idMeta)
				.className("Bean")
				.serialVersionUID(1L)
				.propertyMetas(propertyMetas)
				.columnFamilyName("toto")
				.wideRow(true)
				.build();

		assertThat(meta.isWideRow()).isTrue();
	}
}
