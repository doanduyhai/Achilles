package fr.doan.achilles.entity.metadata.builder;

import static fr.doan.achilles.entity.metadata.builder.EntityMetaBuilder.entityMetaBuilder;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import me.prettyprint.cassandra.model.ExecutingKeyspace;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import parser.entity.Bean;
import parser.entity.CorrectMultiKey;
import fr.doan.achilles.dao.GenericEntityDao;
import fr.doan.achilles.dao.GenericMultiKeyWideRowDao;
import fr.doan.achilles.dao.GenericWideRowDao;
import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.entity.metadata.MultiKeyWideMapMeta;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.metadata.SimpleMeta;
import fr.doan.achilles.entity.metadata.WideMapMeta;
import fr.doan.achilles.serializer.Utils;

@RunWith(MockitoJUnitRunner.class)
public class EntityMetaBuilderTest
{

	@Mock
	private ExecutingKeyspace keyspace;

	@Mock
	private GenericEntityDao<?> dao;

	@Mock
	private PropertyMeta<Void, Long> idMeta;

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Test
	public void should_build_meta() throws Exception
	{

		Map<String, PropertyMeta<?, ?>> propertyMetas = new HashMap<String, PropertyMeta<?, ?>>();
		SimpleMeta<String> simpleMeta = new SimpleMeta<String>();
		Method getter = Bean.class.getDeclaredMethod("getName", (Class<?>[]) null);
		simpleMeta.setGetter(getter);

		Method setter = Bean.class.getDeclaredMethod("setName", String.class);
		simpleMeta.setSetter(setter);

		propertyMetas.put("name", simpleMeta);

		when(idMeta.getValueClass()).thenReturn(Long.class);

		EntityMeta<Long> meta = entityMetaBuilder(idMeta).canonicalClassName("fr.doan.Bean")
				.serialVersionUID(1L).propertyMetas(propertyMetas).keyspace(keyspace).build();

		assertThat(meta.getCanonicalClassName()).isEqualTo("fr.doan.Bean");
		assertThat(meta.getColumnFamilyName()).isEqualTo("fr_doan_Bean");
		assertThat(meta.getIdMeta()).isSameAs(idMeta);
		assertThat(meta.getIdSerializer().getComparatorType()).isEqualTo(
				Utils.LONG_SRZ.getComparatorType());
		assertThat(meta.getPropertyMetas()).containsKey("name");
		assertThat(meta.getPropertyMetas()).containsValue(simpleMeta);

		assertThat(meta.getGetterMetas()).hasSize(1);
		assertThat(meta.getGetterMetas().containsKey(getter));
		assertThat(meta.getGetterMetas().get(getter)).isSameAs((PropertyMeta) simpleMeta);

		assertThat(meta.getSetterMetas()).hasSize(1);
		assertThat(meta.getSetterMetas().containsKey(setter));
		assertThat(meta.getSetterMetas().get(setter)).isSameAs((PropertyMeta) simpleMeta);

		assertThat(meta.getEntityDao()).isNotNull();

	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_build_meta_with_column_family_name() throws Exception
	{

		Map<String, PropertyMeta<?, ?>> propertyMetas = new HashMap<String, PropertyMeta<?, ?>>();
		SimpleMeta<String> simpleMeta = new SimpleMeta<String>();
		propertyMetas.put("name", simpleMeta);

		when(idMeta.getValueClass()).thenReturn(Long.class);

		EntityMeta<Long> meta = entityMetaBuilder(idMeta).canonicalClassName("fr.doan.Bean")
				.serialVersionUID(1L).propertyMetas(propertyMetas).columnFamilyName("toto")
				.keyspace(keyspace).build();

		assertThat(meta.getCanonicalClassName()).isEqualTo("fr.doan.Bean");
		assertThat(meta.getColumnFamilyName()).isEqualTo("toto");
		assertThat(meta.getWideRowMultiKeyDao()).isNull();
		assertThat(meta.getEntityDao()).isExactlyInstanceOf(GenericEntityDao.class);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_build_meta_for_wide_row() throws Exception
	{

		Map<String, PropertyMeta<?, ?>> propertyMetas = new HashMap<String, PropertyMeta<?, ?>>();
		WideMapMeta<Integer, String> wideMapMeta = new WideMapMeta<Integer, String>();
		propertyMetas.put("name", wideMapMeta);

		when(idMeta.getValueClass()).thenReturn(Long.class);

		EntityMeta<Long> meta = entityMetaBuilder(idMeta).canonicalClassName("fr.doan.Bean")
				.serialVersionUID(1L).propertyMetas(propertyMetas).columnFamilyName("toto")
				.keyspace(keyspace).wideRow(true).build();

		assertThat(meta.isWideRow()).isTrue();
		assertThat(meta.getEntityDao()).isNull();
		assertThat(meta.getWideRowMultiKeyDao()).isNull();
		assertThat(meta.getWideRowDao()).isExactlyInstanceOf(GenericWideRowDao.class);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_build_meta_for_multikey_wide_row() throws Exception
	{

		Map<String, PropertyMeta<?, ?>> propertyMetas = new HashMap<String, PropertyMeta<?, ?>>();
		MultiKeyWideMapMeta<CorrectMultiKey, String> wideMapMeta = new MultiKeyWideMapMeta<CorrectMultiKey, String>();

		propertyMetas.put("name", wideMapMeta);

		when(idMeta.getValueClass()).thenReturn(Long.class);

		EntityMeta<Long> meta = entityMetaBuilder(idMeta).canonicalClassName("fr.doan.Bean")
				.serialVersionUID(1L).propertyMetas(propertyMetas).columnFamilyName("toto")
				.keyspace(keyspace).wideRow(true).build();

		assertThat(meta.isWideRow()).isTrue();
		assertThat(meta.getEntityDao()).isNull();
		assertThat(meta.getWideRowDao()).isNull();
		assertThat(meta.getWideRowMultiKeyDao()).isExactlyInstanceOf(
				GenericMultiKeyWideRowDao.class);
	}
}
