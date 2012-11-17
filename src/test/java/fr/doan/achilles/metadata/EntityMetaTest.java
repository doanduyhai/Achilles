package fr.doan.achilles.metadata;

import static org.fest.assertions.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class EntityMetaTest
{
	@Test
	public void should_create_entity_meta() throws Exception
	{
		Map<String, PropertyMeta<?>> map = new HashMap<String, PropertyMeta<?>>();

		SimplePropertyMeta<String> simplePropertyMeta = new SimplePropertyMeta<String>("name", String.class);
		map.put("name", simplePropertyMeta);
		EntityMeta<Long> meta = new EntityMeta<Long>(Long.class, "test.MyClass", 1L, map);

		assertThat(meta.getAttributes()).hasSize(1);
		assertThat(meta.getAttributes().get("name")).isEqualTo(simplePropertyMeta);
		assertThat(meta.getCanonicalClassName()).isEqualTo("test.MyClass");
		assertThat(meta.getSerialVersionUID()).isEqualTo(1L);
		assertThat(meta.getColumnFamilyName()).isEqualTo("test_MyClass");
	}

	@Test
	public void should_create_entity_meta_with_column_family() throws Exception
	{
		Map<String, PropertyMeta<?>> map = new HashMap<String, PropertyMeta<?>>();

		SimplePropertyMeta<String> simplePropertyMeta = new SimplePropertyMeta<String>("name", String.class);
		map.put("name", simplePropertyMeta);
		EntityMeta<Long> meta = new EntityMeta<Long>(Long.class, "test.MyClass", "test_cf", 1L, map);

		assertThat(meta.getAttributes()).hasSize(1);
		assertThat(meta.getAttributes().get("name")).isEqualTo(simplePropertyMeta);
		assertThat(meta.getCanonicalClassName()).isEqualTo("test.MyClass");
		assertThat(meta.getSerialVersionUID()).isEqualTo(1L);
		assertThat(meta.getColumnFamilyName()).isEqualTo("test_cf");
	}
}
