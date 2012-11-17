package fr.doan.achilles.metadata;

import static fr.doan.achilles.metadata.PropertyType.MAP;
import static org.fest.assertions.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import fr.doan.achilles.serializer.Utils;

public class MapPropertyMetaTest
{
	@Test
	public void should_create_new_HashMap_instance() throws Exception
	{
		MapPropertyMeta<String> mapPropertyMeta = new MapPropertyMeta<String>("name", Integer.class, String.class, HashMap.class);

		assertThat(mapPropertyMeta.getKeyClass()).isEqualTo(Integer.class);
		assertThat(mapPropertyMeta.getKeyClassSerializer()).isEqualTo(Utils.INT_SRZ);
		assertThat(mapPropertyMeta.newMapInstance()).isNotNull();
		assertThat(mapPropertyMeta.newMapInstance()).isEmpty();
		assertThat(mapPropertyMeta.newMapInstance() instanceof HashMap).isTrue();
		assertThat(mapPropertyMeta.propertyType()).isEqualTo(MAP);
	}

	@Test
	public void should_create_new_default_set_instance() throws Exception
	{
		MapPropertyMeta<String> mapPropertyMeta = new MapPropertyMeta<String>("name", Integer.class, String.class, Map.class);

		assertThat(mapPropertyMeta.getKeyClass()).isEqualTo(Integer.class);
		assertThat(mapPropertyMeta.getKeyClassSerializer()).isEqualTo(Utils.INT_SRZ);
		assertThat(mapPropertyMeta.newMapInstance()).isNotNull();
		assertThat(mapPropertyMeta.newMapInstance()).isEmpty();
		assertThat(mapPropertyMeta.newMapInstance() instanceof HashMap).isTrue();
		assertThat(mapPropertyMeta.propertyType()).isEqualTo(MAP);
	}
}
