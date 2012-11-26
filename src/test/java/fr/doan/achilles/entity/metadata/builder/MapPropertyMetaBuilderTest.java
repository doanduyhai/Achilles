package fr.doan.achilles.entity.metadata.builder;

import static fr.doan.achilles.entity.metadata.builder.MapPropertyMetaBuilder.mapPropertyMetaBuilder;
import static org.fest.assertions.api.Assertions.assertThat;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import fr.doan.achilles.entity.metadata.MapLazyPropertyMeta;
import fr.doan.achilles.entity.metadata.MapPropertyMeta;
import fr.doan.achilles.entity.metadata.PropertyType;
import fr.doan.achilles.exception.ValidationException;
import fr.doan.achilles.serializer.Utils;

public class MapPropertyMetaBuilderTest
{

	@SuppressWarnings("unchecked")
	@Test
	public void should_build_map_property_meta() throws Exception
	{

		Method[] accessors = new Method[2];
		accessors[0] = Bean.class.getDeclaredMethod("getPreferences", (Class<?>[]) null);
		accessors[1] = Bean.class.getDeclaredMethod("setPreferences", Map.class);

		MapPropertyMeta<Integer, String> meta = (MapPropertyMeta<Integer, String>) mapPropertyMetaBuilder(Integer.class, String.class)
				.propertyName("preferences").accessors(accessors).lazy(false).build();

		assertThat(meta.getPropertyName()).isEqualTo("preferences");
		assertThat(meta.getValueClass()).isEqualTo(String.class);
		assertThat(meta.getValueSerializer().getComparatorType()).isEqualTo(Utils.STRING_SRZ.getComparatorType());
		assertThat(meta.getGetter()).isEqualTo(accessors[0]);
		assertThat(meta.getSetter()).isEqualTo(accessors[1]);

		assertThat(meta.getKeyClass()).isEqualTo(Integer.class);
		assertThat(meta.getKeySerializer().getComparatorType()).isEqualTo(Utils.INT_SRZ.getComparatorType());
		assertThat(meta.newMapInstance()).isInstanceOf(HashMap.class);
		assertThat(meta.isLazy()).isEqualTo(false);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_build_lazy_map_property_meta() throws Exception
	{
		Method[] accessors = new Method[2];
		accessors[0] = Bean.class.getDeclaredMethod("getPreferences", (Class<?>[]) null);
		accessors[1] = Bean.class.getDeclaredMethod("setPreferences", Map.class);

		MapPropertyMeta<Integer, String> meta = (MapPropertyMeta<Integer, String>) mapPropertyMetaBuilder(Integer.class, String.class)
				.propertyName("preferences").accessors(accessors).lazy(true).build();

		assertThat(meta.isLazy()).isEqualTo(true);
		assertThat(meta.propertyType()).isEqualTo(PropertyType.LAZY_MAP);
		assertThat(meta).isInstanceOf(MapLazyPropertyMeta.class);

	}

	@Test(expected = ValidationException.class)
	public void should_exception_when_missing_data() throws Exception
	{

		mapPropertyMetaBuilder(Integer.class, String.class).propertyName("name").build();

	}

	class Bean
	{
		private Map<Integer, String> preferences;

		public Map<Integer, String> getPreferences()
		{
			return preferences;
		}

		public void setPreferences(Map<Integer, String> preferences)
		{
			this.preferences = preferences;
		}
	}
}
