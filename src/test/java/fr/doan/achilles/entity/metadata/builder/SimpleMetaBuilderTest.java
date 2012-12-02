package fr.doan.achilles.entity.metadata.builder;

import static fr.doan.achilles.entity.metadata.builder.SimpleMetaBuilder.simpleMetaBuilder;
import static org.fest.assertions.api.Assertions.assertThat;

import java.lang.reflect.Method;

import org.junit.Test;

import fr.doan.achilles.entity.metadata.PropertyType;
import fr.doan.achilles.entity.metadata.SimpleLazyMeta;
import fr.doan.achilles.entity.metadata.SimpleMeta;
import fr.doan.achilles.exception.ValidationException;
import fr.doan.achilles.serializer.Utils;

public class SimpleMetaBuilderTest
{

	@Test
	public void should_build_property_meta() throws Exception
	{

		Method[] accessors = new Method[2];
		accessors[0] = Bean.class.getDeclaredMethod("getName", (Class<?>[]) null);
		accessors[1] = Bean.class.getDeclaredMethod("setName", String.class);

		SimpleMeta<String> meta = simpleMetaBuilder(String.class).propertyName("name").accessors(accessors).lazy(false).build();

		assertThat(meta.getPropertyName()).isEqualTo("name");
		assertThat(meta.getValueClass()).isEqualTo(String.class);
		assertThat(meta.getValueSerializer().getComparatorType()).isEqualTo(Utils.STRING_SRZ.getComparatorType());
		assertThat(meta.getGetter()).isEqualTo(accessors[0]);
		assertThat(meta.getSetter()).isEqualTo(accessors[1]);
		assertThat(meta.isLazy()).isEqualTo(false);

	}

	@Test
	public void should_build_lazy_property_meta() throws Exception
	{
		Method[] accessors = new Method[2];
		accessors[0] = Bean.class.getDeclaredMethod("getName", (Class<?>[]) null);
		accessors[1] = Bean.class.getDeclaredMethod("setName", String.class);

		SimpleMeta<String> meta = simpleMetaBuilder(String.class).propertyName("name").accessors(accessors).lazy(true).build();

		assertThat(meta.isLazy()).isEqualTo(true);
		assertThat(meta.propertyType()).isEqualTo(PropertyType.LAZY_SIMPLE);
		assertThat(meta).isInstanceOf(SimpleLazyMeta.class);

	}

	@Test(expected = ValidationException.class)
	public void should_exception_when_missing_data() throws Exception
	{

		simpleMetaBuilder(String.class).propertyName("name").build();

	}

	class Bean
	{
		private String name;

		public String getName()
		{
			return name;
		}

		public void setName(String name)
		{
			this.name = name;
		}

	}
}
