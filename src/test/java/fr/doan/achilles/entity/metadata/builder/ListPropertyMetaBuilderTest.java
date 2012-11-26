package fr.doan.achilles.entity.metadata.builder;

import static fr.doan.achilles.entity.metadata.builder.ListPropertyMetaBuilder.listPropertyMetaBuilder;
import static org.fest.assertions.api.Assertions.assertThat;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import fr.doan.achilles.entity.metadata.ListLazyPropertyMeta;
import fr.doan.achilles.entity.metadata.ListPropertyMeta;
import fr.doan.achilles.entity.metadata.PropertyType;
import fr.doan.achilles.exception.ValidationException;
import fr.doan.achilles.serializer.Utils;

public class ListPropertyMetaBuilderTest
{

	@Test
	public void should_build_list_property_meta() throws Exception
	{

		Method[] accessors = new Method[2];
		accessors[0] = Bean.class.getDeclaredMethod("getFriends", (Class<?>[]) null);
		accessors[1] = Bean.class.getDeclaredMethod("setFriends", List.class);

		ListPropertyMeta<String> meta = (ListPropertyMeta<String>) listPropertyMetaBuilder(String.class).propertyName("friends").accessors(accessors)
				.lazy(false).build();

		assertThat(meta.getPropertyName()).isEqualTo("friends");
		assertThat(meta.getValueClass()).isEqualTo(String.class);
		assertThat(meta.getValueSerializer().getComparatorType()).isEqualTo(Utils.STRING_SRZ.getComparatorType());
		assertThat(meta.getGetter()).isEqualTo(accessors[0]);
		assertThat(meta.getSetter()).isEqualTo(accessors[1]);
		assertThat(meta.newListInstance()).isInstanceOf(ArrayList.class);
		assertThat(meta.isLazy()).isEqualTo(false);
	}

	@Test
	public void should_build_lazy_list_property_meta() throws Exception
	{
		Method[] accessors = new Method[2];
		accessors[0] = Bean.class.getDeclaredMethod("getFriends", (Class<?>[]) null);
		accessors[1] = Bean.class.getDeclaredMethod("setFriends", List.class);

		ListPropertyMeta<String> meta = (ListPropertyMeta<String>) listPropertyMetaBuilder(String.class).propertyName("friends").accessors(accessors)
				.lazy(true).build();

		assertThat(meta.isLazy()).isEqualTo(true);
		assertThat(meta.propertyType()).isEqualTo(PropertyType.LAZY_LIST);
		assertThat(meta).isInstanceOf(ListLazyPropertyMeta.class);

	}

	@Test(expected = ValidationException.class)
	public void should_exception_when_missing_data() throws Exception
	{

		listPropertyMetaBuilder(String.class).propertyName("name").build();

	}

	class Bean
	{
		private List<String> friends;

		public List<String> getFriends()
		{
			return friends;
		}

		public void setFriends(List<String> friends)
		{
			this.friends = friends;
		}

	}
}
