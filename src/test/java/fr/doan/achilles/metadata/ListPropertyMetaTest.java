package fr.doan.achilles.metadata;

import static fr.doan.achilles.metadata.PropertyType.LIST;
import static org.fest.assertions.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

public class ListPropertyMetaTest
{

	@Test
	public void should_create_new_ArrayList_instance() throws Exception
	{
		ListPropertyMeta<String> listPropertyMeta = new ListPropertyMeta<String>("name", String.class, ArrayList.class);

		assertThat(listPropertyMeta.newListInstance()).isNotNull();
		assertThat(listPropertyMeta.newListInstance()).isEmpty();
		assertThat(listPropertyMeta.newListInstance() instanceof ArrayList).isTrue();
		assertThat(listPropertyMeta.propertyType()).isEqualTo(LIST);
	}

	@Test
	public void should_create_new_default_list_instance() throws Exception
	{
		ListPropertyMeta<String> listPropertyMeta = new ListPropertyMeta<String>("name", String.class, List.class);

		assertThat(listPropertyMeta.newListInstance()).isNotNull();
		assertThat(listPropertyMeta.newListInstance()).isEmpty();
		assertThat(listPropertyMeta.newListInstance() instanceof ArrayList).isTrue();
		assertThat(listPropertyMeta.propertyType()).isEqualTo(LIST);
	}
}
