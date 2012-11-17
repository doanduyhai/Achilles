package fr.doan.achilles.parser;

import static org.fest.assertions.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;

import more.entity.Entity3;

import org.junit.Test;

import sample.entity.Entity1;
import sample.entity.Entity2;

public class EntityExplorerTest
{
	private EntityExplorer explorer = new EntityExplorer();

	@Test
	public void should_find_entities_from_single_package() throws Exception
	{
		List<Class<?>> entities = explorer.discoverEntities("sample.entity");

		assertThat(entities).hasSize(2);
		assertThat(entities).contains(Entity1.class);
		assertThat(entities).contains(Entity2.class);
	}

	@Test
	public void should_find_entities_from_multiple_packages() throws Exception
	{
		List<Class<?>> entities = explorer.discoverEntities(Arrays.asList("sample.entity", "more.entity"));

		assertThat(entities).hasSize(3);
		assertThat(entities).contains(Entity1.class);
		assertThat(entities).contains(Entity2.class);
		assertThat(entities).contains(Entity3.class);
	}
}
