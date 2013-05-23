package info.archinnov.achilles.entity.parsing;

import static org.fest.assertions.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;

import more.entity.Entity3;

import org.junit.Test;

import sample.entity.Entity1;
import sample.entity.Entity2;

/**
 * AchillesEntityExplorerTest
 * 
 * @author DuyHai DOAN
 * 
 */
public class AchillesEntityExplorerTest
{
	private AchillesEntityExplorer explorer = new AchillesEntityExplorer();

	@Test
	public void should_find_entities_from_multiple_packages() throws Exception
	{
		List<Class<?>> entities = explorer.discoverEntities(Arrays.asList("sample.entity",
				"more.entity"));

		assertThat(entities).hasSize(3);
		assertThat(entities).contains(Entity1.class);
		assertThat(entities).contains(Entity2.class);
		assertThat(entities).contains(Entity3.class);
	}

	@Test
	public void should_find_entity_from_one_package() throws Exception
	{
		List<Class<?>> entities = explorer.discoverEntities(Arrays.asList("more.entity"));
		assertThat(entities).hasSize(1);
		assertThat(entities).contains(Entity3.class);

	}

}
