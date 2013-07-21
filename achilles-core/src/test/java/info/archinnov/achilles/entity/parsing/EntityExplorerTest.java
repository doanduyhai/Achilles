package info.archinnov.achilles.entity.parsing;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.test.more.entity.Entity3;
import info.archinnov.achilles.test.sample.entity.Entity1;
import info.archinnov.achilles.test.sample.entity.Entity2;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

/**
 * AchillesEntityExplorerTest
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntityExplorerTest {
    private EntityExplorer explorer = new EntityExplorer();

    @Test
    public void should_find_entities_from_multiple_packages() throws Exception {
        List<Class<?>> entities = explorer.discoverEntities(Arrays.asList(
                "info.archinnov.achilles.test.sample.entity", "info.archinnov.achilles.test.more.entity"));

        assertThat(entities).hasSize(3);
        assertThat(entities).contains(Entity1.class);
        assertThat(entities).contains(Entity2.class);
        assertThat(entities).contains(Entity3.class);
    }

    @Test
    public void should_find_entity_from_one_package() throws Exception {
        List<Class<?>> entities = explorer
                .discoverEntities(Arrays.asList("info.archinnov.achilles.test.more.entity"));
        assertThat(entities).hasSize(1);
        assertThat(entities).contains(Entity3.class);

    }

}
