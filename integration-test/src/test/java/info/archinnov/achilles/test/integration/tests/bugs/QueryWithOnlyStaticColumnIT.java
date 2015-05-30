package info.archinnov.achilles.test.integration.tests.bugs;

import static org.fest.assertions.api.Assertions.assertThat;

import com.datastax.driver.core.SimpleStatement;
import info.archinnov.achilles.junit.AchillesTestResource;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.test.integration.AchillesInternalCQLResource;
import info.archinnov.achilles.test.integration.entity.ClusteredEntityWithStaticColumn;
import info.archinnov.achilles.test.integration.entity.ClusteredEntityWithStaticCounter;
import info.archinnov.achilles.type.TypedMap;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;

public class QueryWithOnlyStaticColumnIT {

    @Rule
    public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(AchillesTestResource.Steps.AFTER_TEST,
            ClusteredEntityWithStaticColumn.TABLE_NAME, ClusteredEntityWithStaticCounter.TABLE_NAME);

    private PersistenceManager manager = resource.getPersistenceManager();

    @Test
    public void should_slice_query_with_only_static_column_no_clustering() throws Exception {
        //Given
        Long partitionKey = RandomUtils.nextLong(0, Long.MAX_VALUE);
        ClusteredEntityWithStaticColumn parisStreet = new ClusteredEntityWithStaticColumn(new ClusteredEntityWithStaticColumn.ClusteredKey(partitionKey, null), "Paris", null);

        manager.insert(parisStreet);

        //When
        final List<ClusteredEntityWithStaticColumn> actual = manager.sliceQuery(ClusteredEntityWithStaticColumn.class).forSelect()
                .withPartitionComponents(partitionKey)
                .get(10);


        //Then
        assertThat(actual).hasSize(1);
        final ClusteredEntityWithStaticColumn found = actual.get(0);
        assertThat(found.getId().getId()).isEqualTo(partitionKey);
        assertThat(found.getId().getName()).isNull();
        assertThat(found.getCity()).isEqualTo("Paris");
        assertThat(found.getStreet()).isNull();
    }

    @Test
    public void should_slice_query_with_only_static_column_no_clustering_with_proxy() throws Exception {
        //Given
        Long partitionKey = RandomUtils.nextLong(0, Long.MAX_VALUE);
        ClusteredEntityWithStaticColumn parisStreet = new ClusteredEntityWithStaticColumn(new ClusteredEntityWithStaticColumn.ClusteredKey(partitionKey, null), "Paris", null);

        manager.insert(parisStreet);

        //When
        final List<ClusteredEntityWithStaticColumn> actual = manager.sliceQuery(ClusteredEntityWithStaticColumn.class).forSelect()
                .withPartitionComponents(partitionKey)
                .withProxy()
                .get(10);


        //Then
        assertThat(actual).hasSize(1);
        final ClusteredEntityWithStaticColumn found = actual.get(0);
        assertThat(found.getId().getId()).isEqualTo(partitionKey);
        assertThat(found.getId().getName()).isNull();
        assertThat(found.getCity()).isEqualTo("Paris");
        assertThat(found.getStreet()).isNull();
    }

    @Test
    public void should_update_only_static_column_no_clustering() throws Exception {
        //Given
        Long partitionKey = RandomUtils.nextLong(0, Long.MAX_VALUE);
        ClusteredEntityWithStaticColumn parisStreet = new ClusteredEntityWithStaticColumn(new ClusteredEntityWithStaticColumn.ClusteredKey(partitionKey, null), "Paris", null);

        manager.insert(parisStreet);

        //When
        final List<ClusteredEntityWithStaticColumn> actual = manager.sliceQuery(ClusteredEntityWithStaticColumn.class).forSelect()
                .withPartitionComponents(partitionKey)
                .withProxy()
                .get(10);

        final ClusteredEntityWithStaticColumn proxifiedEntity = actual.get(0);
        proxifiedEntity.setCity("London");
        manager.update(proxifiedEntity);

        //Then
        final TypedMap typedMap = manager.nativeQuery(new SimpleStatement("SELECT city FROM " + ClusteredEntityWithStaticColumn.TABLE_NAME + " WHERE id = " + partitionKey)).getFirst();
        assertThat(typedMap).hasSize(1);
        assertThat(typedMap.<String>getTyped("city")).isEqualTo("London");
    }

    @Test
    public void should_typed_query_with_only_static_column() throws Exception {
        //Given
        Long partitionKey = RandomUtils.nextLong(0, Long.MAX_VALUE);
        ClusteredEntityWithStaticColumn parisStreet = new ClusteredEntityWithStaticColumn(new ClusteredEntityWithStaticColumn.ClusteredKey(partitionKey, null), "Paris", null);

        manager.insert(parisStreet);

        //When
        final ClusteredEntityWithStaticColumn found = manager.typedQuery(ClusteredEntityWithStaticColumn.class, new SimpleStatement("SELECT id,city FROM " + ClusteredEntityWithStaticColumn.TABLE_NAME + " WHERE id = ?"), partitionKey).getFirst();

        //Then
        assertThat(found).isNotNull();
        assertThat(found.getCity()).isEqualTo("Paris");
    }

    @Test
    public void should_update_proxified_entity_from_typed_query_with_only_static_column() throws Exception {
        //Given
        Long partitionKey = RandomUtils.nextLong(0, Long.MAX_VALUE);
        ClusteredEntityWithStaticColumn parisStreet = new ClusteredEntityWithStaticColumn(new ClusteredEntityWithStaticColumn.ClusteredKey(partitionKey, null), "Paris", null);

        manager.insert(parisStreet);
        final ClusteredEntityWithStaticColumn found = manager.typedQuery(ClusteredEntityWithStaticColumn.class, new SimpleStatement("SELECT id,city FROM " + ClusteredEntityWithStaticColumn.TABLE_NAME + " WHERE id = ?"), partitionKey).withProxy().getFirst();

        //When
        found.setCity("London");
        manager.update(found);

        //Then
        final TypedMap typedMap = manager.nativeQuery(new SimpleStatement("SELECT city FROM " + ClusteredEntityWithStaticColumn.TABLE_NAME + " WHERE id = " + partitionKey)).getFirst();
        assertThat(typedMap).hasSize(1);
        assertThat(typedMap.<String>getTyped("city")).isEqualTo("London");
    }
}
