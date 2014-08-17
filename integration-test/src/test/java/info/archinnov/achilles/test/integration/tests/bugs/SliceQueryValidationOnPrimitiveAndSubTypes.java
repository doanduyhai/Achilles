package info.archinnov.achilles.test.integration.tests.bugs;

import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.test.integration.AchillesInternalCQLResource;
import info.archinnov.achilles.test.integration.entity.ClusteredEntityWithPrimitiveAndSubTypes;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.Rule;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;

import static org.fest.assertions.api.Assertions.assertThat;

/**
 *  Test case for bug #119: Validation of PartitionComponents fails for simple types
 */
public class SliceQueryValidationOnPrimitiveAndSubTypes {

    @Rule
    public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(
            Steps.BOTH, ClusteredEntityWithPrimitiveAndSubTypes.TABLE_NAME);

    private PersistenceManager manager = resource.getPersistenceManager();

    @Test
    public void should_slice_using_partition_keys_IN() {
        //Given
        final Long partition = RandomUtils.nextLong();
        final int bucket1 = 1, bucket2 = 2;
        final Date date1 = new Date(100000), date2 = new Date(200000);

        manager.insert(new ClusteredEntityWithPrimitiveAndSubTypes(partition, bucket1, date1.toString()));
        manager.insert(new ClusteredEntityWithPrimitiveAndSubTypes(partition, bucket1, date2.toString()));
        manager.insert(new ClusteredEntityWithPrimitiveAndSubTypes(partition, bucket2, date1.toString()));
        manager.insert(new ClusteredEntityWithPrimitiveAndSubTypes(partition, bucket2, date2.toString()));

        //When
        final List<ClusteredEntityWithPrimitiveAndSubTypes> result = manager.sliceQuery(ClusteredEntityWithPrimitiveAndSubTypes.class)
                .forSelect()
                .withPartitionComponents(partition)
                .andPartitionComponentsIN(bucket1, bucket2)
                .get(10);

        //Then
        assertThat(result).hasSize(4);
    }
    
    @Test
    public void should_slice_with_clustering_components() throws Exception {
        //Given
        final Long partition = RandomUtils.nextLong();
        final int bucket = 1;
        final Date date1 = new Date(100000);
        final Date date2 = new Date(200000);
        final Date date3 = new Date(300000);
        final Date date4 = new Date(400000);
        final Date date5 = new Date(500000);

        manager.insert(new ClusteredEntityWithPrimitiveAndSubTypes(partition, bucket, date1.toString()));
        manager.insert(new ClusteredEntityWithPrimitiveAndSubTypes(partition, bucket, date2.toString()));
        manager.insert(new ClusteredEntityWithPrimitiveAndSubTypes(partition, bucket, date3.toString()));
        manager.insert(new ClusteredEntityWithPrimitiveAndSubTypes(partition, bucket, date4.toString()));
        manager.insert(new ClusteredEntityWithPrimitiveAndSubTypes(partition, bucket, date5.toString()));

        //When
        final List<ClusteredEntityWithPrimitiveAndSubTypes> result = manager.sliceQuery(ClusteredEntityWithPrimitiveAndSubTypes.class)
                .forSelect()
                .withPartitionComponents(partition, bucket)
                .fromClusterings(ByteBuffer.wrap(date2.toString().getBytes()))
                .toClusterings(ByteBuffer.wrap(date5.toString().getBytes()))
                .fromInclusiveToExclusiveBounds()
                .get(100);


        //Then
        assertThat(result).hasSize(3);
    }

}
