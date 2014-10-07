package info.archinnov.achilles.test.integration.tests.bugs;

import info.archinnov.achilles.internal.metadata.holder.PropertyType;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.test.integration.AchillesInternalCQLResource;
import info.archinnov.achilles.test.integration.entity.ClusteredEntityForTranscoding;
import info.archinnov.achilles.test.integration.entity.ClusteredEntityWithPrimitiveAndSubTypes;
import org.apache.commons.lang3.RandomUtils;
import org.fest.assertions.core.Condition;
import org.junit.Rule;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;

import static org.fest.assertions.api.Assertions.assertThat;

/**
 * Test case for bug #108: Slice query: partition components and clustering keys should be encoded properly
 */
public class SliceQueryEncodingOnPrimitiveAndEnumTypesIT {

    @Rule
    public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(
            Steps.BOTH, ClusteredEntityForTranscoding.TABLE_NAME);

    private PersistenceManager manager = resource.getPersistenceManager();

    private final Condition<PropertyType> typeValueCheck = new Condition<PropertyType>() {
        @Override
        public boolean matches(PropertyType propertyType) {
            return propertyType == PropertyType.LIST || propertyType == PropertyType.SET;
        }
    };

    private final ByteBuffer bytes1 = ByteBuffer.wrap("bytes1".getBytes());
    private final ByteBuffer bytes2 = ByteBuffer.wrap("bytes2".getBytes());

    private final Condition<ByteBuffer> bytesValueCheck = new Condition<ByteBuffer>() {
        @Override
        public boolean matches(ByteBuffer byteBuffer) {
            return byteBuffer.equals(bytes1.duplicate()) || byteBuffer.equals(bytes2.duplicate());
        }
    };

    @Test
    public void should_slice_using_partition_keys_and_clustering_keys_IN() {
        //Given
        final long partition = RandomUtils.nextLong(0,Long.MAX_VALUE);
        final PropertyType type1 = PropertyType.LIST, type2 = PropertyType.SET;
        final int year = 2014;


        manager.insert(new ClusteredEntityForTranscoding(partition, type1, year, bytes1));
        manager.insert(new ClusteredEntityForTranscoding(partition, type1, year, bytes2));
        manager.insert(new ClusteredEntityForTranscoding(partition, type2, year, bytes1));
        manager.insert(new ClusteredEntityForTranscoding(partition, type2, year, bytes2));

        //When
        final List<ClusteredEntityForTranscoding> result = manager.sliceQuery(ClusteredEntityForTranscoding.class)
                .forSelect()
                .withPartitionComponents(partition)
                .andPartitionComponentsIN(type1, type2)
                .withClusterings(year)
                .andClusteringsIN(bytes1, bytes2)
                .get(10);

        //Then
        assertThat(result).hasSize(4);
        final ClusteredEntityForTranscoding first = result.get(0);
        assertThat(first.getId().getId()).isEqualTo(partition);
        assertThat(first.getId().getType()).is(typeValueCheck);
        assertThat(first.getId().getYear()).isEqualTo(year);
        assertThat(first.getId().getBytes()).is(bytesValueCheck);

        final ClusteredEntityForTranscoding second = result.get(1);
        assertThat(second.getId().getId()).isEqualTo(partition);
        assertThat(second.getId().getType()).is(typeValueCheck);
        assertThat(second.getId().getYear()).isEqualTo(year);
        assertThat(second.getId().getBytes()).is(bytesValueCheck);

        final ClusteredEntityForTranscoding third = result.get(2);
        assertThat(third.getId().getId()).isEqualTo(partition);
        assertThat(third.getId().getType()).is(typeValueCheck);
        assertThat(third.getId().getYear()).isEqualTo(year);
        assertThat(third.getId().getBytes()).is(bytesValueCheck);

        final ClusteredEntityForTranscoding fourth = result.get(3);
        assertThat(fourth.getId().getId()).isEqualTo(partition);
        assertThat(fourth.getId().getType()).is(typeValueCheck);
        assertThat(fourth.getId().getYear()).isEqualTo(year);
        assertThat(fourth.getId().getBytes()).is(bytesValueCheck);
    }
}
