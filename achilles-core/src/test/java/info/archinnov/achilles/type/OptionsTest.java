package info.archinnov.achilles.type;

import static info.archinnov.achilles.type.ConsistencyLevel.*;
import static org.fest.assertions.api.Assertions.assertThat;
import org.junit.Test;

public class OptionsTest {

    @Test
    public void should_duplicate_without_ttl_and_timestamp() throws Exception
    {
        Options options = OptionsBuilder
                .withConsistency(EACH_QUORUM)
                .ttl(10)
                .timestamp(100L);

        Options newOptions = options.duplicateWithoutTtlAndTimestamp();

        assertThat(newOptions.getConsistencyLevel().get()).isSameAs(EACH_QUORUM);
        assertThat(newOptions.getTimestamp().isPresent()).isFalse();
        assertThat(newOptions.getTtl().isPresent()).isFalse();
    }

    @Test
    public void should_duplicate_with_new_consistency_level() throws Exception
    {
        Options options = OptionsBuilder
                .withConsistency(EACH_QUORUM)
                .ttl(10)
                .timestamp(100L);

        Options newOptions = options.duplicateWithNewConsistencyLevel(LOCAL_QUORUM);

        assertThat(newOptions.getConsistencyLevel().get()).isSameAs(LOCAL_QUORUM);
        assertThat(newOptions.getTimestamp().get()).isEqualTo(100L);
        assertThat(newOptions.getTtl().get()).isEqualTo(10);
    }
}
