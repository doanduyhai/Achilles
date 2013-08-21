package info.archinnov.achilles.type;

import static info.archinnov.achilles.type.ConsistencyLevel.*;
import static org.fest.assertions.api.Assertions.assertThat;
import org.junit.Test;

public class OptionsBuilderTest {

    @Test
    public void should_create_options_with_all_parameters() throws Exception
    {
        Options options = OptionsBuilder.withConsistency(ALL)
                .ttl(10)
                .timestamp(100L);

        assertThat(options.getConsistencyLevel().get()).isSameAs(ALL);
        assertThat(options.getTtl().get()).isEqualTo(10);
        assertThat(options.getTimestamp().get()).isEqualTo(100L);

        options = OptionsBuilder.withTtl(11)
                .consistency(ANY)
                .timestamp(111L);

        assertThat(options.getConsistencyLevel().get()).isSameAs(ANY);
        assertThat(options.getTtl().get()).isEqualTo(11);
        assertThat(options.getTimestamp().get()).isEqualTo(111L);

        options = OptionsBuilder.withTimestamp(122L)
                .consistency(ONE)
                .ttl(12);

        assertThat(options.getConsistencyLevel().get()).isSameAs(ONE);
        assertThat(options.getTtl().get()).isEqualTo(12);
        assertThat(options.getTimestamp().get()).isEqualTo(122L);

    }

    @Test
    public void should_create_no_options() throws Exception
    {
        Options options = OptionsBuilder.noOptions();

        assertThat(options.getConsistencyLevel().isPresent()).isFalse();
        assertThat(options.getTtl().isPresent()).isFalse();
        assertThat(options.getTimestamp().isPresent()).isFalse();
    }
}
