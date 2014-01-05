package info.archinnov.achilles.consistency;

import static info.archinnov.achilles.type.ConsistencyLevel.EACH_QUORUM;
import static info.archinnov.achilles.type.ConsistencyLevel.LOCAL_QUORUM;
import static info.archinnov.achilles.type.OptionsBuilder.noOptions;
import static info.archinnov.achilles.type.OptionsBuilder.withConsistency;
import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import info.archinnov.achilles.context.AbstractFlushContext;
import info.archinnov.achilles.context.BatchingFlushContext;
import info.archinnov.achilles.context.ImmediateFlushContext;
import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Options;
import info.archinnov.achilles.type.OptionsBuilder;

@RunWith(MockitoJUnitRunner.class)
public class ConsistencyOverriderTest {
    
    private ConsistencyOverrider overrider = new ConsistencyOverrider();

    @Mock
    private PersistenceContext context;

    @Mock
    private EntityMeta meta;

    @Mock
    private PropertyMeta pm;

    private Options noOptions = noOptions();

    private Options options = withConsistency(EACH_QUORUM);

    @Test
    public void should_override_runtime_value_by_batch_setting() throws Exception {
        //Given
        Options options = withConsistency(LOCAL_QUORUM);
        AbstractFlushContext flushContext = new BatchingFlushContext(null, EACH_QUORUM);

        //When
        final Options actual = overrider.overrideRuntimeValueByBatchSetting(options, flushContext);

        //Then
        assertThat(actual.getConsistencyLevel().isPresent()).isTrue();
        assertThat(actual.getConsistencyLevel().get()).isEqualTo(EACH_QUORUM);
    }

    @Test
    public void should_not_override_runtime_value_if_no_batch() throws Exception {
        //Given
        Options options = withConsistency(LOCAL_QUORUM);
        AbstractFlushContext flushContext = new ImmediateFlushContext(null, EACH_QUORUM);

        //When
        final Options actual = overrider.overrideRuntimeValueByBatchSetting(options, flushContext);

        //Then
        assertThat(actual.getConsistencyLevel().isPresent()).isTrue();
        assertThat(actual.getConsistencyLevel().get()).isEqualTo(LOCAL_QUORUM);
    }

    @Test
    public void should_get_read_level_from_context_rather_than_entity_meta() throws Exception {
        //Given
        when(context.getConsistencyLevel()).thenReturn(options.getConsistencyLevel());

        //When
        final ConsistencyLevel actual = overrider.getReadLevel(context, meta);

        //Then
        assertThat(actual).isEqualTo(EACH_QUORUM);
    }

    @Test
    public void should_get_read_level_from_entity_meta() throws Exception {
        //Given
        when(context.getConsistencyLevel()).thenReturn(noOptions.getConsistencyLevel());
        when(meta.getReadConsistencyLevel()).thenReturn(LOCAL_QUORUM);

        //When
        final ConsistencyLevel actual = overrider.getReadLevel(context, meta);

        //Then
        assertThat(actual).isEqualTo(LOCAL_QUORUM);
    }

    @Test
    public void should_get_write_level_from_context_rather_than_entity_meta() throws Exception {
        //Given
        when(context.getConsistencyLevel()).thenReturn(options.getConsistencyLevel());

        //When
        final ConsistencyLevel actual = overrider.getWriteLevel(context, meta);

        //Then
        assertThat(actual).isEqualTo(EACH_QUORUM);
    }

    @Test
    public void should_get_write_level_from_entity_meta() throws Exception {
        //Given
        when(context.getConsistencyLevel()).thenReturn(noOptions.getConsistencyLevel());
        when(meta.getWriteConsistencyLevel()).thenReturn(LOCAL_QUORUM);

        //When
        final ConsistencyLevel actual = overrider.getWriteLevel(context, meta);

        //Then
        assertThat(actual).isEqualTo(LOCAL_QUORUM);
    }




    @Test
    public void should_get_read_level_from_context_rather_than_property_meta() throws Exception {
        //Given
        when(context.getConsistencyLevel()).thenReturn(options.getConsistencyLevel());

        //When
        final ConsistencyLevel actual = overrider.getReadLevel(context, pm);

        //Then
        assertThat(actual).isEqualTo(EACH_QUORUM);
    }

    @Test
    public void should_get_read_level_from_property_meta() throws Exception {
        //Given
        when(context.getConsistencyLevel()).thenReturn(noOptions.getConsistencyLevel());
        when(pm.getReadConsistencyLevel()).thenReturn(LOCAL_QUORUM);

        //When
        final ConsistencyLevel actual = overrider.getReadLevel(context, pm);

        //Then
        assertThat(actual).isEqualTo(LOCAL_QUORUM);
    }

    @Test
    public void should_get_write_level_from_context_rather_than_property_meta() throws Exception {
        //Given
        when(context.getConsistencyLevel()).thenReturn(options.getConsistencyLevel());

        //When
        final ConsistencyLevel actual = overrider.getWriteLevel(context, pm);

        //Then
        assertThat(actual).isEqualTo(EACH_QUORUM);
    }

    @Test
    public void should_get_write_level_from_property_meta() throws Exception {
        //Given
        when(context.getConsistencyLevel()).thenReturn(noOptions.getConsistencyLevel());
        when(pm.getWriteConsistencyLevel()).thenReturn(LOCAL_QUORUM);

        //When
        final ConsistencyLevel actual = overrider.getWriteLevel(context, pm);

        //Then
        assertThat(actual).isEqualTo(LOCAL_QUORUM);
    }
}
