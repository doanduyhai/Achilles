/*
 * Copyright (C) 2012-2014 DuyHai DOAN
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package info.archinnov.achilles.internal.consistency;

import static com.google.common.base.Optional.fromNullable;
import static info.archinnov.achilles.type.ConsistencyLevel.EACH_QUORUM;
import static info.archinnov.achilles.type.ConsistencyLevel.LOCAL_QUORUM;
import static info.archinnov.achilles.type.OptionsBuilder.noOptions;
import static info.archinnov.achilles.type.OptionsBuilder.withConsistency;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import com.google.common.base.Optional;
import info.archinnov.achilles.internal.context.AbstractFlushContext;
import info.archinnov.achilles.internal.context.BatchingFlushContext;
import info.archinnov.achilles.internal.context.ImmediateFlushContext;
import info.archinnov.achilles.internal.context.PersistenceContext;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Options;

@RunWith(MockitoJUnitRunner.class)
public class ConsistencyOverriderTest {
    private  static final Optional<com.datastax.driver.core.ConsistencyLevel> NO_SERIAL_CONSISTENCY = Optional.absent();

    private ConsistencyOverrider overrider = new ConsistencyOverrider();

    @Mock
    private PersistenceContext.StateHolderFacade context;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private EntityMeta meta;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private PropertyMeta pm;

    private static final Optional<ConsistencyLevel> NO_CONSISTENCY = Optional.absent();

    private Options noOptions = noOptions();

    private Options options = withConsistency(EACH_QUORUM);

    @Test
    public void should_override_runtime_value_by_batch_setting() throws Exception {
        //Given
        Options options = withConsistency(LOCAL_QUORUM);
        AbstractFlushContext flushContext = new BatchingFlushContext(null, EACH_QUORUM, NO_SERIAL_CONSISTENCY);

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
        AbstractFlushContext flushContext = new ImmediateFlushContext(null, EACH_QUORUM, NO_SERIAL_CONSISTENCY);

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
        final ConsistencyLevel actual = overrider.getReadLevel(context);

        //Then
        assertThat(actual).isEqualTo(EACH_QUORUM);
    }

    @Test
    public void should_get_read_level_from_entity_meta() throws Exception {
        //Given
        when(context.getEntityMeta()).thenReturn(meta);
        when(context.getConsistencyLevel()).thenReturn(NO_CONSISTENCY);
        when(meta.config().getReadConsistencyLevel()).thenReturn(LOCAL_QUORUM);

        //When
        final ConsistencyLevel actual = overrider.getReadLevel(context);

        //Then
        assertThat(actual).isEqualTo(LOCAL_QUORUM);
    }

    @Test
    public void should_get_write_level_from_context_rather_than_entity_meta() throws Exception {
        //Given
        when(context.getEntityMeta()).thenReturn(meta);
        when(context.getConsistencyLevel()).thenReturn(fromNullable(EACH_QUORUM));
        when(meta.config().getReadConsistencyLevel()).thenReturn(LOCAL_QUORUM);

        //When
        final ConsistencyLevel actual = overrider.getWriteLevel(context);

        //Then
        assertThat(actual).isEqualTo(EACH_QUORUM);
    }

    @Test
    public void should_get_write_level_from_entity_meta() throws Exception {
        //Given
        when(context.getConsistencyLevel()).thenReturn(NO_CONSISTENCY);
        when(context.getEntityMeta()).thenReturn(meta);
        when(meta.config().getWriteConsistencyLevel()).thenReturn(LOCAL_QUORUM);

        //When
        final ConsistencyLevel actual = overrider.getWriteLevel(context);

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
        when(pm.config().getReadConsistencyLevel()).thenReturn(LOCAL_QUORUM);

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
        when(pm.config().getWriteConsistencyLevel()).thenReturn(LOCAL_QUORUM);

        //When
        final ConsistencyLevel actual = overrider.getWriteLevel(context, pm);

        //Then
        assertThat(actual).isEqualTo(LOCAL_QUORUM);
    }
}
