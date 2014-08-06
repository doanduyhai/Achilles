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

package info.archinnov.achilles.internal.metadata.parsing.context;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import com.google.common.collect.ImmutableMap;
import info.archinnov.achilles.internal.context.ConfigurationContext;

@RunWith(MockitoJUnitRunner.class)
public class EntityParsingContextTest {

    @InjectMocks
    private EntityParsingContext parsingContext;

    @Mock
    private ConfigurationContext configContext;

    @Test
    public void should_determine_schema_update_status_for_table_from_map() throws Exception {
        //Given
        when(configContext.getEnableSchemaUpdateForTables()).thenReturn(ImmutableMap.of("table", false));

        //When
        final boolean actual = parsingContext.isSchemaUpdateEnabled("table");

        //Then
        assertThat(actual).isFalse();
    }

    @Test
    public void should_determine_schema_update_status_for_table_from_default() throws Exception {
        //Given
        when(configContext.getEnableSchemaUpdateForTables()).thenReturn(ImmutableMap.of("another_table", false));
        when(configContext.isEnableSchemaUpdate()).thenReturn(true);

        //When
        final boolean actual = parsingContext.isSchemaUpdateEnabled("table");

        //Then
        assertThat(actual).isTrue();
    }

}
