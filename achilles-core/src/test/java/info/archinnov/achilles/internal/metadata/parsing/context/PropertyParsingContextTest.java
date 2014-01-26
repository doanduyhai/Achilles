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

import static org.fest.assertions.api.Assertions.*;
import info.archinnov.achilles.internal.metadata.parsing.context.PropertyParsingContext;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PropertyParsingContextTest {

    @Test
    public void should_set_primaryKey_to_true_when_embedded_id() throws Exception {
        //Given
        PropertyParsingContext context = new PropertyParsingContext(null,null);

        //When
        context.setEmbeddedId(true);

        //Then
        assertThat(context.isEmbeddedId()).isTrue();
        assertThat(context.isPrimaryKey()).isTrue();
    }
}
