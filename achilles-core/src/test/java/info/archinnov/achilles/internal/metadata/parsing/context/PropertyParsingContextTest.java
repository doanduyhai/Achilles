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
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.lang.reflect.Field;

@RunWith(MockitoJUnitRunner.class)
public class PropertyParsingContextTest {

    @Test
    public void should_set_primaryKey_to_true_when_embedded_id() throws Exception {
        //Given
        EntityParsingContext entityContext = mock(EntityParsingContext.class, RETURNS_DEEP_STUBS);
        when(entityContext.getCurrentEntityClass()).thenReturn((Class) CompleteBean.class);
        Field field = CompleteBean.class.getDeclaredField("age");

        PropertyParsingContext context = new PropertyParsingContext(entityContext, field);

        //When
        context.setEmbeddedId(true);

        //Then
        assertThat(context.isEmbeddedId()).isTrue();
        assertThat(context.isPrimaryKey()).isTrue();
        assertThat(context.<CompleteBean>getCurrentEntityClass()).isEqualTo(CompleteBean.class);
        assertThat(context.getCurrentCQLColumnName()).isEqualTo("age_in_years");
    }


}
