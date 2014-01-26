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

package info.archinnov.achilles.internal.statement.wrapper;

import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;

@RunWith(MockitoJUnitRunner.class)
public class SimpleStatementWrapperTest {

    private final Object[] values = new Object[]{1};

    private SimpleStatementWrapper wrapper;

    @Mock
    private Session session;

    @Test
    public void should_execute() throws Exception {
        //Given
        wrapper = new SimpleStatementWrapper("SELECT", values);

        //When
        wrapper.execute(session);

        //Then
        verify(session).execute("SELECT",values);
    }

    @Test
    public void should_get_bound_statement() throws Exception {
        //Given
        wrapper = new SimpleStatementWrapper("SELECT",values);

        //When
        final SimpleStatement simpleStatement = wrapper.getStatement();

        //Then
        assertThat(simpleStatement.getQueryString()).isEqualTo("SELECT");
    }
}
