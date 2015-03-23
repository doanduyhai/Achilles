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

import static com.datastax.driver.core.ConsistencyLevel.LOCAL_SERIAL;
import static com.datastax.driver.core.ConsistencyLevel.ONE;
import static com.google.common.base.Optional.of;
import static info.archinnov.achilles.LogInterceptionRule.interceptDMLStatementViaMockedAppender;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.Appender;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import com.datastax.driver.core.BoundStatement;
import com.google.common.base.Optional;
import info.archinnov.achilles.LogInterceptionRule.DMLStatementInterceptor;
import info.archinnov.achilles.listener.LWTResultListener;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;

import java.util.List;

@RunWith(MockitoJUnitRunner.class)
public class BoundStatementWrapperTest {

    private BoundStatementWrapper wrapper;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private BoundStatement bs;

    @Captor
    private ArgumentCaptor<LoggingEvent> loggingEvent;

    @Rule
    public DMLStatementInterceptor dmlStmntInterceptor = interceptDMLStatementViaMockedAppender();


    private static final Optional<LWTResultListener> NO_LISTENER = Optional.absent();

    @Test
    public void should_get_bound_statement() throws Exception {
        //Given
        wrapper = new BoundStatementWrapper(CompleteBean.class, bs, new Object[] { 1 }, ONE, NO_LISTENER, of(LOCAL_SERIAL));

        //When
        final BoundStatement expectedBs = wrapper.getStatement();

        //Then
        assertThat(expectedBs).isSameAs(bs);
        verify(bs).setSerialConsistencyLevel(LOCAL_SERIAL);
    }

    @Test
    public void should_log_dml_statement_with_bound_values() throws Exception {
        //Given
        wrapper = new BoundStatementWrapper(CompleteBean.class, bs, new Object[] { 73L, "bob" }, ONE, NO_LISTENER, of(LOCAL_SERIAL));
        given(bs.preparedStatement().getQueryString()).willReturn("insert ...");

        // When
        wrapper.logDMLStatement("");

        // Then
        verify(dmlStmntInterceptor.appender(), times(2)).doAppend(loggingEvent.capture());
        final List<LoggingEvent> allValues = loggingEvent.getAllValues();
        final Object[] argumentArray1 = allValues.get(0).getArgumentArray();
        assertThat(argumentArray1[1]).isEqualTo("insert ...");
        assertThat(argumentArray1[2]).isEqualTo("DEFAULT");

        final Object[] argumentArray2 = allValues.get(1).getArgumentArray();
        final List<Object> boundValues = (List<Object>) argumentArray2[0];
        assertThat(boundValues.get(0)).isEqualTo(73L);
        assertThat(boundValues.get(1)).isEqualTo("bob");
    }
}
