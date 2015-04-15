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

import static info.archinnov.achilles.LogInterceptionRule.interceptDMLStatementViaMockedAppender;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import info.archinnov.achilles.LogInterceptionRule.DMLStatementInterceptor;
import info.archinnov.achilles.listener.LWTResultListener;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;

@RunWith(MockitoJUnitRunner.class)
public class BoundStatementWrapperTest {

    private BoundStatementWrapper wrapper;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private BoundStatement bs;

    @Mock
    private Supplier<BoundStatement> supplier;

    @Rule
    public DMLStatementInterceptor dmlStmntInterceptor = interceptDMLStatementViaMockedAppender();

    @Captor
    private ArgumentCaptor<LoggingEvent> loggingEvent;

    private static final Optional<LWTResultListener> NO_LISTENER = Optional.absent();

    @Test
    public void should_not_create_bound_statement_if_not_required() throws Exception {
        //Given
        wrapper = new BoundStatementWrapper(CompleteBean.class, supplier, new Object[] { 1 }, ConsistencyLevel.ONE, NO_LISTENER, Optional.of(ConsistencyLevel.LOCAL_SERIAL));

        //Then
        verify(supplier, never()).get();
    }

    @Test
    public void should_get_bound_statement() throws Exception {
        //Given
        wrapper = new BoundStatementWrapper(CompleteBean.class, Suppliers.ofInstance(bs), new Object[] { 1 }, ConsistencyLevel.ONE, NO_LISTENER, Optional.of(ConsistencyLevel.LOCAL_SERIAL));

        //When
        final BoundStatement expectedBs = wrapper.getStatement();

        //Then
        assertThat(expectedBs).isSameAs(bs);
        verify(bs).setSerialConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL);
    }

    @Test
    public void should_log_dml_statement_with_bound_values() throws Exception {
        //Given
        wrapper = new BoundStatementWrapper(CompleteBean.class, Suppliers.ofInstance(bs), new Object[] { 73L, "bob" }, ConsistencyLevel.ONE, NO_LISTENER, Optional.of(ConsistencyLevel.LOCAL_SERIAL));
        given(bs.preparedStatement().getQueryString()).willReturn("insert ...");

        // When
        wrapper.logDMLStatement("");

        // Then
        verify(dmlStmntInterceptor.appender(), times(2)).doAppend(loggingEvent.capture());
        assertThat(loggingEvent.getAllValues().get(0).getMessage().toString())
                .contains("[insert ...] with CONSISTENCY LEVEL [DEFAULT]");
        assertThat(loggingEvent.getAllValues().get(1).getMessage().toString())
                .contains("bound values : [73, bob]");
    }
}
