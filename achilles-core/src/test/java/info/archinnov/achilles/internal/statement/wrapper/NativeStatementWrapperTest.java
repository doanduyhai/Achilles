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

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static info.archinnov.achilles.LogInterceptionRule.interceptDMLStatementViaMockedAppender;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.runners.MockitoJUnitRunner;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.querybuilder.Insert;
import com.google.common.base.Optional;
import info.archinnov.achilles.LogInterceptionRule.DMLStatementInterceptor;
import info.archinnov.achilles.listener.LWTResultListener;

@RunWith(MockitoJUnitRunner.class)
public class NativeStatementWrapperTest {

    @Rule
    public DMLStatementInterceptor dmlStmntInterceptor = interceptDMLStatementViaMockedAppender();

    @Captor
    private ArgumentCaptor<LoggingEvent> loggingEvent;

    @Test
    public void should_build_parameterized_statement() throws Exception {
        //Given
        final Insert statement = insertInto("test").value("id", bindMarker("id"));
        statement.setConsistencyLevel(ConsistencyLevel.ALL);
        statement.setSerialConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL);
        final NativeStatementWrapper wrapper = new NativeStatementWrapper(NativeQueryLog.class, statement, new Object[] { 10L }, Optional.<LWTResultListener>absent());
        final ByteBuffer[] boundValues = new SimpleStatement("select", 10L).getValues(ProtocolVersion.V2);

        //When
        final SimpleStatement actual = (SimpleStatement)wrapper.buildParameterizedStatement();

        //Then
        assertThat(actual.getQueryString()).isEqualTo(statement.getQueryString());
        assertThat(actual.getConsistencyLevel()).isEqualTo(ConsistencyLevel.ALL);
        assertThat(actual.getSerialConsistencyLevel()).isEqualTo(ConsistencyLevel.LOCAL_SERIAL);

        assertThat(actual.getValues(ProtocolVersion.V2)).hasSize(1);
        assertThat(actual.getValues(ProtocolVersion.V2)).isEqualTo(boundValues);
    }

    @Test
    public void should_return_regular_statement() throws Exception {
        //Given
        final Insert statement = insertInto("test").value("id", 10L).value("uuid", new UUID(10,10));
        statement.setConsistencyLevel(ConsistencyLevel.ALL);
        statement.setSerialConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL);
        final NativeStatementWrapper wrapper = new NativeStatementWrapper(NativeQueryLog.class, statement, new Object[] {}, Optional.<LWTResultListener>absent());

        //When
        final RegularStatement actual = (RegularStatement)wrapper.buildParameterizedStatement();

        //Then
        assertThat(actual).isSameAs(statement);
        assertThat(actual.getQueryString()).isEqualTo(statement.getQueryString());
        assertThat(actual.getValues(ProtocolVersion.V2)).isNotEmpty();
        assertThat(actual.getConsistencyLevel()).isEqualTo(ConsistencyLevel.ALL);
        assertThat(actual.getSerialConsistencyLevel()).isEqualTo(ConsistencyLevel.LOCAL_SERIAL);
    }

    @Test
    public void should_log_dml_of_a_native_statement() throws Exception {
        //Given
        final Insert statement = insertInto("test").value("id", 73L).value("value", "whatever, as replaced by a '?' when using RegularStatement.getQueryString");
        statement.setConsistencyLevel(ConsistencyLevel.ALL);
        statement.setSerialConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL);
        final NativeStatementWrapper wrapper = new NativeStatementWrapper(NativeQueryLog.class, statement, new Object[] {}, Optional.<LWTResultListener>absent());


        // When
        wrapper.logDMLStatement("");

        // Then
        verify(dmlStmntInterceptor.appender(), times(1)).doAppend(loggingEvent.capture());
        assertThat(loggingEvent.getValue().getMessage().toString())
                .contains("[INSERT INTO test(id,value) VALUES (73,?);] with CONSISTENCY LEVEL [ALL]");
    }

}
