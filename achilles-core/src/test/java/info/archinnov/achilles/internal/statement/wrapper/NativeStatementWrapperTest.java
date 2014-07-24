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
import static org.fest.assertions.api.Assertions.assertThat;
import java.nio.ByteBuffer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.querybuilder.Insert;
import com.google.common.base.Optional;
import info.archinnov.achilles.listener.CASResultListener;

@RunWith(MockitoJUnitRunner.class)
public class NativeStatementWrapperTest {

    @Test
    public void should_build_parameterized_statement() throws Exception {
        //Given
        final Insert statement = insertInto("test").value("id", bindMarker("id"));
        statement.setConsistencyLevel(ConsistencyLevel.ALL);
        statement.setSerialConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL);
        final NativeStatementWrapper wrapper = new NativeStatementWrapper(NativeQueryLog.class, statement, new Object[] { 10L }, Optional.<CASResultListener>absent());
        final ByteBuffer[] boundValues = new SimpleStatement("select", 10L).getValues();

        //When
        final SimpleStatement actual = wrapper.buildParameterizedStatement();

        //Then
        assertThat(actual.getQueryString()).isEqualTo(statement.getQueryString());
        assertThat(actual.getConsistencyLevel()).isEqualTo(ConsistencyLevel.ALL);
        assertThat(actual.getSerialConsistencyLevel()).isEqualTo(ConsistencyLevel.LOCAL_SERIAL);

        assertThat(actual.getValues()).hasSize(1);
        assertThat(actual.getValues()).isEqualTo(boundValues);
    }


}
