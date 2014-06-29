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
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.google.common.base.Optional;
import info.archinnov.achilles.listener.CASResultListener;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;

@RunWith(MockitoJUnitRunner.class)
public class BoundStatementWrapperTest {

    private BoundStatementWrapper wrapper;

    @Mock
    private BoundStatement bs;

    @Mock
    private PreparedStatement ps;

    @Mock
    private Session session;

    private static final Optional<CASResultListener> NO_LISTENER = Optional.absent();
    private  static final Optional<com.datastax.driver.core.ConsistencyLevel> NO_SERIAL_CONSISTENCY = Optional.absent();

    @Test
    public void should_execute() throws Exception {
        //Given
        wrapper = new BoundStatementWrapper(CompleteBean.class, bs, new Object[] { 1 }, ConsistencyLevel.ALL, NO_LISTENER, Optional.fromNullable(LOCAL_SERIAL));
        when(bs.preparedStatement()).thenReturn(ps);
        when(ps.getQueryString()).thenReturn("SELECT");

        //When
        wrapper.execute(session);

        //Then
        verify(session).execute(bs);
        verify(bs).setConsistencyLevel(ConsistencyLevel.ALL);
        verify(bs).setSerialConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL);
    }

    @Test
    public void should_get_bound_statement() throws Exception {
        //Given
        wrapper = new BoundStatementWrapper(CompleteBean.class, bs, new Object[] { 1 }, ConsistencyLevel.ONE, NO_LISTENER, NO_SERIAL_CONSISTENCY);

        //When
        final BoundStatement expectedBs = wrapper.getStatement();

        //Then
        assertThat(expectedBs).isSameAs(bs);
    }
}
