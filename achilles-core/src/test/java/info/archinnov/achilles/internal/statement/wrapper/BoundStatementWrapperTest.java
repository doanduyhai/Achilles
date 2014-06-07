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

import static com.google.common.base.Optional.fromNullable;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.google.common.base.Optional;
import info.archinnov.achilles.listener.CASResultListener;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;

@RunWith(MockitoJUnitRunner.class)
public class BoundStatementWrapperTest {

    private BoundStatementWrapper wrapper;

    @Mock
    private BoundStatement bs;


    private static final Optional<CASResultListener> NO_LISTENER = Optional.absent();

    @Test
    public void should_get_bound_statement() throws Exception {
        //Given
        wrapper = new BoundStatementWrapper(CompleteBean.class, bs, new Object[] { 1 }, ConsistencyLevel.ONE, NO_LISTENER, fromNullable(ConsistencyLevel.LOCAL_SERIAL));

        //When
        final BoundStatement expectedBs = wrapper.getStatement();

        //Then
        assertThat(expectedBs).isSameAs(bs);
        verify(bs).setSerialConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL);
    }
}
