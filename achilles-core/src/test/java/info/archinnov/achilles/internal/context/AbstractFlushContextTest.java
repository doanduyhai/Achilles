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
package info.archinnov.achilles.internal.context;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import java.util.Collections;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import com.datastax.driver.core.ResultSet;
import com.google.common.util.concurrent.ListenableFuture;
import info.archinnov.achilles.interceptor.Event;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.statement.wrapper.AbstractStatementWrapper;
import info.archinnov.achilles.type.Empty;

@RunWith(MockitoJUnitRunner.class)
public class AbstractFlushContextTest {
    @Mock
    private DaoContext daoContext;

    @Test
    public void should_doesnt_do_anything_if_there_is_no_statement() {
        AbstractFlushContextForTest abstractFlushContext = new AbstractFlushContextForTest(daoContext);
        
        abstractFlushContext.executeBatch(null, Collections.<AbstractStatementWrapper>emptyList());
        
        verify(daoContext, never()).execute(any(AbstractStatementWrapper.class));
    }

    private static final class AbstractFlushContextForTest extends AbstractFlushContext {
        public AbstractFlushContextForTest(DaoContext daoContext) {
            super(daoContext, null, null);
        }
    
        @Override
        public void startBatch() {
        }
    
        @Override
        public ListenableFuture<List<ResultSet>> flush() {
            return null;
        }
    
        @Override
        public ListenableFuture<Empty> flushBatch() {
            return null;
        }
    
        @Override
        public FlushType type() {
            return null;
        }
    
        @Override
        public AbstractFlushContext duplicate() {
            return null;
        }
    
        @Override
        public void triggerInterceptor(EntityMeta meta, Object entity, Event event) {
        }
    }
}