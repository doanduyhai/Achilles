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
package info.archinnov.achilles.persistence;

import com.google.common.util.concurrent.FutureCallback;
import info.archinnov.achilles.async.AchillesFuture;
import info.archinnov.achilles.internal.async.AsyncUtils;
import info.archinnov.achilles.internal.context.ConfigurationContext;
import info.archinnov.achilles.internal.context.DaoContext;
import info.archinnov.achilles.internal.context.PersistenceContextFactory;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.type.Empty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * <p>
 * Create an asynchronous Batch session to perform
 * <ul>
 *     <li>
 *         <em>insert()</em>
 *     </li>
 *     <li>
 *         <em>update()</em>
 *     </li>
 *     <li>
 *         <em>remove()</em>
 *     </li>
 *     <li>
 *         <em>removeById()</em>
 *     </li>
 * </ul>
 *
 * This AsyncBatch is a <strong>state-full</strong> object that stacks up all operations. They will be flushed upon call to
 * <em>asyncEndBatch()</em>
 *
 * <br/>
 * <br/>
 *
 * There are 2 types of batch: <strong>ordered</strong> and <strong>unordered</strong>. Ordered batches will automatically add
 * increasing generated timestamp for each statement so that their ordering is guaranteed.
 *
 * <pre class="code"><code class="java">
 *
 *   AsyncBatch asyncBatch = asyncManager.createBatch();
 *
 *   asyncBatch.insert(new User(10L, "John","LENNNON")); // nothing happens here
 *
 *   Future<Empty> emptyFuture = asyncBatch.asyncEndBatch(); // send the INSERT statement to Cassandra
 *
 * </code></pre>
 *
 * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Batch-Mode" target="_blank">Batch Mode</a>
 *
 */
public class AsyncBatch extends CommonBatch {

    private static final Logger log = LoggerFactory.getLogger(AsyncBatch.class);

    private AsyncUtils asyncUtils = AsyncUtils.Singleton.INSTANCE.get();

    AsyncBatch(Map<Class<?>, EntityMeta> entityMetaMap, PersistenceContextFactory contextFactory,
               DaoContext daoContext, ConfigurationContext configContext, boolean orderedBatch) {
        super(entityMetaMap, contextFactory, daoContext, configContext, orderedBatch);
    }

    /**
     * End an existing batch and flush all the pending statements asynchronously.
     *
     * Do nothing if there is no pending statement
     *
     * @return Future<Empty> an empty future
     *
     */
    public AchillesFuture<Empty> asyncEndBatch() {
        log.debug("Flushing batch asynchronously");
        try {
            return flushContext.flushBatch();
        } finally {
            flushContext = flushContext.duplicateWithNoData(defaultConsistencyLevel);
        }
    }

    /**
     * End an existing batch and flush all the pending statements.
     *
     * Do nothing if there is no pending statement
     *
     */
    public AchillesFuture<Empty> asyncEndBatch(FutureCallback<Object>... asyncListeners) {
        log.debug("Flushing batch asynchronously");
        try {
            final ExecutorService executorService = configContext.getExecutorService();
            AchillesFuture<Empty> emptyFuture = flushContext.flushBatch();
            asyncUtils.maybeAddAsyncListeners(emptyFuture, asyncListeners, executorService);
            return asyncUtils.buildInterruptible(emptyFuture);
        } finally {
            flushContext = flushContext.duplicateWithNoData(defaultConsistencyLevel);
        }
    }
}
