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

import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import info.archinnov.achilles.internal.context.ConfigurationContext;
import info.archinnov.achilles.internal.context.DaoContext;
import info.archinnov.achilles.internal.context.PersistenceContextFactory;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;

/**
 * <p>
 * Create a Batch session to perform
 * <ul>
 *     <li>
 *         <em>insert()</em>
 *     </li>
 *     <li>
 *         <em>update()</em>
 *     </li>
 *     <li>
 *         <em>delete()</em>
 *     </li>
 *     <li>
 *         <em>deleteById()</em>
 *     </li>
 * </ul>
 *
 * This Batch is a <strong>state-full</strong> object that stacks up all operations. They will be flushed upon call to
 * <em>endBatch()</em>
 *
 * <br/>
 * <br/>
 *
 * There are 2 types of batch: <strong>ordered</strong> and <strong>unordered</strong>. Ordered batches will automatically add
 * increasing generated timestamp for each statement so that their ordering is guaranteed.
 *
 * <pre class="code"><code class="java">
 *
 *   Batch batch = manager.createBatch();
 *
 *   batch.insert(new User(10L, "John","LENNNON")); // nothing happens here
 *
 *   batch.endBatch(); // send the INSERT statement to Cassandra
 *
 * </code></pre>
 *
 * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Batch-Mode" target="_blank">Batch Mode</a>
 *
 */
public class Batch extends CommonBatch {

    private static final Logger log = LoggerFactory.getLogger(Batch.class);

    Batch(Map<Class<?>, EntityMeta> entityMetaMap, PersistenceContextFactory contextFactory,
            DaoContext daoContext, ConfigurationContext configContext, boolean orderedBatch) {
        super(entityMetaMap, contextFactory, daoContext, configContext, orderedBatch);
    }

    /**
     * End an existing batch and flush all the pending statements.
     *
     * Do nothing if there is no pending statement
     *
     */
    public void endBatch() {
        log.debug("Flushing batch");
        try {
            flushContext.flushBatch().getImmediately();
        } finally {
            flushContext = flushContext.duplicateWithNoData(defaultConsistencyLevel);
        }
    }
}
