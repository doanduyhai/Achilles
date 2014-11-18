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

package info.archinnov.achilles.internal.consistency;

import static info.archinnov.achilles.internal.context.AbstractFlushContext.FlushType.BATCH;

import info.archinnov.achilles.annotations.Consistency;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import info.archinnov.achilles.internal.context.AbstractFlushContext;
import info.archinnov.achilles.internal.context.facade.PersistentStateHolder;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Options;

public class ConsistencyOverrider {

    private static final Logger log = LoggerFactory.getLogger(ConsistencyOverrider.class);

    public Options overrideRuntimeValueByBatchSetting(Options options, AbstractFlushContext flushContext) {
        Options result = options;
        if (flushContext.type() == BATCH && flushContext.getConsistencyLevel() != null) {
            result = options.duplicateWithNewConsistencyLevel(flushContext.getConsistencyLevel());
        }
        return result;
    }

    public ConsistencyLevel getReadLevel(PersistentStateHolder context) {
        EntityMeta entityMeta = context.getEntityMeta();
        ConsistencyLevel readLevel = context.getConsistencyLevel().isPresent() ? context.getConsistencyLevel().get() : entityMeta.config().getReadConsistencyLevel();
        log.trace("Read consistency level : " + readLevel);
        return readLevel;
    }

    public ConsistencyLevel getWriteLevel(PersistentStateHolder context) {
        EntityMeta entityMeta = context.getEntityMeta();
        ConsistencyLevel writeLevel = context.getConsistencyLevel().isPresent() ? context.getConsistencyLevel().get() : entityMeta.config().getWriteConsistencyLevel();
        log.trace("Write consistency level : " + writeLevel);
        return writeLevel;
    }

    public ConsistencyLevel getReadLevel(PersistentStateHolder context, PropertyMeta pm) {
        ConsistencyLevel consistency = context.getConsistencyLevel().isPresent() ? context.getConsistencyLevel().get()
                : pm.config().getReadConsistencyLevel();
        log.trace("Read consistency level : " + consistency);
        return consistency;
    }

    public ConsistencyLevel getWriteLevel(PersistentStateHolder context, PropertyMeta pm) {
        ConsistencyLevel consistency = context.getConsistencyLevel().isPresent() ? context.getConsistencyLevel().get()
                : pm.config().getWriteConsistencyLevel();
        log.trace("Write consistency level : " + consistency);
        return consistency;
    }

    public static enum Singleton {
        INSTANCE;

        private final ConsistencyOverrider instance = new ConsistencyOverrider();

        public ConsistencyOverrider get() {
            return instance;
        }
    }
}
