package info.archinnov.achilles.consistency;

import static info.archinnov.achilles.context.AbstractFlushContext.FlushType.BATCH;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import info.archinnov.achilles.context.AbstractFlushContext;
import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
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

    public ConsistencyLevel getReadLevel(PersistenceContext context, EntityMeta meta) {
        ConsistencyLevel readLevel = context.getConsistencyLevel().isPresent() ? context.getConsistencyLevel().get()
                : meta.getReadConsistencyLevel();
        log.trace("Read consistency level : " + readLevel);
        return readLevel;
    }

    public ConsistencyLevel getWriteLevel(PersistenceContext context, EntityMeta meta) {
        ConsistencyLevel writeLevel = context.getConsistencyLevel().isPresent() ? context.getConsistencyLevel().get()
                : meta.getWriteConsistencyLevel();
        log.trace("Write consistency level : " + writeLevel);
        return writeLevel;
    }

    public ConsistencyLevel getReadLevel(PersistenceContext context, PropertyMeta pm) {
        ConsistencyLevel consistency = context.getConsistencyLevel().isPresent() ? context.getConsistencyLevel().get()
                : pm.getReadConsistencyLevel();
        log.trace("Read consistency level : " + consistency);
        return consistency;
    }

    public ConsistencyLevel getWriteLevel(PersistenceContext context, PropertyMeta pm) {
        ConsistencyLevel consistency = context.getConsistencyLevel().isPresent() ? context.getConsistencyLevel().get()
                : pm.getWriteConsistencyLevel();
        log.trace("Write consistency level : " + consistency);
        return consistency;
    }
}
