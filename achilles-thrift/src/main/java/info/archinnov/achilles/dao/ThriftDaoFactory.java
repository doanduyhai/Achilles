package info.archinnov.achilles.dao;

import static info.archinnov.achilles.helper.PropertyHelper.isSupportedType;
import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.context.ConfigurationContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.type.Counter;
import info.archinnov.achilles.type.Pair;
import java.util.Map;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.Composite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftDaoFactory {

    private static final Logger log = LoggerFactory.getLogger(ThriftDaoFactory.class);

    public void createDaosForEntity(Cluster cluster, Keyspace keyspace, ConfigurationContext configContext,
            EntityMeta entityMeta, Map<String, ThriftGenericEntityDao> entityDaosMap,
            Map<String, ThriftGenericWideRowDao> wideRowDaosMap) {

        createEntityDao(cluster, keyspace, configContext, entityMeta, entityDaosMap);

    }

    private void createEntityDao(Cluster cluster, Keyspace keyspace, ConfigurationContext configContext,
            EntityMeta entityMeta, Map<String, ThriftGenericEntityDao> entityDaosMap) {
        String tableName = entityMeta.getTableName();
        ThriftGenericEntityDao entityDao = new ThriftGenericEntityDao(//
                cluster, //
                keyspace, //
                tableName, //
                configContext.getConsistencyPolicy(), //
                new Pair<Class<?>, Class<String>>(entityMeta.getIdClass(), String.class));
        entityDaosMap.put(tableName, entityDao);
        log.debug("Build entity dao for column family {}", tableName);
    }

    public void createClusteredEntityDao(Cluster cluster, Keyspace keyspace, ConfigurationContext configContext,
            EntityMeta entityMeta, Map<String, ThriftGenericWideRowDao> wideRowDaosMap) {

        Class<?> keyClass = entityMeta.getIdMeta().getComponentClasses().get(0);
        PropertyMeta<?, ?> pm = entityMeta.getFirstMeta();

        Class<?> valueClass;
        if (pm.isJoin()) {
            valueClass = pm.joinIdMeta().getValueClass();
        } else {
            valueClass = pm.getValueClass();
        }

        ThriftGenericWideRowDao dao;

        String tableName = entityMeta.getTableName();
        AchillesConsistencyLevelPolicy consistencyPolicy = configContext.getConsistencyPolicy();
        if (isSupportedType(valueClass)) {
            dao = new ThriftGenericWideRowDao(cluster, keyspace, //
                    tableName, consistencyPolicy, //
                    new Pair<Class<?>, Class<?>>(keyClass, valueClass));
        } else if (Counter.class.isAssignableFrom(valueClass)) {
            dao = new ThriftGenericWideRowDao(cluster, keyspace, //
                    tableName, consistencyPolicy,//
                    new Pair<Class<?>, Class<Long>>(keyClass, Long.class));
        } else {
            dao = new ThriftGenericWideRowDao(cluster, keyspace, //
                    tableName, consistencyPolicy, //
                    new Pair<Class<?>, Class<String>>(keyClass, String.class));
        }
        wideRowDaosMap.put(tableName, dao);
        log.debug("Build clustered entity dao for column family {}", tableName);
    }

    public ThriftCounterDao createCounterDao(Cluster cluster, Keyspace keyspace, ConfigurationContext configContext) {
        ThriftCounterDao counterDao = new ThriftCounterDao(cluster, keyspace, configContext.getConsistencyPolicy(), //
                new Pair<Class<Composite>, Class<Long>>(Composite.class, Long.class));
        log.debug("Build achillesCounterCF dao");

        return counterDao;
    }
}
