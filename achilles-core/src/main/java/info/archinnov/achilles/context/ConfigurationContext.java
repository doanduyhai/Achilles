package info.archinnov.achilles.context;

import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.json.ObjectMapperFactory;

/**
 * ConfigurationContext
 * 
 * @author DuyHai DOAN
 * 
 */
public class ConfigurationContext {
    private boolean forceColumnFamilyCreation;

    private boolean ensureJoinConsistency;

    private AchillesConsistencyLevelPolicy consistencyPolicy;

    private ObjectMapperFactory objectMapperFactory;

    public boolean isForceColumnFamilyCreation() {
        return forceColumnFamilyCreation;
    }

    public void setForceColumnFamilyCreation(boolean forceColumnFamilyCreation) {
        this.forceColumnFamilyCreation = forceColumnFamilyCreation;
    }

    public boolean isEnsureJoinConsistency() {
        return ensureJoinConsistency;
    }

    public void setEnsureJoinConsistency(boolean ensureJoinConsistency) {
        this.ensureJoinConsistency = ensureJoinConsistency;
    }

    public AchillesConsistencyLevelPolicy getConsistencyPolicy() {
        return consistencyPolicy;
    }

    public void setConsistencyPolicy(AchillesConsistencyLevelPolicy consistencyPolicy) {
        this.consistencyPolicy = consistencyPolicy;
    }

    public ObjectMapperFactory getObjectMapperFactory() {
        return objectMapperFactory;
    }

    public void setObjectMapperFactory(ObjectMapperFactory objectMapperFactory) {
        this.objectMapperFactory = objectMapperFactory;
    }
}
