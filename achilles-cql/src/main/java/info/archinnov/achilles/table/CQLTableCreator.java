package info.archinnov.achilles.table;

import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;

public class CQLTableCreator extends TableCreator {

    @Override
    protected void validateOrCreateCFForWideMap(PropertyMeta<?, ?> propertyMeta, Class<?> keyClass,
            boolean forceColumnFamilyCreation, String externalColumnFamilyName, String entityName) {
        // TODO Auto-generated method stub

    }

    @Override
    protected void validateOrCreateCFForEntity(EntityMeta entityMeta, boolean forceColumnFamilyCreation) {
        // TODO Auto-generated method stub

    }

    @Override
    protected void validateOrCreateCFForCounter(boolean forceColumnFamilyCreation) {
        // TODO Auto-generated method stub

    }

}
