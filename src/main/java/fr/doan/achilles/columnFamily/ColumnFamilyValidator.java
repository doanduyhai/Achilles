package fr.doan.achilles.columnFamily;

import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import org.apache.commons.lang.StringUtils;
import fr.doan.achilles.exception.InvalidColumnFamilyException;
import fr.doan.achilles.metadata.EntityMeta;

public class ColumnFamilyValidator {

    public void validate(ColumnFamilyDefinition cfDef, EntityMeta<?> entityMeta) {

        if (!StringUtils.equals(cfDef.getKeyValidationClass(), entityMeta.getIdSerializer().getComparatorType()
                .getTypeName())) {
            throw new InvalidColumnFamilyException("The column family '" + entityMeta.getColumnFamilyName()
                    + "' key class '" + cfDef.getKeyValidationClass()
                    + "' does not correspond to the entity id class '"
                    + entityMeta.getIdSerializer().getComparatorType().getTypeName() + "'");
        }

        if (cfDef.getComparatorType() == null || cfDef.getComparatorType() != ComparatorType.COMPOSITETYPE) {
            throw new InvalidColumnFamilyException("The column family '" + entityMeta.getColumnFamilyName()
                    + "' comparator type should be 'CompositeType'");
        }
    }
}
