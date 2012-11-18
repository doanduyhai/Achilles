package fr.doan.achilles.columnFamily;

import static fr.doan.achilles.validation.Validator.validateNotBlank;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import me.prettyprint.cassandra.model.BasicColumnDefinition;
import me.prettyprint.hector.api.ddl.ColumnDefinition;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.factory.HFactory;
import com.google.common.base.Charsets;
import fr.doan.achilles.metadata.EntityMeta;
import fr.doan.achilles.metadata.PropertyMeta;

public class ColumnFamilyBuilder {

    public <ID extends Serializable> ColumnFamilyDefinition build(EntityMeta<ID> entityMeta, String keyspaceName) {
        validateNotBlank(keyspaceName, "keyspaceName");
        List<ColumnDefinition> columnMetadatas = new ArrayList<ColumnDefinition>();

        for (Entry<String, PropertyMeta<?>> entry : entityMeta.getPropertyMetas().entrySet()) {
            BasicColumnDefinition column = new BasicColumnDefinition();
            column.setName(ByteBuffer.wrap(entry.getKey().getBytes(Charsets.UTF_8)));
            column.setValidationClass(entry.getValue().getValueSerializer().getClass().getCanonicalName());
            columnMetadatas.add(column);
        }

        ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(keyspaceName,
                entityMeta.getColumnFamilyName(), ComparatorType.COMPOSITETYPE, columnMetadatas);
        cfDef.setKeyValidationClass(entityMeta.getIdSerializer().getComparatorType().getClassName());
        cfDef.setComment("Column family for entity '" + entityMeta.getCanonicalClassName() + "'");

        return cfDef;
    }
}
