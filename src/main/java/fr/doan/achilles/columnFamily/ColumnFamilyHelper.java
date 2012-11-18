package fr.doan.achilles.columnFamily;

import java.util.Map;
import java.util.Map.Entry;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import org.apache.commons.lang.StringUtils;
import fr.doan.achilles.exception.InvalidColumnFamilyException;
import fr.doan.achilles.metadata.EntityMeta;

public class ColumnFamilyHelper {
    private Cluster cluster;
    private Keyspace keyspace;
    private ColumnFamilyBuilder columnFamilyBuilder = new ColumnFamilyBuilder();
    private ColumnFamilyValidator columnFamilyValidator = new ColumnFamilyValidator();

    public ColumnFamilyHelper(Cluster cluster, Keyspace keyspace) {
        this.cluster = cluster;
        this.keyspace = keyspace;
    }

    public ColumnFamilyDefinition discoverColumnFamily(String columnFamilyName) {
        KeyspaceDefinition keyspaceDef = this.cluster.describeKeyspace(this.keyspace.getKeyspaceName());
        if (keyspaceDef != null && keyspaceDef.getCfDefs() != null) {

            for (ColumnFamilyDefinition cfDef : keyspaceDef.getCfDefs()) {
                if (StringUtils.equals(cfDef.getName(), columnFamilyName)) {
                    return cfDef;
                }
            }
        }
        return null;
    }

    public String addColumnFamily(ColumnFamilyDefinition cfDef) {
        return this.cluster.addColumnFamily(cfDef, true);
    }

    public void createColumnFamily(EntityMeta<?> entityMeta) {
        ColumnFamilyDefinition cfDef = this.columnFamilyBuilder.build(entityMeta, this.keyspace.getKeyspaceName());
        this.addColumnFamily(cfDef);
    }

    public void validateColumnFamilies(Map<Class<?>, EntityMeta<?>> entityMetaMap, boolean forceColumnFamilyCreation) {
        for (Entry<Class<?>, EntityMeta<?>> entry : entityMetaMap.entrySet()) {
            ColumnFamilyDefinition cfDef = this.discoverColumnFamily(entry.getValue().getColumnFamilyName());
            if (cfDef == null) {
                if (forceColumnFamilyCreation) {
                    this.createColumnFamily(entry.getValue());
                } else {
                    throw new InvalidColumnFamilyException("The required column family '"
                            + entry.getValue().getColumnFamilyName() + "' does not exist for entity '"
                            + entry.getValue().getCanonicalClassName() + "'");
                }
            } else {
                this.columnFamilyValidator.validate(cfDef, entry.getValue());
            }
        }
    }

}
