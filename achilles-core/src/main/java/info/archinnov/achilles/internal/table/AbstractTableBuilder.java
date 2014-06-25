package info.archinnov.achilles.internal.table;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import info.archinnov.achilles.internal.metadata.holder.IndexProperties;

public abstract class AbstractTableBuilder {

    protected Set<IndexProperties> indexedColumns = new HashSet<>();

    public boolean hasIndices() {
        return indexedColumns.size() > 0;
    }

    public Collection<String> generateIndices(Collection<IndexProperties> indexedColumns, String tableName) {
        Collection<String> indicesScripts = new LinkedList<>();
        if (hasIndices()) {
            for (IndexProperties indexProperties : indexedColumns) {
                String indexName = indexProperties.getName();
                indexName = indexName != null && indexName.trim().length() > 0 ? indexName : tableName + "_"
                        + indexProperties.getPropertyName();
                StringBuilder ddl = new StringBuilder();
                ddl.append("\n");
                ddl.append("CREATE INDEX ").append(indexName).append(" ");
                ddl.append("ON ").append(tableName).append("(").append(indexProperties.getPropertyName()).append(");\n");
                indicesScripts.add(ddl.toString());
            }
        }
        return indicesScripts;
    }
}
