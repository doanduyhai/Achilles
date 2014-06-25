package info.archinnov.achilles.internal.table;

import static info.archinnov.achilles.internal.cql.TypeMapper.toCQLType;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import info.archinnov.achilles.internal.metadata.holder.IndexProperties;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyType;
import info.archinnov.achilles.internal.validation.Validator;
import info.archinnov.achilles.type.Pair;

public class TableUpdateBuilder extends AbstractTableBuilder {

    private String tableName;
    private Map<String, String> columns = new LinkedHashMap<>();
    private Map<String, String> lists = new LinkedHashMap<>();
    private Map<String, String> sets = new LinkedHashMap<>();
    private List<String> counters = new LinkedList<>();
    private Map<String, Pair<String, String>> maps = new LinkedHashMap<>();

    public TableUpdateBuilder(String tableName) {
        this.tableName = TableNameNormalizer.normalizerAndValidateColumnFamilyName(tableName);
    }

    public TableUpdateBuilder addColumn(String columnName, Class<?> javaType) {
        columns.put(columnName, toCQLType(javaType).toString());
        return this;
    }

    public TableUpdateBuilder addList(String listName, Class<?> javaValueType) {
        lists.put(listName, toCQLType(javaValueType).toString());
        return this;
    }

    public TableUpdateBuilder addSet(String setName, Class<?> javaValueType) {
        sets.put(setName, toCQLType(javaValueType).toString());
        return this;
    }

    public TableUpdateBuilder addMap(String mapName, Class<?> javaKeyType, Class<?> javaValueType) {
        maps.put(mapName, Pair.create(toCQLType(javaKeyType).toString(), toCQLType(javaValueType).toString()));
        return this;
    }

    public TableUpdateBuilder addCounter(String propertyName) {
        counters.add(propertyName);
        return this;
    }

    public TableUpdateBuilder addIndex(IndexProperties indexProperties, PropertyMeta indexPropertyMeta) {
        PropertyType type = indexPropertyMeta.type();
        String name = indexProperties.getName();
        Validator.validateFalse(type == PropertyType.LIST, "Index '%s' for table '%s' cannot be of list type", name, tableName);
        Validator.validateFalse(type == PropertyType.SET, "Index '%s' for table '%s' cannot be of set type", name, tableName);
        Validator.validateFalse(type == PropertyType.MAP, "Index '%s' for table '%s' cannot be of map type", name, tableName);
        Validator.validateFalse(indexPropertyMeta.isCounter(), "Index '%s' for table '%s' cannot be set on a counter table", name, tableName);
        indexedColumns.add(indexProperties);
        return this;
    }

    public String generateDDLScript() {
        StringBuilder ddl = new StringBuilder();

        ddl.append("\n");

        for (Map.Entry<String, String> columnEntry : columns.entrySet()) {
            appendAlterStart(ddl);
            ddl.append(columnEntry.getKey());
            ddl.append(" ");
            ddl.append(columnEntry.getValue());
            ddl.append(";\n");
        }
        for (Map.Entry<String, String> listEntry : lists.entrySet()) {
            appendAlterStart(ddl);
            ddl.append(listEntry.getKey());
            ddl.append(" list<");
            ddl.append(listEntry.getValue());
            ddl.append(">");
            ddl.append(";\n");
        }
        for (Map.Entry<String, String> setEntry : sets.entrySet()) {
            appendAlterStart(ddl);
            ddl.append(setEntry.getKey());
            ddl.append(" set<");
            ddl.append(setEntry.getValue());
            ddl.append(">");
            ddl.append(";\n");
        }
        for (Map.Entry<String, Pair<String, String>> mapEntry : maps.entrySet()) {
            appendAlterStart(ddl);
            ddl.append(mapEntry.getKey());
            ddl.append(" map<");
            ddl.append(mapEntry.getValue().left);
            ddl.append(",");
            ddl.append(mapEntry.getValue().right);
            ddl.append(">");
            ddl.append(";\n");
        }

        for (String counter : counters) {
            appendAlterStart(ddl);
            ddl.append(counter);
            ddl.append(" counter");
            ddl.append(";\n");
        }
        return ddl.toString();
    }

    private void appendAlterStart(StringBuilder ddl) {
        ddl.append("\tALTER TABLE ");
        ddl.append(tableName).append(" ADD ");
    }

    public Collection<String> generateIndices() {
        return super.generateIndices(indexedColumns, tableName);
    }


}
