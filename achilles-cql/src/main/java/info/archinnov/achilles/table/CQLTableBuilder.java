package info.archinnov.achilles.table;

import static com.datastax.driver.core.DataType.Name.COUNTER;
import static info.archinnov.achilles.cql.CQLTypeMapper.toCQLType;
import static info.archinnov.achilles.table.TableCreator.ACHILLES_DDL_SCRIPT;
import static info.archinnov.achilles.table.TableNameNormalizer.normalizerAndValidateColumnFamilyName;
import info.archinnov.achilles.validation.Validator;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.cassandra.utils.Pair;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CQLTableBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLTableBuilder {

    private static final Logger log = LoggerFactory.getLogger(ACHILLES_DDL_SCRIPT);

    private String tableName;
    private String comment;
    private List<String> primaryKeys = new ArrayList<String>();
    private Map<String, String> columns = new LinkedHashMap<String, String>();
    private Map<String, String> lists = new LinkedHashMap<String, String>();
    private Map<String, String> sets = new LinkedHashMap<String, String>();
    private Map<String, Pair<String, String>> maps = new LinkedHashMap<String, Pair<String, String>>();

    private boolean counter;

    public static CQLTableBuilder createTable(String tableName)
    {
        return new CQLTableBuilder(tableName, false);
    }

    public static CQLTableBuilder createCounterTable(String tableName)
    {
        return new CQLTableBuilder(tableName, true);
    }

    private CQLTableBuilder(String tableName, boolean counter) {
        this.counter = counter;
        this.tableName = normalizerAndValidateColumnFamilyName(tableName);
    }

    public CQLTableBuilder addColumn(String columnName, Class<?> javaType)
    {
        columns.put(columnName, toCQLType(javaType).toString());
        return this;
    }

    public CQLTableBuilder addList(String listName, Class<?> javaValueType)
    {
        lists.put(listName, toCQLType(javaValueType).toString());
        return this;
    }

    public CQLTableBuilder addSet(String setName, Class<?> javaValueType)
    {
        sets.put(setName, toCQLType(javaValueType).toString());
        return this;
    }

    public CQLTableBuilder addMap(String mapName, Class<?> javaKeyType, Class<?> javaValueType)
    {
        maps.put(mapName, Pair.create(toCQLType(javaKeyType).toString(), toCQLType(javaValueType)
                .toString()));
        return this;
    }

    public CQLTableBuilder addPrimaryKey(String columnName)
    {
        Validator.validateFalse(lists.containsKey(columnName),
                "Primary key '%s' for table '%s' cannot be of list type", columnName, tableName);
        Validator.validateFalse(sets.containsKey(columnName),
                "Primary key '%s' for table '%s' cannot be of set type", columnName, tableName);
        Validator.validateFalse(maps.containsKey(columnName),
                "Primary key '%s' for table '%s' cannot be of map type", columnName, tableName);

        Validator.validateTrue(columns.containsKey(columnName),
                "Property '%s' for table '%s' cannot be found. Did you forget to declare it as column first ?",
                columnName, tableName);

        primaryKeys.add(columnName);

        return this;
    }

    public CQLTableBuilder addPrimaryKeys(List<String> columnsName)
    {
        for (String columnName : columnsName)
        {
            addPrimaryKey(columnName);
        }
        return this;
    }

    public CQLTableBuilder addComment(String comment)
    {
        Validator.validateNotBlank(comment, "Comment for table '%s' should not be blank", tableName);
        this.comment = comment.replaceAll("'", "\"");
        return this;
    }

    public String generateDDLScript()
    {

        String ddlScript;
        if (counter)
        {
            ddlScript = generateCounterTable();
        }
        else
        {
            ddlScript = generateTable();
        }

        log.debug(ddlScript);

        return ddlScript;
    }

    private String generateTable() {
        StringBuilder ddl = new StringBuilder();

        ddl.append("\n");
        ddl.append("\tCREATE TABLE ");
        ddl.append(tableName).append("(\n");

        for (Entry<String, String> columnEntry : columns.entrySet())
        {
            ddl.append("\t\t");
            ddl.append(columnEntry.getKey());
            ddl.append(" ");
            ddl.append(columnEntry.getValue());
            ddl.append(",\n");
        }
        for (Entry<String, String> listEntry : lists.entrySet())
        {
            ddl.append("\t\t");
            ddl.append(listEntry.getKey());
            ddl.append(" list<");
            ddl.append(listEntry.getValue());
            ddl.append(">");
            ddl.append(",\n");
        }
        for (Entry<String, String> setEntry : sets.entrySet())
        {
            ddl.append("\t\t");
            ddl.append(setEntry.getKey());
            ddl.append(" set<");
            ddl.append(setEntry.getValue());
            ddl.append(">");
            ddl.append(",\n");
        }
        for (Entry<String, Pair<String, String>> mapEntry : maps.entrySet())
        {
            ddl.append("\t\t");
            ddl.append(mapEntry.getKey());
            ddl.append(" map<");
            ddl.append(mapEntry.getValue().left);
            ddl.append(",");
            ddl.append(mapEntry.getValue().right);
            ddl.append(">");
            ddl.append(",\n");;
        }

        ddl.append("\t\t");
        ddl.append("PRIMARY KEY(");
        ddl.append(StringUtils.join(primaryKeys, ", "));
        ddl.append(")\n");

        ddl.append("\t)");

        // Add comments
        ddl.append(" WITH COMMENT = '").append(comment).append("'");
        return ddl.toString();
    }

    private String generateCounterTable() {

        Validator.validateTrue(columns.size() == primaryKeys.size() + 1,
                "Counter table '%s' should contain only one counter column and primary keys", tableName);

        StringBuilder ddl = new StringBuilder();

        ddl.append("\n");
        ddl.append("\tCREATE TABLE ");
        ddl.append(tableName).append("(\n");

        for (Entry<String, String> columnEntry : columns.entrySet())
        {
            String columnName = columnEntry.getKey();
            String valueType = columnEntry.getValue();

            ddl.append("\t\t");
            ddl.append(columnName);
            ddl.append(" ");
            if (primaryKeys.contains(columnName))
            {
                ddl.append(valueType);
            }
            else
            {
                Validator.validateTrue(StringUtils.equals(valueType, COUNTER.toString()),
                        "Column '%s' of table '%s' should be of type 'counter'", columnName, tableName);
                ddl.append("counter");
            }
            ddl.append(",\n");
        }

        ddl.append("\t\t");
        ddl.append("PRIMARY KEY(");
        ddl.append(StringUtils.join(primaryKeys, ", "));
        ddl.append(")\n");

        ddl.append("\t)");

        // Add comments
        ddl.append(" WITH COMMENT = '").append(comment).append("'");
        return ddl.toString();
    }
}
