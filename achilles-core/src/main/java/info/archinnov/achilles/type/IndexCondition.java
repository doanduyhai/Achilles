package info.archinnov.achilles.type;


import com.google.common.base.Objects;

public class IndexCondition {

    private final String columnName;

    private final IndexRelation indexRelation;

    private final Object columnValue;

    public IndexCondition(String columnName, IndexRelation indexEquality, Object columnValue) {
        super();
        this.columnName = columnName;
        this.indexRelation = indexEquality;
        this.columnValue = columnValue;
    }

    public String getColumnName() {
        return columnName;
    }

    public IndexRelation getIndexRelation() {
        return indexRelation;
    }

    public Object getColumnValue() {
        return columnValue;
    }

    public String generateWhereClause() {
        return new StringBuilder().append(columnName.toLowerCase()).append(indexRelation)
                                  .append("'").append(columnValue).append("'").toString();
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(IndexCondition.class)
                      .add("columnName", columnValue)
                      .add("columnValue", columnValue)
                      .add("index relation", indexRelation)
                      .toString();
    }

}
