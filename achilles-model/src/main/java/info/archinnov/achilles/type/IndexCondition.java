package info.archinnov.achilles.type;

public class IndexCondition {

	private final String columnName;
	private final IndexEquality indexEquality;
	private final Object columnValue;

	public IndexCondition(String columnName, IndexEquality indexEquality, Object columnValue) {
		super();
		this.columnName = columnName;
		this.indexEquality = indexEquality;
		this.columnValue = columnValue;
	}

	public String getColumnName() {
		return columnName;
	}

	public IndexEquality getIndexEquality() {
		return indexEquality;
	}

	public Object getColumnValue() {
		return columnValue;
	}

}
