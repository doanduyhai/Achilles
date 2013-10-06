package info.archinnov.achilles.type;

public class IndexCondition {

	private final Object columnName;
	private final IndexEquality indexEquality;
	private final Object columnValue;

	public IndexCondition(Object columnName, IndexEquality indexEquality, Object columnValue) {
		super();
		this.columnName = columnName;
		this.indexEquality = indexEquality;
		this.columnValue = columnValue;
	}

	public Object getColumnName() {
		return columnName;
	}

	public IndexEquality getIndexEquality() {
		return indexEquality;
	}

	public Object getColumnValue() {
		return columnValue;
	}

}
