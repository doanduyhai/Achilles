package info.archinnov.achilles.type;

import com.google.common.base.Objects;

public class IndexCondition {

	private final String columnName;

	private final IndexRelation indexRelation;

	private final Object columnValue;

	/**
	 * Shortcut constructor to build an EQUAL index condition
	 * 
	 * @param columnName
	 *            name of indexed column
	 * @param columnValue
	 *            value of indexed column
	 */
	public IndexCondition(String columnName, Object columnValue) {
		this.columnName = columnName;
		this.indexRelation = IndexRelation.EQUAL;
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

	@Override
	public String toString() {
		return Objects.toStringHelper(IndexCondition.class).add("columnName", columnValue)
				.add("columnValue", columnValue).add("index relation", indexRelation).toString();
	}

}
