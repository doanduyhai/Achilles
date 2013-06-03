package info.archinnov.achilles.statement.cache;

import java.util.Set;

/**
 * PreparedStatementCacheKey
 * 
 * @author DuyHai DOAN
 * 
 */
public class StatementCacheKey
{
	private CacheType type;

	private String tableName;

	private Set<String> fields;

	private Class<?> entityClass;

	public StatementCacheKey(CacheType type, String tableName, Set<String> fields,
			Class<?> entityClass)
	{
		this.type = type;
		this.entityClass = entityClass;
		this.tableName = tableName;
		this.fields = fields;
	}

	public CacheType getType()
	{
		return type;
	}

	public String getTableName()
	{
		return tableName;
	}

	public Set<String> getFields()
	{
		return fields;
	}

	public Class<?> getEntityClass()
	{
		return entityClass;
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((fields == null) ? 0 : fields.hashCode());
		result = prime * result + ((tableName == null) ? 0 : tableName.hashCode());
		result = prime * result + ((type == null) ? 0 : type.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		StatementCacheKey other = (StatementCacheKey) obj;
		if (fields == null)
		{
			if (other.fields != null)
				return false;
		}
		else if (!fields.equals(other.fields))
			return false;
		if (tableName == null)
		{
			if (other.tableName != null)
				return false;
		}
		else if (!tableName.equals(other.tableName))
			return false;
		if (type != other.type)
			return false;
		return true;
	}
}
