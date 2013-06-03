package info.archinnov.achilles.statement;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static info.archinnov.achilles.context.CQLDaoContext.*;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;

import java.util.HashMap;
import java.util.Map;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Selection;

/**
 * CQLPreparedStatementHelper
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLPreparedStatementGenerator
{
	public PreparedStatement prepareInsertPS(Session session, EntityMeta entityMeta)
	{
		PropertyMeta<?, ?> idMeta = entityMeta.getIdMeta();
		Insert insert = insertInto(entityMeta.getTableName());
		prepareInsertPrimaryKey(idMeta, insert);

		for (PropertyMeta<?, ?> pm : entityMeta.getAllMetas())
		{
			if (!pm.type().isProxyType())
			{
				insert.value(pm.getPropertyName(), bindMarker());
			}
		}
		return session.prepare(insert.getQueryString());
	}

	public PreparedStatement prepareSelectFieldPS(Session session, EntityMeta entityMeta,
			PropertyMeta<?, ?> pm)
	{
		PropertyMeta<?, ?> idMeta = entityMeta.getIdMeta();

		if (!pm.isProxyType())
		{
			Selection select;
			if (pm.isSingleKey())
			{
				select = select().column(pm.getPropertyName());
			}
			else
			{
				select = select();
				for (String component : pm.getMultiKeyProperties().getComponentNames())
				{
					select = select.column(component);
				}
			}
			Select from = select.from(entityMeta.getTableName());
			Statement statement = prepareWhereClauseForSelect(idMeta, from);
			return session.prepare(statement.getQueryString());
		}
		else
		{
			throw new IllegalArgumentException("Cannot prepare statement for property '"
					+ pm.getPropertyName() + "' of entity '" + entityMeta.getClassName()
					+ "' because it is of proxy type");
		}
	}

	public PreparedStatement prepareSelectEagerPS(Session session, EntityMeta entityMeta)
	{
		PropertyMeta<?, ?> idMeta = entityMeta.getIdMeta();

		Selection select = select();

		for (PropertyMeta<?, ?> pm : entityMeta.getEagerMetas())
		{
			select.column(pm.getPropertyName());
		}
		Select from = select.from(entityMeta.getTableName());

		Statement statement = prepareWhereClauseForSelect(idMeta, from);
		return session.prepare(statement.getQueryString());
	}

	private void prepareInsertPrimaryKey(PropertyMeta<?, ?> idMeta, Insert insert)
	{
		if (idMeta.type().isClusteredKey())
		{
			for (String component : idMeta.getMultiKeyProperties().getComponentNames())
			{
				insert.value(component, bindMarker());
			}
		}
		else
		{
			insert.value(idMeta.getPropertyName(), bindMarker());
		}
	}

	private Statement prepareWhereClauseForSelect(PropertyMeta<?, ?> idMeta, Select from)
	{
		Statement statement;
		if (idMeta.type().isClusteredKey())
		{
			Select.Where where = null;
			int i = 0;
			for (String clusteredId : idMeta.getMultiKeyProperties().getComponentNames())
			{
				if (i == 0)
				{
					where = from.where(eq(clusteredId, bindMarker()));
				}
				else
				{
					where.and(eq(clusteredId, bindMarker()));
				}
				i++;
			}
			statement = where;
		}
		else
		{
			statement = from.where(eq(idMeta.getPropertyName(), bindMarker()));
		}
		return statement;
	}

	public Map<String, PreparedStatement> prepareRemovePSs(Session session, EntityMeta entityMeta)
	{
		PropertyMeta<?, ?> idMeta = entityMeta.getIdMeta();

		Map<String, PreparedStatement> removePSs = new HashMap<String, PreparedStatement>();

		Delete mainFrom = QueryBuilder.delete().from(entityMeta.getTableName());
		Statement mainStatement = prepareWhereClauseForDelete(idMeta, mainFrom);
		removePSs.put(entityMeta.getTableName(), session.prepare(mainStatement.getQueryString()));
		for (PropertyMeta<?, ?> pm : entityMeta.getAllMetas())
		{
			switch (pm.type())
			{
				case WIDE_MAP:
				case JOIN_WIDE_MAP:
				case COUNTER_WIDE_MAP:
					Delete wideMapFrom = QueryBuilder.delete().from(pm.getExternalTableName());
					Statement wideMapStatement = prepareWhereClauseForDelete(idMeta, wideMapFrom);
					removePSs.put(pm.getExternalTableName(),
							session.prepare(wideMapStatement.getQueryString()));
					break;

				case COUNTER:
					Statement counterStatement = QueryBuilder
							.delete()
							.from(ACHILLES_COUNTER_TABLE)
							.where(eq(ACHILLES_COUNTER_FQCN, bindMarker()))
							.and(eq(ACHILLES_COUNTER_PK, bindMarker()));
					removePSs.put(ACHILLES_COUNTER_TABLE,
							session.prepare(counterStatement.getQueryString()));
					break;
				default:
					break;
			}
		}
		return removePSs;
	}

	private Statement prepareWhereClauseForDelete(PropertyMeta<?, ?> idMeta, Delete mainFrom)
	{
		Statement mainStatement;
		if (idMeta.type().isClusteredKey())
		{
			Delete.Where where = null;
			int i = 0;
			for (String clusteredId : idMeta.getMultiKeyProperties().getComponentNames())
			{
				if (i == 0)
				{
					where = mainFrom.where(eq(clusteredId, bindMarker()));
				}
				else
				{
					where.and(eq(clusteredId, bindMarker()));
				}
				i++;
			}

			mainStatement = where;
		}
		else
		{
			mainStatement = mainFrom.where(eq(idMeta.getPropertyName(), bindMarker()));
		}
		return mainStatement;
	}
}
