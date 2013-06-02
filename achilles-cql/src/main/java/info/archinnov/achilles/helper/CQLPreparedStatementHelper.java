package info.archinnov.achilles.helper;

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
public class CQLPreparedStatementHelper
{
	public PreparedStatement prepareInsertPS(Session session, EntityMeta meta)
	{
		Insert insert = insertInto(meta.getTableName());
		for (PropertyMeta<?, ?> pm : meta.getAllMetas())
		{
			if (!pm.type().isProxyType())
			{
				insert.value(pm.getPropertyName(), bindMarker());
			}
		}
		return session.prepare(insert.getQueryString());
	}

	public PreparedStatement prepareSelectForExistenceCheckPS(Session session, EntityMeta meta)
	{
		PropertyMeta<?, ?> idMeta = meta.getIdMeta();
		Select select = select().column(idMeta.getPropertyName()).from(meta.getTableName());
		Statement statement = prepareWhereClauseForSelect(idMeta, select);
		return session.prepare(statement.getQueryString());
	}

	public PreparedStatement prepareSelectEagerPS(Session session, EntityMeta meta)
	{
		PropertyMeta<?, ?> idMeta = meta.getIdMeta();

		Selection select = select();

		for (PropertyMeta<?, ?> pm : meta.getEagerMetas())
		{
			select.column(pm.getPropertyName());
		}
		Select from = select.from(meta.getTableName());

		Statement statement = prepareWhereClauseForSelect(idMeta, from);
		return session.prepare(statement.getQueryString());
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

	public Map<String, PreparedStatement> prepareRemovePSs(Session session, EntityMeta meta)
	{
		PropertyMeta<?, ?> idMeta = meta.getIdMeta();

		Map<String, PreparedStatement> removePSs = new HashMap<String, PreparedStatement>();

		Delete mainFrom = QueryBuilder.delete().from(meta.getTableName());
		Statement mainStatement = prepareWhereClauseForDelete(idMeta, mainFrom);
		removePSs.put(meta.getTableName(), session.prepare(mainStatement.getQueryString()));
		for (PropertyMeta<?, ?> pm : meta.getAllMetas())
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
