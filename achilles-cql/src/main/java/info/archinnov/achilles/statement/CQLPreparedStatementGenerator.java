package info.archinnov.achilles.statement;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static info.archinnov.achilles.counter.AchillesCounter.*;
import static info.archinnov.achilles.counter.AchillesCounter.CQLQueryType.*;
import info.archinnov.achilles.counter.AchillesCounter;
import info.archinnov.achilles.counter.AchillesCounter.CQLQueryType;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Selection;
import com.datastax.driver.core.querybuilder.Update;
import com.datastax.driver.core.querybuilder.Update.Assignments;
import com.google.common.collect.FluentIterable;

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
		Insert insert = insertInto(entityMeta.getCQLTableName());
		prepareInsertPrimaryKey(idMeta, insert);

		List<PropertyMeta<?, ?>> nonProxyMetas = FluentIterable
				.from(entityMeta.getAllMetas())
				.filter(PropertyType.excludeProxyType)
				.toImmutableList();

		List<PropertyMeta<?, ?>> fieldMetas = new ArrayList<PropertyMeta<?, ?>>(nonProxyMetas);
		fieldMetas.remove(idMeta);

		for (PropertyMeta<?, ?> pm : fieldMetas)
		{
			insert.value(pm.getCQLPropertyName(), bindMarker());
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
				select = select().column(pm.getCQLPropertyName());
			}
			else
			{
				select = select();
				for (String component : pm.getMultiKeyProperties().getCQLComponentNames())
				{
					select = select.column(component);
				}
			}
			Select from = select.from(entityMeta.getCQLTableName());
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

	public PreparedStatement prepareUpdateFields(Session session, EntityMeta entityMeta,
			List<PropertyMeta<?, ?>> pms)
	{
		PropertyMeta<?, ?> idMeta = entityMeta.getIdMeta();
		Update update = update(entityMeta.getCQLTableName());

		int i = 0;
		Assignments assignments = null;
		for (PropertyMeta<?, ?> pm : pms)
		{
			if (i == 0)
			{
				assignments = update.with(set(pm.getCQLPropertyName(), bindMarker()));
			}
			else
			{
				assignments.and(set(pm.getCQLPropertyName(), bindMarker()));
			}
			i++;
		}
		Statement statement = prepareWhereClauseForUpdate(idMeta, assignments);
		return session.prepare(statement.getQueryString());
	}

	public PreparedStatement prepareSelectEagerPS(Session session, EntityMeta entityMeta)
	{
		PropertyMeta<?, ?> idMeta = entityMeta.getIdMeta();

		Selection select = select();

		for (PropertyMeta<?, ?> pm : entityMeta.getEagerMetas())
		{
			select.column(pm.getCQLPropertyName());
		}
		Select from = select.from(entityMeta.getCQLTableName());

		Statement statement = prepareWhereClauseForSelect(idMeta, from);
		return session.prepare(statement.getQueryString());
	}

	public Map<CQLQueryType, PreparedStatement> prepareSimpleCounterQueryMap(Session session)
	{
		StringBuilder incr = new StringBuilder();
		incr.append("UPDATE ").append(CQL_COUNTER_TABLE).append(" ");
		incr.append("SET ").append(CQL_COUNTER_VALUE).append(" = ");
		incr.append(CQL_COUNTER_VALUE).append(" + ? ");
		incr.append("WHERE ").append(CQL_COUNTER_FQCN).append(" = ? ");
		incr.append("AND ").append(CQL_COUNTER_PRIMARY_KEY).append(" = ? ");
		incr.append("AND ").append(CQL_COUNTER_PROPERTY_NAME).append(" = ?");

		StringBuilder decr = new StringBuilder();
		decr.append("UPDATE ").append(CQL_COUNTER_TABLE).append(" ");
		decr.append("SET ").append(CQL_COUNTER_VALUE).append(" = ");
		decr.append(CQL_COUNTER_VALUE).append(" - ? ");
		decr.append("WHERE ").append(CQL_COUNTER_FQCN).append(" = ? ");
		decr.append("AND ").append(CQL_COUNTER_PRIMARY_KEY).append(" = ? ");
		decr.append("AND ").append(CQL_COUNTER_PROPERTY_NAME).append(" = ?");

		StringBuilder select = new StringBuilder();
		select.append("SELECT ").append(CQL_COUNTER_VALUE).append(" ");
		select.append("FROM ").append(CQL_COUNTER_TABLE).append(" ");
		select.append("WHERE ").append(CQL_COUNTER_FQCN).append(" = ? ");
		select.append("AND ").append(CQL_COUNTER_PRIMARY_KEY).append(" = ? ");
		select.append("AND ").append(CQL_COUNTER_PROPERTY_NAME).append(" = ?");

		StringBuilder delete = new StringBuilder();
		delete.append("DELETE FROM ").append(CQL_COUNTER_TABLE).append(" ");
		delete.append("WHERE ").append(CQL_COUNTER_FQCN).append(" = ? ");
		delete.append("AND ").append(CQL_COUNTER_PRIMARY_KEY).append(" = ? ");
		delete.append("AND ").append(CQL_COUNTER_PROPERTY_NAME).append(" = ?");

		Map<CQLQueryType, PreparedStatement> counterPSMap = new HashMap<AchillesCounter.CQLQueryType, PreparedStatement>();
		counterPSMap.put(INCR, session.prepare(incr.toString()));
		counterPSMap.put(DECR, session.prepare(decr.toString()));
		counterPSMap.put(SELECT, session.prepare(select.toString()));
		counterPSMap.put(DELETE, session.prepare(delete.toString()));

		return counterPSMap;
	}

	private void prepareInsertPrimaryKey(PropertyMeta<?, ?> idMeta, Insert insert)
	{
		if (idMeta.type().isClusteredKey())
		{
			for (String component : idMeta.getMultiKeyProperties().getCQLComponentNames())
			{
				insert.value(component, bindMarker());
			}
		}
		else
		{
			insert.value(idMeta.getCQLPropertyName(), bindMarker());
		}
	}

	private Statement prepareWhereClauseForSelect(PropertyMeta<?, ?> idMeta, Select from)
	{
		Statement statement;
		if (idMeta.type().isClusteredKey())
		{
			Select.Where where = null;
			int i = 0;
			for (String clusteredId : idMeta.getMultiKeyProperties().getCQLComponentNames())
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
			statement = from.where(eq(idMeta.getCQLPropertyName(), bindMarker()));
		}
		return statement;
	}

	private Statement prepareWhereClauseForUpdate(PropertyMeta<?, ?> idMeta, Assignments update)
	{
		Statement statement;
		if (idMeta.type().isClusteredKey())
		{
			Update.Where where = null;
			int i = 0;
			for (String clusteredId : idMeta.getMultiKeyProperties().getCQLComponentNames())
			{
				if (i == 0)
				{
					where = update.where(eq(clusteredId, bindMarker()));
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
			statement = update.where(eq(idMeta.getCQLPropertyName(), bindMarker()));
		}
		return statement;
	}

	public Map<String, PreparedStatement> prepareRemovePSs(Session session, EntityMeta entityMeta)
	{
		PropertyMeta<?, ?> idMeta = entityMeta.getIdMeta();

		Map<String, PreparedStatement> removePSs = new HashMap<String, PreparedStatement>();

		Delete mainFrom = QueryBuilder.delete().from(entityMeta.getCQLTableName());
		Statement mainStatement = prepareWhereClauseForDelete(idMeta, mainFrom);
		removePSs.put(entityMeta.getTableName(), session.prepare(mainStatement.getQueryString()));
		for (PropertyMeta<?, ?> pm : entityMeta.getAllMetas())
		{
			switch (pm.type())
			{
				case WIDE_MAP:
				case JOIN_WIDE_MAP:
				case COUNTER_WIDE_MAP:
					Delete wideMapFrom = QueryBuilder.delete().from(pm.getCQLExternalTableName());
					Statement wideMapStatement = prepareWhereClauseForDelete(idMeta, wideMapFrom);
					removePSs.put(pm.getExternalTableName(),
							session.prepare(wideMapStatement.getQueryString()));
					break;

				case COUNTER:
					Statement counterStatement = QueryBuilder
							.delete()
							.from(AchillesCounter.CQL_COUNTER_TABLE)
							.where(eq(AchillesCounter.CQL_COUNTER_FQCN, bindMarker()))
							.and(eq(AchillesCounter.CQL_COUNTER_PRIMARY_KEY, bindMarker()));
					removePSs.put(AchillesCounter.CQL_COUNTER_TABLE,
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
			for (String clusteredId : idMeta.getMultiKeyProperties().getCQLComponentNames())
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
			mainStatement = mainFrom.where(eq(idMeta.getCQLPropertyName(), bindMarker()));
		}
		return mainStatement;
	}
}
