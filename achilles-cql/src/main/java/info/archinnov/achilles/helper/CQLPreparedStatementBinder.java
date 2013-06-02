package info.archinnov.achilles.helper;

import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.proxy.AchillesMethodInvoker;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;

/**
 * CQLPreparedStatementBinder
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLPreparedStatementBinder
{
	private AchillesMethodInvoker invoker = new AchillesMethodInvoker();

	public BoundStatement bindForInsert(PreparedStatement ps, EntityMeta entityMeta, Object entity)
	{
		List<Object> values = new ArrayList<Object>();
		PropertyMeta<?, ?> idMeta = entityMeta.getIdMeta();
		bindPrimaryKey(entity, values, idMeta);
		for (PropertyMeta<?, ?> pm : entityMeta.getAllMetas())
		{
			values.add(invoker.getValueFromField(entity, pm.getGetter()));
		}
		return ps.bind(values);
	}

	private void bindPrimaryKey(Object entity, List<Object> values, PropertyMeta<?, ?> idMeta)
	{
		if (idMeta.type().isClusteredKey())
		{
			for (Method componentGetter : idMeta.getMultiKeyProperties().getComponentGetters())
			{
				values.add(invoker.getValueFromField(entity, componentGetter));
			}
		}
		else
		{
			values.add(invoker.getValueFromField(entity, idMeta.getGetter()));
		}
	}

	public BoundStatement bindStatementWithOnlyPKInWhereClause(PreparedStatement ps,
			EntityMeta entityMeta, Object entity)
	{
		List<Object> values = new ArrayList<Object>();
		PropertyMeta<?, ?> idMeta = entityMeta.getIdMeta();
		bindPrimaryKey(entity, values, idMeta);
		return ps.bind(values);
	}

}
