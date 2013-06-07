package info.archinnov.achilles.statement;

import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.proxy.MethodInvoker;

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
	private MethodInvoker invoker = new MethodInvoker();

	public BoundStatement bindForInsert(PreparedStatement ps, EntityMeta entityMeta, Object entity)
	{
		List<Object> values = new ArrayList<Object>();
		PropertyMeta<?, ?> idMeta = entityMeta.getIdMeta();
		bindPrimaryKey(entity, values, idMeta);
		for (PropertyMeta<?, ?> pm : entityMeta.getAllMetas())
		{
			Object value = invoker.getValueFromField(entity, pm.getGetter());
			values.add(pm.writeValueAsSupportedTypeOrString(value));
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

	public BoundStatement bindForUpdate(PreparedStatement ps, EntityMeta entityMeta,
			List<PropertyMeta<?, ?>> pms, Object entity)
	{
		List<Object> values = new ArrayList<Object>();
		PropertyMeta<?, ?> idMeta = entityMeta.getIdMeta();
		for (PropertyMeta<?, ?> pm : pms)
		{
			Object value = invoker.getValueFromField(entity, pm.getGetter());
			values.add(pm.writeValueAsSupportedTypeOrString(value));
		}
		bindPrimaryKey(entity, values, idMeta);
		return ps.bind(values);
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
