package info.archinnov.achilles.context;

import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.helper.CQLPreparedStatementHelper;

import java.util.Map;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.google.common.base.Function;
import com.google.common.collect.Maps;

/**
 * CQLDaoContextBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLDaoContextBuilder
{
	private CQLPreparedStatementHelper preparedStatementHelper = new CQLPreparedStatementHelper();
	private Session session;

	private Function<EntityMeta, PreparedStatement> insertPSTransformer = new Function<EntityMeta, PreparedStatement>()
	{
		@Override
		public PreparedStatement apply(EntityMeta meta)
		{
			return preparedStatementHelper.prepareInsertPS(session, meta);
		}
	};

	private Function<EntityMeta, PreparedStatement> selectForExistenceCheckPSTransformer = new Function<EntityMeta, PreparedStatement>()
	{
		@Override
		public PreparedStatement apply(EntityMeta meta)
		{
			return preparedStatementHelper.prepareSelectForExistenceCheckPS(session, meta);
		}
	};

	private Function<EntityMeta, PreparedStatement> selectEagerPSTransformer = new Function<EntityMeta, PreparedStatement>()
	{
		@Override
		public PreparedStatement apply(EntityMeta meta)
		{
			return preparedStatementHelper.prepareSelectEagerPS(session, meta);
		}
	};

	private Function<EntityMeta, Map<String, PreparedStatement>> removePSTransformer = new Function<EntityMeta, Map<String, PreparedStatement>>()
	{
		@Override
		public Map<String, PreparedStatement> apply(EntityMeta meta)
		{
			return preparedStatementHelper.prepareRemovePSs(session, meta);
		}

	};

	public CQLDaoContextBuilder(Session session) {
		this.session = session;
	}

	public CQLDaoContext build(Session session, Map<Class<?>, EntityMeta> entityMetaMap)
	{
		Map<Class<?>, PreparedStatement> insertPSMap = Maps.transformValues(entityMetaMap,
				insertPSTransformer);
		Map<Class<?>, PreparedStatement> selectForExistenceCheckPSMap = Maps.transformValues(
				entityMetaMap, selectForExistenceCheckPSTransformer);

		Map<Class<?>, PreparedStatement> selectEagerPSMap = Maps.transformValues(entityMetaMap,
				selectEagerPSTransformer);
		Map<Class<?>, Map<String, PreparedStatement>> removePSMap = Maps.transformValues(
				entityMetaMap, removePSTransformer);

		return new CQLDaoContext(insertPSMap, selectForExistenceCheckPSMap, selectEagerPSMap,
				removePSMap, session);
	}

}
