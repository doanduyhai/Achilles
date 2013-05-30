package info.archinnov.achilles.entity.operations.impl;

import com.datastax.driver.core.Session;

/**
 * CQLPersisterImpl
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLPersisterImpl
{
	private Session session;

	public CQLPersisterImpl(Session session) {
		this.session = session;
	}

}
