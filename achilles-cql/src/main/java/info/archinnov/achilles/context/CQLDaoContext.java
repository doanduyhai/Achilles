package info.archinnov.achilles.context;

import com.datastax.driver.core.Session;

/**
 * CQLDaoContext
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLDaoContext
{
	private Session session;

	public CQLDaoContext(Session session) {
		this.session = session;
	}

}
