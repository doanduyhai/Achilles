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
	public static final String ACHILLES_COUNTER_TABLE = "achillesCounterTable";
	public static final String ACHILLES_COUNTER_FQCN = "fqcn";
	public static final String ACHILLES_COUNTER_PK = "pk";

	private Session session;

	public CQLDaoContext(Session session) {
		this.session = session;
	}

}
