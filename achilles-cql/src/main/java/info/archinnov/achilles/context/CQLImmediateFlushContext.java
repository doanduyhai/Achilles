package info.archinnov.achilles.context;


/**
 * CQLImmediateFlushContext
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLImmediateFlushContext extends CQLAbstractFlushContext
{

	public CQLImmediateFlushContext(CQLDaoContext daoContext) {
		super(daoContext);
	}

	@Override
	public void flush()
	{
		super.doFlush();
	}

	@Override
	public FlushType type()
	{
		return FlushType.IMMEDIATE;
	}

}
