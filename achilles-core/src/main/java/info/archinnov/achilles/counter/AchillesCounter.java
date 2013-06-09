package info.archinnov.achilles.counter;

/**
 * AchillesCounter
 * 
 * @author DuyHai DOAN
 * 
 */
public interface AchillesCounter
{

	public static final String CQL_COUNTER_TABLE = "achilles_counter_table";
	public static final String CQL_COUNTER_FQCN = "fqcn";
	public static final String CQL_COUNTER_PRIMARY_KEY = "primary_key";
	public static final String CQL_COUNTER_PROPERTY_NAME = "property_name";
	public static final String CQL_COUNTER_VALUE = "counter_value";

	public static final String THRIFT_COUNTER_CF = "achillesCounterCF";

	public static enum CQLQueryType
	{
		INCR,
		DECR,
		SELECT,
		DELETE;
	}
}
