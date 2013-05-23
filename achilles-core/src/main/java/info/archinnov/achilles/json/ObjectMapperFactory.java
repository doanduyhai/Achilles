package info.archinnov.achilles.json;

import org.codehaus.jackson.map.ObjectMapper;

/**
 * ObjectMapperFactory
 * 
 * @author DuyHai DOAN
 * 
 */
public interface ObjectMapperFactory
{
	public <T> ObjectMapper getMapper(Class<T> type);
}
