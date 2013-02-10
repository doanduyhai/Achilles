package info.archinnov.achilles.json;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize.Inclusion;

/**
 * DefaultObjectMapperFactory
 * 
 * @author DuyHai DOAN
 * 
 */
public class DefaultObjectMapperFactory implements ObjectMapperFactory
{
	private ObjectMapper mapper;

	public DefaultObjectMapperFactory() {
		mapper = new ObjectMapper();
		mapper.setSerializationInclusion(Inclusion.NON_NULL);
	}

	@Override
	public <T> ObjectMapper getMapper(Class<T> type)
	{
		return mapper;
	}

}
