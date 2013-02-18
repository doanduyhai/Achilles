package info.archinnov.achilles.json;

import org.codehaus.jackson.map.DeserializationConfig;
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
		mapper.getSerializationConfig().withSerializationInclusion(Inclusion.NON_NULL);
		mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	}

	@Override
	public <T> ObjectMapper getMapper(Class<T> type)
	{
		return mapper;
	}

}
