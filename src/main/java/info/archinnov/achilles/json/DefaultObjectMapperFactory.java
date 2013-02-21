package info.archinnov.achilles.json;

import org.codehaus.jackson.map.AnnotationIntrospector;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize.Inclusion;
import org.codehaus.jackson.map.introspect.JacksonAnnotationIntrospector;
import org.codehaus.jackson.xc.JaxbAnnotationIntrospector;

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

		AnnotationIntrospector primary = new JacksonAnnotationIntrospector();
		AnnotationIntrospector secondary = new JaxbAnnotationIntrospector();
		AnnotationIntrospector pair = new AnnotationIntrospector.Pair(primary, secondary);

		mapper.setAnnotationIntrospector(pair);
	}

	@Override
	public <T> ObjectMapper getMapper(Class<T> type)
	{
		return mapper;
	}

}
