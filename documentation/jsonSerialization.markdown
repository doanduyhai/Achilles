## JSON Serialization

 Internally, the Thrift entity manager stores data in JSON format using the **Jackson Mapper** library.
 JSON serialization is way faster than the old plain Object serialization since only data are serialized,
 not class structure.
 
 By default, **Achilles** sets up an internal **Object Mapper** with the following feature config:
 
  1. Serialization Inclusion = NON NULL
  2. Deserialization FAIL_ON_UNKNOWN_PROPERTIES = false
  3. JacksonAnnotationIntrospector + JaxbAnnotationIntrospector
  
 **Jackson** will serialize all your entities even if they do not have any JSON annotations. You can also 
 use **JAXB** annotations.
 
 Otherwise you can inject a custom **Object Mapper* into **Achilles** as constructor argument for the
 `ThriftEntityManagerFactoryImpl` class.
 
 Last but not least, it is possible to further custom JSON serialization using the `ObjectMapperFactory`
 interface:
 
	public interface ObjectMapperFactory
	{
		public <T> ObjectMapper getMapper(Class<T> type);
	} 
 
 All you need is to pass an implementation of this interface to the `ThriftEntityManagerFactoryImpl` as
 a constructor argument.
 
  
 
 
 
 

  