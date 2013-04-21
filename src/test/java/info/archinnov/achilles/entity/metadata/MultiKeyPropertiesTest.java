package info.archinnov.achilles.entity.metadata;

import static info.archinnov.achilles.serializer.SerializerUtils.*;
import static org.fest.assertions.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;

import me.prettyprint.hector.api.Serializer;

import org.junit.Test;

/**
 * MultiKeyPropertiesTest
 * 
 * @author DuyHai DOAN
 * 
 */
public class MultiKeyPropertiesTest
{
	@SuppressWarnings("unchecked")
	@Test
	public void should_to_string() throws Exception
	{
		List<Class<?>> componentClasses = Arrays.asList((Class<?>) Integer.class, String.class);
		List<Serializer<?>> componentSerializers = Arrays.asList((Serializer<?>) INT_SRZ,
				STRING_SRZ);
		MultiKeyProperties props = new MultiKeyProperties();
		props.setComponentClasses(componentClasses);
		props.setComponentSerializers(componentSerializers);

		StringBuilder toString = new StringBuilder();
		toString.append("MultiKeyProperties [componentClasses=[");
		toString.append("java.lang.Integer,java.lang.String], ");
		toString.append("componentSerializers=[");
		toString.append(INT_SRZ.getComparatorType().getTypeName());
		toString.append(",");
		toString.append(STRING_SRZ.getComparatorType().getTypeName());
		toString.append("]]");

		assertThat(props.toString()).isEqualTo(toString.toString());
	}
}
