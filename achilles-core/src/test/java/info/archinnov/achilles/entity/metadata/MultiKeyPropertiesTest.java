package info.archinnov.achilles.entity.metadata;

import static org.fest.assertions.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

/**
 * MultiKeyPropertiesTest
 * 
 * @author DuyHai DOAN
 * 
 */
public class MultiKeyPropertiesTest
{
	@Test
	public void should_to_string() throws Exception
	{
		List<Class<?>> componentClasses = Arrays.<Class<?>> asList(Integer.class, String.class);
		MultiKeyProperties props = new MultiKeyProperties();
		props.setComponentClasses(componentClasses);
		props.setComponentNames(Arrays.asList("id", "age"));

		StringBuilder toString = new StringBuilder();
		toString.append("MultiKeyProperties [componentClasses=[");
		toString.append("java.lang.Integer,java.lang.String], componentNames=[id, age]]");

		assertThat(props.toString()).isEqualTo(toString.toString());
	}

	@Test
	public void should_escape_component_names() throws Exception
	{
		MultiKeyProperties props = new MultiKeyProperties();
		props.setComponentNames(Arrays.asList("Id", "aGe"));

		assertThat(props.getCQLComponentNames()).containsExactly("id", "age");

	}
}
