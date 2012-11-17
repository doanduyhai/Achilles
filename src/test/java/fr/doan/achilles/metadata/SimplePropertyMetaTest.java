package fr.doan.achilles.metadata;

import static fr.doan.achilles.metadata.PropertyType.SIMPLE;
import static org.fest.assertions.Assertions.assertThat;

import java.io.Serializable;

import me.prettyprint.cassandra.serializers.ObjectSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;

import org.junit.Test;

public class SimplePropertyMetaTest
{
	@Test
	public void should_infer_properties_for_string_type() throws Exception
	{
		SimplePropertyMeta<String> propertyMeta = new SimplePropertyMeta<String>("name", String.class);

		assertThat(propertyMeta.getValueClass()).isEqualTo(String.class);
		assertThat(propertyMeta.getName()).isEqualTo("name");
		assertThat(propertyMeta.getValueCanonicalClassName()).isEqualTo("java.lang.String");
		assertThat(propertyMeta.getValueSerializer()).isEqualTo(StringSerializer.get());
		assertThat(propertyMeta.propertyType()).isEqualTo(SIMPLE);
	}

	@Test
	public void should_infer_properties_for_generic_type() throws Exception
	{

		SimplePropertyMeta<TestClass> propertyMeta = new SimplePropertyMeta<TestClass>("name", TestClass.class);

		assertThat(propertyMeta.getValueClass()).isEqualTo(TestClass.class);
		assertThat(propertyMeta.getName()).isEqualTo("name");
		assertThat(propertyMeta.getValueCanonicalClassName()).isEqualTo("fr.doan.achilles.metadata.SimplePropertyMetaTest.TestClass");
		assertThat(propertyMeta.getValueSerializer()).isEqualTo(ObjectSerializer.get());
		assertThat(propertyMeta.propertyType()).isEqualTo(SIMPLE);
	}

	@Test
	public void should_get_object_with_correct_type() throws Exception
	{
		SimplePropertyMeta<TestClass> propertyMeta = new SimplePropertyMeta<TestClass>("name", TestClass.class);
		Object testClass = new TestClass();

		TestClass cast = propertyMeta.get(testClass);
		assertThat(cast).isInstanceOf(TestClass.class);

	}

	public class TestClass implements Serializable
	{
		private static final long serialVersionUID = 1L;
	};

}
