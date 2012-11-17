package fr.doan.achilles.parser;

import static org.fest.assertions.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import fr.doan.achilles.metadata.ListPropertyMeta;
import fr.doan.achilles.metadata.MapPropertyMeta;
import fr.doan.achilles.metadata.PropertyMeta;
import fr.doan.achilles.metadata.PropertyType;
import fr.doan.achilles.metadata.SetPropertyMeta;
import fr.doan.achilles.metadata.SimplePropertyMeta;
import fr.doan.achilles.serializer.Utils;

@SuppressWarnings("unused")
public class PropertyParserTest
{

	@Test
	public void should_parse_simple_property_string() throws Exception
	{
		class Test
		{
			private String name;
		}

		PropertyMeta<String> meta = PropertyParser.parse(Test.class.getDeclaredField("name"), "name");

		assertThat(meta).isInstanceOf(SimplePropertyMeta.class);
		assertThat(meta.getName()).isEqualTo("name");
		assertThat(meta.getValueClass()).isEqualTo(String.class);
		assertThat(meta.getValueCanonicalClassName()).isEqualTo("java.lang.String");
		assertThat(meta.getValueSerializer()).isEqualTo(Utils.STRING_SRZ);
		assertThat(meta.propertyType()).isEqualTo(PropertyType.SIMPLE);
	}

	@Test
	public void should_parse_simple_property_and_override_name() throws Exception
	{
		class Test
		{
			private String name;
		}

		PropertyMeta<String> meta = PropertyParser.parse(Test.class.getDeclaredField("name"), "firstname");

		assertThat(meta.getName()).isEqualTo("firstname");
	}

	@Test
	public void should_parse_list() throws Exception
	{
		class Test
		{
			private List<String> friends;
		}

		PropertyMeta<String> meta = PropertyParser.parse(Test.class.getDeclaredField("friends"), "friends");

		assertThat(meta).isInstanceOf(ListPropertyMeta.class);
		assertThat(meta.getName()).isEqualTo("friends");
		assertThat(meta.getValueClass()).isEqualTo(String.class);
		assertThat(meta.getValueCanonicalClassName()).isEqualTo("java.lang.String");
		assertThat(meta.getValueSerializer()).isEqualTo(Utils.STRING_SRZ);
		assertThat(meta.propertyType()).isEqualTo(PropertyType.LIST);
	}

	@Test
	public void should_parse_set() throws Exception
	{
		class Test
		{
			private Set<Long> followers;
		}

		PropertyMeta<Long> meta = PropertyParser.parse(Test.class.getDeclaredField("followers"), "followers");

		assertThat(meta).isInstanceOf(SetPropertyMeta.class);
		assertThat(meta.getName()).isEqualTo("followers");
		assertThat(meta.getValueClass()).isEqualTo(Long.class);
		assertThat(meta.getValueCanonicalClassName()).isEqualTo("java.lang.Long");
		assertThat(meta.getValueSerializer()).isEqualTo(Utils.LONG_SRZ);
		assertThat(meta.propertyType()).isEqualTo(PropertyType.SET);
	}

	@Test
	public void should_parse_map() throws Exception
	{
		class Test
		{
			private Map<Integer, String> preferences;
		}

		PropertyMeta<String> meta = PropertyParser.parse(Test.class.getDeclaredField("preferences"), "preferences");

		assertThat(meta).isInstanceOf(MapPropertyMeta.class);
		assertThat(meta.getName()).isEqualTo("preferences");
		assertThat(meta.getValueClass()).isEqualTo(String.class);
		assertThat(meta.getValueCanonicalClassName()).isEqualTo("java.lang.String");
		assertThat(meta.getValueSerializer()).isEqualTo(Utils.STRING_SRZ);
		assertThat(meta.propertyType()).isEqualTo(PropertyType.MAP);

		MapPropertyMeta<String> mapMeta = (MapPropertyMeta<String>) meta;

		assertThat(mapMeta.getKeyClass()).isEqualTo(Integer.class);
		assertThat(mapMeta.getKeyClassSerializer()).isEqualTo(Utils.INT_SRZ);
	}
}
