package fr.doan.achilles.parser;

import static fr.doan.achilles.serializer.Utils.STRING_SRZ;
import static org.fest.assertions.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.Basic;
import javax.persistence.FetchType;

import me.prettyprint.hector.api.Serializer;

import org.junit.Test;

import fr.doan.achilles.exception.ValidationException;
import fr.doan.achilles.metadata.ListPropertyMeta;
import fr.doan.achilles.metadata.MapPropertyMeta;
import fr.doan.achilles.metadata.PropertyMeta;
import fr.doan.achilles.metadata.PropertyType;
import fr.doan.achilles.metadata.SetPropertyMeta;
import fr.doan.achilles.metadata.SimplePropertyMeta;
import fr.doan.achilles.serializer.Utils;

@SuppressWarnings(
{
		"unused",
		"rawtypes"
})
public class PropertyParserTest
{

	private final PropertyParser parser = new PropertyParser();

	@SuppressWarnings("unchecked")
	@Test
	public void should_parse_simple_property_string() throws Exception
	{
		class Test
		{
			private String name;

			public String getName()
			{
				return name;
			}

			public void setName(String name)
			{
				this.name = name;
			}
		}

		PropertyMeta<String> meta = parser.parse(Test.class, Test.class.getDeclaredField("name"), "name");

		assertThat(meta).isInstanceOf(SimplePropertyMeta.class);
		assertThat(meta.getPropertyName()).isEqualTo("name");
		assertThat(meta.getValueClass()).isEqualTo(String.class);
		assertThat((Serializer<String>) meta.getValueSerializer()).isEqualTo(STRING_SRZ);

		assertThat(meta.getGetter().getName()).isEqualTo("getName");
		assertThat((Class) meta.getGetter().getReturnType()).isEqualTo(String.class);
		assertThat(meta.getSetter().getName()).isEqualTo("setName");
		assertThat((Class) meta.getSetter().getParameterTypes()[0]).isEqualTo(String.class);

		assertThat(meta.propertyType()).isEqualTo(PropertyType.SIMPLE);
	}

	@Test
	public void should_parse_simple_property_and_override_name() throws Exception
	{
		class Test
		{
			private String name;

			public String getName()
			{
				return name;
			}

			public void setName(String name)
			{
				this.name = name;
			}
		}

		PropertyMeta<String> meta = parser.parse(Test.class, Test.class.getDeclaredField("name"), "firstname");

		assertThat(meta.getPropertyName()).isEqualTo("firstname");
	}

	@Test
	public void should_parse_lazy() throws Exception
	{
		class Test
		{
			@Basic(fetch = FetchType.LAZY)
			private List<String> friends;

			public List<String> getFriends()
			{
				return friends;
			}

			public void setFriends(List<String> friends)
			{
				this.friends = friends;
			}
		}

		PropertyMeta<String> meta = parser.parse(Test.class, Test.class.getDeclaredField("friends"), "friends");

		assertThat(meta.isLazy()).isTrue();
	}

	@Test
	public void should_parse_eager() throws Exception
	{
		class Test
		{
			@Basic(fetch = FetchType.EAGER)
			private List<String> friends;

			public List<String> getFriends()
			{
				return friends;
			}

			public void setFriends(List<String> friends)
			{
				this.friends = friends;
			}
		}

		PropertyMeta<String> meta = parser.parse(Test.class, Test.class.getDeclaredField("friends"), "friends");

		assertThat(meta.isLazy()).isFalse();
	}

	@Test
	public void should_parse_eager_as_default() throws Exception
	{
		class Test
		{
			@Basic
			private List<String> friends;

			public List<String> getFriends()
			{
				return friends;
			}

			public void setFriends(List<String> friends)
			{
				this.friends = friends;
			}
		}

		PropertyMeta<String> meta = parser.parse(Test.class, Test.class.getDeclaredField("friends"), "friends");

		assertThat(meta.isLazy()).isFalse();
	}

	@Test
	public void should_parse_list() throws Exception
	{
		class Test
		{
			private List<String> friends;

			public List<String> getFriends()
			{
				return friends;
			}

			public void setFriends(List<String> friends)
			{
				this.friends = friends;
			}
		}

		PropertyMeta<String> meta = parser.parse(Test.class, Test.class.getDeclaredField("friends"), "friends");

		assertThat(meta).isInstanceOf(ListPropertyMeta.class);
		assertThat(meta.getPropertyName()).isEqualTo("friends");
		assertThat(meta.getValueClass()).isEqualTo(String.class);
		assertThat((Serializer) meta.getValueSerializer()).isEqualTo(Utils.STRING_SRZ);

		assertThat(meta.getGetter().getName()).isEqualTo("getFriends");
		assertThat((Class) meta.getGetter().getReturnType()).isEqualTo(List.class);
		assertThat(meta.getSetter().getName()).isEqualTo("setFriends");
		assertThat((Class) meta.getSetter().getParameterTypes()[0]).isEqualTo(List.class);

		assertThat(meta.propertyType()).isEqualTo(PropertyType.LIST);
	}

	@Test
	public void should_parse_set() throws Exception
	{
		class Test
		{
			private Set<Long> followers;

			public Set<Long> getFollowers()
			{
				return followers;
			}

			public void setFollowers(Set<Long> followers)
			{
				this.followers = followers;
			}
		}

		PropertyMeta<Long> meta = parser.parse(Test.class, Test.class.getDeclaredField("followers"), "followers");

		assertThat(meta).isInstanceOf(SetPropertyMeta.class);
		assertThat(meta.getPropertyName()).isEqualTo("followers");
		assertThat(meta.getValueClass()).isEqualTo(Long.class);
		assertThat((Serializer) meta.getValueSerializer()).isEqualTo(Utils.LONG_SRZ);

		assertThat(meta.getGetter().getName()).isEqualTo("getFollowers");
		assertThat((Class) meta.getGetter().getReturnType()).isEqualTo(Set.class);
		assertThat(meta.getSetter().getName()).isEqualTo("setFollowers");
		assertThat((Class) meta.getSetter().getParameterTypes()[0]).isEqualTo(Set.class);

		assertThat(meta.propertyType()).isEqualTo(PropertyType.SET);
	}

	@Test
	public void should_parse_map() throws Exception
	{
		class Test
		{
			private Map<Integer, String> preferences;

			public Map<Integer, String> getPreferences()
			{
				return preferences;
			}

			public void setPreferences(Map<Integer, String> preferences)
			{
				this.preferences = preferences;
			}
		}

		PropertyMeta<String> meta = parser.parse(Test.class, Test.class.getDeclaredField("preferences"), "preferences");

		assertThat(meta).isInstanceOf(MapPropertyMeta.class);
		assertThat(meta.getPropertyName()).isEqualTo("preferences");
		assertThat(meta.getValueClass()).isEqualTo(String.class);
		assertThat((Serializer) meta.getValueSerializer()).isEqualTo(Utils.STRING_SRZ);
		assertThat(meta.propertyType()).isEqualTo(PropertyType.MAP);

		MapPropertyMeta<String> mapMeta = (MapPropertyMeta<String>) meta;
		assertThat(mapMeta.getKeyClass()).isEqualTo(Integer.class);

		assertThat(meta.getGetter().getName()).isEqualTo("getPreferences");
		assertThat((Class) meta.getGetter().getReturnType()).isEqualTo(Map.class);
		assertThat(meta.getSetter().getName()).isEqualTo("setPreferences");
		assertThat((Class) meta.getSetter().getParameterTypes()[0]).isEqualTo(Map.class);

		assertThat((Serializer) mapMeta.getKeySerializer()).isEqualTo(Utils.INT_SRZ);
	}

	@Test(expected = ValidationException.class)
	public void should_exception_when_field_not_serializable() throws Exception
	{

		class Test
		{
			private PropertyParser parser;

			public PropertyParser getParser()
			{
				return parser;
			}

			public void setParser(PropertyParser parser)
			{
				this.parser = parser;
			}
		}

		parser.parse(Test.class, Test.class.getDeclaredField("parser"), "parser");
	}

	@Test(expected = ValidationException.class)
	public void should_exception_when_value_of_list_not_serializable() throws Exception
	{

		class Test
		{
			private List<PropertyParser> parsers;

			public List<PropertyParser> getParsers()
			{
				return parsers;
			}

			public void setParsers(List<PropertyParser> parsers)
			{
				this.parsers = parsers;
			}
		}

		parser.parse(Test.class, Test.class.getDeclaredField("parsers"), "parsers");
	}

	@Test(expected = ValidationException.class)
	public void should_exception_when_value_of_set_not_serializable() throws Exception
	{

		class Test
		{
			private Set<PropertyParser> parsers;

			public Set<PropertyParser> getParsers()
			{
				return parsers;
			}

			public void setParsers(Set<PropertyParser> parsers)
			{
				this.parsers = parsers;
			}
		}

		parser.parse(Test.class, Test.class.getDeclaredField("parsers"), "parsers");
	}

	@Test(expected = ValidationException.class)
	public void should_exception_when_value_and_key_of_map_not_serializable() throws Exception
	{

		class Test
		{
			private Map<PropertyParser, PropertyParser> parsers;

			public Map<PropertyParser, PropertyParser> getParsers()
			{
				return parsers;
			}

			public void setParsers(Map<PropertyParser, PropertyParser> parsers)
			{
				this.parsers = parsers;
			}
		}

		parser.parse(Test.class, Test.class.getDeclaredField("parsers"), "parsers");
	}
}
