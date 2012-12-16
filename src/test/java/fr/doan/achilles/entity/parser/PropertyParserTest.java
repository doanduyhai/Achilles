package fr.doan.achilles.entity.parser;

import static fr.doan.achilles.serializer.Utils.STRING_SRZ;
import static org.fest.assertions.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.persistence.Basic;
import javax.persistence.FetchType;

import me.prettyprint.hector.api.Serializer;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import parser.entity.CorrectMultiKey;
import parser.entity.CorrectMultiKeyUnorderedKeys;
import parser.entity.MultiKeyNotInstantiable;
import parser.entity.MultiKeyWithNegativeOrder;
import parser.entity.MultiKeyWithNoAnnotation;
import fr.doan.achilles.annotations.Lazy;
import fr.doan.achilles.entity.metadata.ListMeta;
import fr.doan.achilles.entity.metadata.MapMeta;
import fr.doan.achilles.entity.metadata.MultiKeyWideMapMeta;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.metadata.PropertyType;
import fr.doan.achilles.entity.metadata.SetMeta;
import fr.doan.achilles.entity.metadata.SimpleMeta;
import fr.doan.achilles.entity.metadata.WideMapMeta;
import fr.doan.achilles.entity.type.WideMap;
import fr.doan.achilles.exception.ValidationException;
import fr.doan.achilles.serializer.Utils;

@SuppressWarnings(
{
		"unused",
		"rawtypes"
})
public class PropertyParserTest
{

	@Rule
	public ExpectedException expectedEx = ExpectedException.none();

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

		PropertyMeta<String> meta = parser.parse(Test.class, Test.class.getDeclaredField("name"),
				"name");

		assertThat(meta).isInstanceOf(SimpleMeta.class);
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

		PropertyMeta<String> meta = parser.parse(Test.class, Test.class.getDeclaredField("name"),
				"firstname");

		assertThat(meta.getPropertyName()).isEqualTo("firstname");
	}

	@Test
	public void should_parse_lazy() throws Exception
	{
		class Test
		{
			@Lazy
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

		PropertyMeta<String> meta = parser.parse(Test.class,
				Test.class.getDeclaredField("friends"), "friends");

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

		PropertyMeta<String> meta = parser.parse(Test.class,
				Test.class.getDeclaredField("friends"), "friends");

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

		PropertyMeta<String> meta = parser.parse(Test.class,
				Test.class.getDeclaredField("friends"), "friends");

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

		PropertyMeta<String> meta = parser.parse(Test.class,
				Test.class.getDeclaredField("friends"), "friends");

		assertThat(meta).isInstanceOf(ListMeta.class);
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

		PropertyMeta<Long> meta = parser.parse(Test.class,
				Test.class.getDeclaredField("followers"), "followers");

		assertThat(meta).isInstanceOf(SetMeta.class);
		assertThat(meta.getPropertyName()).isEqualTo("followers");
		assertThat(meta.getValueClass()).isEqualTo(Long.class);
		assertThat((Serializer) meta.getValueSerializer()).isEqualTo(Utils.LONG_SRZ);

		assertThat(meta.getGetter().getName()).isEqualTo("getFollowers");
		assertThat((Class) meta.getGetter().getReturnType()).isEqualTo(Set.class);
		assertThat(meta.getSetter().getName()).isEqualTo("setFollowers");
		assertThat((Class) meta.getSetter().getParameterTypes()[0]).isEqualTo(Set.class);

		assertThat(meta.propertyType()).isEqualTo(PropertyType.SET);
	}

	@SuppressWarnings("unchecked")
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

		PropertyMeta<String> meta = parser.parse(Test.class,
				Test.class.getDeclaredField("preferences"), "preferences");

		assertThat(meta).isInstanceOf(MapMeta.class);
		assertThat(meta.getPropertyName()).isEqualTo("preferences");
		assertThat(meta.getValueClass()).isEqualTo(String.class);
		assertThat((Serializer) meta.getValueSerializer()).isEqualTo(Utils.STRING_SRZ);
		assertThat(meta.propertyType()).isEqualTo(PropertyType.MAP);

		MapMeta<Integer, String> mapMeta = (MapMeta<Integer, String>) meta;
		assertThat(mapMeta.getKeyClass()).isEqualTo(Integer.class);

		assertThat(meta.getGetter().getName()).isEqualTo("getPreferences");
		assertThat((Class) meta.getGetter().getReturnType()).isEqualTo(Map.class);
		assertThat(meta.getSetter().getName()).isEqualTo("setPreferences");
		assertThat((Class) meta.getSetter().getParameterTypes()[0]).isEqualTo(Map.class);

		assertThat((Serializer) mapMeta.getKeySerializer()).isEqualTo(Utils.INT_SRZ);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_parse_wide_map() throws Exception
	{
		class Test
		{
			private WideMap<UUID, String> tweets;

			public WideMap<UUID, String> getTweets()
			{
				return tweets;
			}

			public void setTweets(WideMap<UUID, String> tweets)
			{
				this.tweets = tweets;
			}
		}

		PropertyMeta<String> meta = parser.parse(Test.class, Test.class.getDeclaredField("tweets"),
				"tweets");

		assertThat(meta).isInstanceOf(WideMapMeta.class);
		assertThat(meta.getPropertyName()).isEqualTo("tweets");
		assertThat(meta.getValueClass()).isEqualTo(String.class);
		assertThat((Serializer) meta.getValueSerializer()).isEqualTo(Utils.STRING_SRZ);
		assertThat(meta.propertyType()).isEqualTo(PropertyType.WIDE_MAP);

		WideMapMeta<UUID, String> wideMapMeta = (WideMapMeta<UUID, String>) meta;
		assertThat(wideMapMeta.getKeyClass()).isEqualTo(UUID.class);

		assertThat((Serializer) wideMapMeta.getKeySerializer()).isEqualTo(Utils.UUID_SRZ);
	}

	@Test
	public void should_exception_when_invalid_wide_map_key() throws Exception
	{
		class Test
		{
			private WideMap<Void, String> tweets;

			public WideMap<Void, String> getTweets()
			{
				return tweets;
			}

			public void setTweets(WideMap<Void, String> tweets)
			{
				this.tweets = tweets;
			}
		}

		expectedEx.expect(ValidationException.class);
		expectedEx.expectMessage("The class '" + Void.class.getCanonicalName()
				+ "' is not allowed as WideMap key");

		parser.parse(Test.class, Test.class.getDeclaredField("tweets"), "tweets");
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_parse_multi_key_wide_map() throws Exception
	{

		class Test
		{
			private WideMap<CorrectMultiKey, String> tweets;

			public WideMap<CorrectMultiKey, String> getTweets()
			{
				return tweets;
			}

			public void setTweets(WideMap<CorrectMultiKey, String> tweets)
			{
				this.tweets = tweets;
			}
		}

		PropertyMeta<String> meta = parser.parse(Test.class, Test.class.getDeclaredField("tweets"),
				"tweets");

		assertThat(meta).isInstanceOf(MultiKeyWideMapMeta.class);
		assertThat(meta.getPropertyName()).isEqualTo("tweets");
		assertThat(meta.getValueClass()).isEqualTo(String.class);
		assertThat((Serializer) meta.getValueSerializer()).isEqualTo(Utils.STRING_SRZ);
		assertThat(meta.propertyType()).isEqualTo(PropertyType.WIDE_MAP);

		MultiKeyWideMapMeta<CorrectMultiKey, String> wideMapMeta = (MultiKeyWideMapMeta<CorrectMultiKey, String>) meta;

		assertThat(wideMapMeta.getKeyClass()).isEqualTo(CorrectMultiKey.class);

		assertThat(wideMapMeta.getComponentGetters()).hasSize(2);
		assertThat(wideMapMeta.getComponentGetters().get(0).getName()).isEqualTo("getName");
		assertThat(wideMapMeta.getComponentGetters().get(1).getName()).isEqualTo("getRank");

		assertThat(wideMapMeta.getComponentSetters()).hasSize(2);
		assertThat(wideMapMeta.getComponentSetters().get(0).getName()).isEqualTo("setName");
		assertThat(wideMapMeta.getComponentSetters().get(1).getName()).isEqualTo("setRank");

		assertThat(wideMapMeta.getComponentSerializers()).hasSize(2);
		assertThat((Serializer) wideMapMeta.getComponentSerializers().get(0)).isEqualTo(
				Utils.STRING_SRZ);
		assertThat((Serializer) wideMapMeta.getComponentSerializers().get(1)).isEqualTo(
				Utils.INT_SRZ);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_parse_multi_key_wide_map_unordered_keys() throws Exception
	{

		class Test
		{
			private WideMap<CorrectMultiKeyUnorderedKeys, String> tweets;

			public WideMap<CorrectMultiKeyUnorderedKeys, String> getTweets()
			{
				return tweets;
			}

			public void setTweets(WideMap<CorrectMultiKeyUnorderedKeys, String> tweets)
			{
				this.tweets = tweets;
			}
		}

		PropertyMeta<String> meta = parser.parse(Test.class, Test.class.getDeclaredField("tweets"),
				"tweets");

		assertThat(meta).isInstanceOf(MultiKeyWideMapMeta.class);
		assertThat(meta.getPropertyName()).isEqualTo("tweets");
		assertThat(meta.getValueClass()).isEqualTo(String.class);
		assertThat((Serializer) meta.getValueSerializer()).isEqualTo(Utils.STRING_SRZ);
		assertThat(meta.propertyType()).isEqualTo(PropertyType.WIDE_MAP);

		MultiKeyWideMapMeta<CorrectMultiKeyUnorderedKeys, String> wideMapMeta = (MultiKeyWideMapMeta<CorrectMultiKeyUnorderedKeys, String>) meta;

		assertThat(wideMapMeta.getKeyClass()).isEqualTo(CorrectMultiKeyUnorderedKeys.class);

		assertThat(wideMapMeta.getComponentGetters()).hasSize(2);
		assertThat(wideMapMeta.getComponentGetters().get(0).getName()).isEqualTo("getName");
		assertThat(wideMapMeta.getComponentGetters().get(1).getName()).isEqualTo("getRank");

		assertThat(wideMapMeta.getComponentSerializers()).hasSize(2);
		assertThat((Serializer) wideMapMeta.getComponentSerializers().get(0)).isEqualTo(
				Utils.STRING_SRZ);
		assertThat((Serializer) wideMapMeta.getComponentSerializers().get(1)).isEqualTo(
				Utils.INT_SRZ);
	}

	@Test
	public void should_exception_when_invalid_multi_key_negative_order() throws Exception
	{
		class Test
		{
			private WideMap<MultiKeyWithNegativeOrder, String> tweets;

			public WideMap<MultiKeyWithNegativeOrder, String> getTweets()
			{
				return tweets;
			}

			public void setTweets(WideMap<MultiKeyWithNegativeOrder, String> tweets)
			{
				this.tweets = tweets;
			}
		}

		expectedEx.expect(ValidationException.class);
		expectedEx.expectMessage("The key orders is wrong for MultiKey class '"
				+ MultiKeyWithNegativeOrder.class.getCanonicalName() + "'");

		parser.parse(Test.class, Test.class.getDeclaredField("tweets"), "tweets");

	}

	@Test
	public void should_exception_when_no_annotation_in_multi_key() throws Exception
	{
		class Test
		{
			private WideMap<MultiKeyWithNoAnnotation, String> tweets;

			public WideMap<MultiKeyWithNoAnnotation, String> getTweets()
			{
				return tweets;
			}

			public void setTweets(WideMap<MultiKeyWithNoAnnotation, String> tweets)
			{
				this.tweets = tweets;
			}
		}

		expectedEx.expect(ValidationException.class);
		expectedEx.expectMessage("No field with @Key annotation found in the class '"
				+ MultiKeyWithNoAnnotation.class.getCanonicalName() + "'");

		parser.parse(Test.class, Test.class.getDeclaredField("tweets"), "tweets");
	}

	@Test
	public void should_exception_when_multi_key_not_instantiable() throws Exception
	{
		class Test
		{
			private WideMap<MultiKeyNotInstantiable, String> tweets;

			public WideMap<MultiKeyNotInstantiable, String> getTweets()
			{
				return tweets;
			}

			public void setTweets(WideMap<MultiKeyNotInstantiable, String> tweets)
			{
				this.tweets = tweets;
			}
		}

		expectedEx.expect(ValidationException.class);
		expectedEx.expectMessage("The class '" + MultiKeyNotInstantiable.class.getCanonicalName()
				+ "' should have a public default constructor");

		parser.parse(Test.class, Test.class.getDeclaredField("tweets"), "tweets");
	}

	@Test
	public void should_parse_join_wide_map() throws Exception
	{

		parser.parse(Test.class, Test.class.getDeclaredField("tweets"), "tweets");
	}

	@Test
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

		expectedEx.expect(ValidationException.class);
		expectedEx.expectMessage("The property 'parser' should be Serializable");

		parser.parse(Test.class, Test.class.getDeclaredField("parser"), "parser");
	}

	@Test
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

		expectedEx.expect(ValidationException.class);
		expectedEx.expectMessage("The list value type of 'parsers' should be Serializable");

		parser.parse(Test.class, Test.class.getDeclaredField("parsers"), "parsers");
	}

	@Test
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

		expectedEx.expect(ValidationException.class);
		expectedEx.expectMessage("The set value type of 'parsers' should be Serializable");

		parser.parse(Test.class, Test.class.getDeclaredField("parsers"), "parsers");
	}

	@Test
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

		expectedEx.expect(ValidationException.class);
		expectedEx.expectMessage("The map value type of 'parsers' should be Serializable");

		parser.parse(Test.class, Test.class.getDeclaredField("parsers"), "parsers");
	}
}
