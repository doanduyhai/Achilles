package info.archinnov.achilles.entity.parser;

import static info.archinnov.achilles.serializer.SerializerUtils.STRING_SRZ;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.annotations.Counter;
import info.archinnov.achilles.annotations.Lazy;
import info.archinnov.achilles.dao.CounterDao;
import info.archinnov.achilles.dao.GenericCompositeDao;
import info.archinnov.achilles.entity.metadata.ExternalWideMapProperties;
import info.archinnov.achilles.entity.metadata.MultiKeyProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.type.WideMap;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.exception.BeanMappingException;
import info.archinnov.achilles.serializer.SerializerUtils;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.persistence.Column;

import me.prettyprint.cassandra.model.ExecutingKeyspace;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.powermock.reflect.Whitebox;

import parser.entity.CorrectMultiKey;
import parser.entity.CorrectMultiKeyUnorderedKeys;
import parser.entity.MultiKeyNotInstantiable;
import parser.entity.MultiKeyWithNegativeOrder;
import parser.entity.MultiKeyWithNoAnnotation;
import parser.entity.UserBean;

/**
 * PropertyParserTest
 * 
 * @author DuyHai DOAN
 * 
 */
@SuppressWarnings(
{
		"unused",
		"rawtypes",
		"unchecked"
})
public class PropertyParserTest
{

	@Rule
	public ExpectedException expectedEx = ExpectedException.none();

	private final PropertyParser parser = new PropertyParser();

	private CounterDao counterDao;

	private ObjectMapper objectMapper = new ObjectMapper();

	private Map<String, PropertyMeta<?, ?>> propertyMetas = new HashMap<String, PropertyMeta<?, ?>>();
	private Map<Field, String> externalWideMaps = new HashMap<Field, String>();

	@Test
	public void should_parse_simple_property_string() throws Exception
	{
		class Test
		{
			@Column
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

		PropertyMeta<?, ?> meta = parser.parse(propertyMetas, externalWideMaps, Test.class,
				Test.class.getDeclaredField("name"), false, objectMapper, counterDao);

		assertThat(meta.getPropertyName()).isEqualTo("name");
		assertThat((Class) meta.getValueClass()).isEqualTo(String.class);
		assertThat((Serializer<String>) meta.getValueSerializer()).isEqualTo(STRING_SRZ);

		assertThat(meta.getGetter().getName()).isEqualTo("getName");
		assertThat((Class) meta.getGetter().getReturnType()).isEqualTo(String.class);
		assertThat(meta.getSetter().getName()).isEqualTo("setName");
		assertThat((Class) meta.getSetter().getParameterTypes()[0]).isEqualTo(String.class);

		assertThat(meta.type()).isEqualTo(PropertyType.SIMPLE);

		assertThat((PropertyMeta) propertyMetas.get("name")).isSameAs(meta);
	}

	@Test
	public void should_parse_simple_property_and_override_name() throws Exception
	{
		class Test
		{
			@Column(name = "firstname")
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

		PropertyMeta<?, ?> meta = parser.parse(propertyMetas, externalWideMaps, Test.class,
				Test.class.getDeclaredField("name"), false, objectMapper, counterDao);

		assertThat(meta.getPropertyName()).isEqualTo("firstname");
	}

	@Test
	public void should_parse_primitive_property() throws Exception
	{
		class Test
		{
			@Column
			private boolean active;

			public boolean isActive()
			{
				return active;
			}

			public void setActive(boolean active)
			{
				this.active = active;
			}

		}

		PropertyMeta<?, ?> meta = parser.parse(propertyMetas, externalWideMaps, Test.class,
				Test.class.getDeclaredField("active"), false, objectMapper, counterDao);

		assertThat((Class) meta.getValueClass()).isEqualTo(boolean.class);
	}

	@Test
	public void should_parse_counter_property() throws Exception
	{
		class Test
		{
			@Counter
			@Column
			private Long counter;

			public Long getCounter()
			{
				return counter;
			}

			public void setCounter(Long counter)
			{
				this.counter = counter;
			}
		}

		PropertyMeta<?, ?> meta = parser.parse(propertyMetas, externalWideMaps, Test.class,
				Test.class.getDeclaredField("counter"), false, objectMapper, counterDao);

		assertThat(meta.type()).isEqualTo(PropertyType.COUNTER);
		assertThat(meta.getCounterProperties()).isNotNull();
		assertThat(meta.getCounterProperties().getDao()).isSameAs(counterDao);
		assertThat(meta.getCounterProperties().getFqcn()).isEqualTo(Test.class.getCanonicalName());
	}

	@Test
	public void should_parse_counter_property_with_long_primitive() throws Exception
	{
		class Test
		{
			@Counter
			@Column
			private long counter;

			public long getCounter()
			{
				return counter;
			}

			public void setCounter(long counter)
			{
				this.counter = counter;
			}
		}

		PropertyMeta<?, ?> meta = parser.parse(propertyMetas, externalWideMaps, Test.class,
				Test.class.getDeclaredField("counter"), false, objectMapper, counterDao);

		assertThat(meta.type()).isEqualTo(PropertyType.COUNTER);
		assertThat(meta.getCounterProperties()).isNotNull();
		assertThat(meta.getCounterProperties().getDao()).isSameAs(counterDao);
		assertThat(meta.getCounterProperties().getFqcn()).isEqualTo(Test.class.getCanonicalName());
	}

	@Test
	public void should_exception_when_counter_type_is_not_long() throws Exception
	{
		class Test
		{
			@Counter
			@Column
			private String counter;

			public String getCounter()
			{
				return counter;
			}

			public void setCounter(String counter)
			{
				this.counter = counter;
			}
		}

		expectedEx.expect(AchillesException.class);
		expectedEx
				.expectMessage("Wrong type for the field 'counter'. Only java.lang.Long and primitive long are allowed for @Counter types");

		parser.parse(propertyMetas, externalWideMaps, Test.class,
				Test.class.getDeclaredField("counter"), false, objectMapper, counterDao);
	}

	@Test
	public void should_parse_enum_property() throws Exception
	{
		class Test
		{
			@Column
			private PropertyType type;

			public PropertyType getType()
			{
				return type;
			}

			public void setType(PropertyType type)
			{
				this.type = type;
			}
		}

		PropertyMeta<?, ?> meta = parser.parse(propertyMetas, externalWideMaps, Test.class,
				Test.class.getDeclaredField("type"), false, objectMapper, counterDao);

		assertThat((Class) meta.getValueClass()).isEqualTo(PropertyType.class);
	}

	@Test
	public void should_parse_allowed_type_property() throws Exception
	{
		class Test
		{
			@Column
			private UUID uuid;

			public UUID getUuid()
			{
				return uuid;
			}

			public void setUuid(UUID uuid)
			{
				this.uuid = uuid;
			}
		}

		PropertyMeta<?, ?> meta = parser.parse(propertyMetas, externalWideMaps, Test.class,
				Test.class.getDeclaredField("uuid"), false, objectMapper, counterDao);

		assertThat((Class) meta.getValueClass()).isEqualTo(UUID.class);
	}

	@Test
	public void should_parse_lazy() throws Exception
	{
		class Test
		{
			@Column
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

		PropertyMeta<?, ?> meta = parser.parse(propertyMetas, externalWideMaps, Test.class,
				Test.class.getDeclaredField("friends"), false, objectMapper, counterDao);

		assertThat(meta.type().isLazy()).isTrue();
	}

	@Test
	public void should_parse_eager() throws Exception
	{
		class Test
		{
			@Column
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

		PropertyMeta<?, ?> meta = parser.parse(propertyMetas, externalWideMaps, Test.class,
				Test.class.getDeclaredField("friends"), false, objectMapper, counterDao);

		assertThat(meta.type().isLazy()).isFalse();
	}

	@Test
	public void should_parse_list() throws Exception
	{
		class Test
		{
			@Column
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

		PropertyMeta<?, ?> meta = parser.parse(propertyMetas, externalWideMaps, Test.class,
				Test.class.getDeclaredField("friends"), false, objectMapper, counterDao);

		assertThat(meta.getPropertyName()).isEqualTo("friends");
		assertThat((Class) meta.getValueClass()).isEqualTo(String.class);
		assertThat((Serializer) meta.getValueSerializer()).isEqualTo(SerializerUtils.STRING_SRZ);

		assertThat(meta.getGetter().getName()).isEqualTo("getFriends");
		assertThat((Class) meta.getGetter().getReturnType()).isEqualTo(List.class);
		assertThat(meta.getSetter().getName()).isEqualTo("setFriends");
		assertThat((Class) meta.getSetter().getParameterTypes()[0]).isEqualTo(List.class);

		assertThat(meta.type()).isEqualTo(PropertyType.LIST);
	}

	@Test
	public void should_parse_set() throws Exception
	{
		class Test
		{
			@Column
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

		PropertyMeta<?, ?> meta = parser.parse(propertyMetas, externalWideMaps, Test.class,
				Test.class.getDeclaredField("followers"), false, objectMapper, counterDao);

		assertThat(meta.getPropertyName()).isEqualTo("followers");
		assertThat((Class) meta.getValueClass()).isEqualTo(Long.class);
		assertThat((Serializer) meta.getValueSerializer()).isEqualTo(SerializerUtils.LONG_SRZ);

		assertThat(meta.getGetter().getName()).isEqualTo("getFollowers");
		assertThat((Class) meta.getGetter().getReturnType()).isEqualTo(Set.class);
		assertThat(meta.getSetter().getName()).isEqualTo("setFollowers");
		assertThat((Class) meta.getSetter().getParameterTypes()[0]).isEqualTo(Set.class);

		assertThat(meta.type()).isEqualTo(PropertyType.SET);
	}

	@Test
	public void should_parse_map() throws Exception
	{
		class Test
		{
			@Column
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

		PropertyMeta<?, ?> meta = parser.parse(propertyMetas, externalWideMaps, Test.class,
				Test.class.getDeclaredField("preferences"), false, objectMapper, counterDao);

		assertThat(meta.getPropertyName()).isEqualTo("preferences");
		assertThat((Class) meta.getValueClass()).isEqualTo(String.class);
		assertThat((Serializer) meta.getValueSerializer()).isEqualTo(SerializerUtils.STRING_SRZ);
		assertThat(meta.type()).isEqualTo(PropertyType.MAP);

		assertThat((Class) meta.getKeyClass()).isEqualTo(Integer.class);

		assertThat(meta.getGetter().getName()).isEqualTo("getPreferences");
		assertThat((Class) meta.getGetter().getReturnType()).isEqualTo(Map.class);
		assertThat(meta.getSetter().getName()).isEqualTo("setPreferences");
		assertThat((Class) meta.getSetter().getParameterTypes()[0]).isEqualTo(Map.class);

		assertThat((Serializer) meta.getKeySerializer()).isEqualTo(SerializerUtils.INT_SRZ);
	}

	@Test
	public void should_parse_wide_map() throws Exception
	{
		class Test
		{
			@Column
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

		PropertyMeta<?, ?> meta = parser.parse(propertyMetas, externalWideMaps, Test.class,
				Test.class.getDeclaredField("tweets"), false, objectMapper, counterDao);

		assertThat(meta.type()).isEqualTo(PropertyType.WIDE_MAP);
		assertThat(meta.getPropertyName()).isEqualTo("tweets");
		assertThat((Class) meta.getValueClass()).isEqualTo(String.class);
		assertThat((Serializer) meta.getValueSerializer()).isEqualTo(SerializerUtils.STRING_SRZ);

		assertThat((Class) meta.getKeyClass()).isEqualTo(UUID.class);

		assertThat((Serializer) meta.getKeySerializer()).isEqualTo(SerializerUtils.UUID_SRZ);
	}

	@Test
	public void should_fill_external_widemap_hashmap() throws Exception
	{
		class Test
		{
			@Column(table = "tablename")
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

		Field tweetsField = Test.class.getDeclaredField("tweets");
		parser.parse(propertyMetas, externalWideMaps, Test.class, tweetsField, false, objectMapper,
				counterDao);

		assertThat(externalWideMaps).hasSize(1);
		assertThat(externalWideMaps.get(tweetsField)).isEqualTo("tweets");
	}

	@Test
	public void should_parse_external_widemap() throws Exception
	{
		class Test
		{
			@Column(table = "tablename")
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

		Keyspace keyspace = mock(ExecutingKeyspace.class);
		PropertyMeta<Void, Long> idMeta = mock(PropertyMeta.class);
		when(idMeta.getValueSerializer()).thenReturn(SerializerUtils.LONG_SRZ);

		PropertyMeta<?, ?> meta = parser.parseExternalWideMapProperty(keyspace, idMeta, Test.class,
				Test.class.getDeclaredField("tweets"), "tweets", objectMapper);

		assertThat(meta.type()).isEqualTo(PropertyType.EXTERNAL_WIDE_MAP);
		assertThat(meta.getPropertyName()).isEqualTo("tweets");
		assertThat((Class) meta.getValueClass()).isEqualTo(String.class);
		assertThat((Serializer) meta.getValueSerializer()).isEqualTo(SerializerUtils.STRING_SRZ);

		assertThat((Class) meta.getKeyClass()).isEqualTo(UUID.class);
		assertThat((Serializer) meta.getKeySerializer()).isEqualTo(SerializerUtils.UUID_SRZ);
		ExternalWideMapProperties<?> externalWideMapProperties = meta
				.getExternalWideMapProperties();
		assertThat(externalWideMapProperties.getExternalWideMapDao()).isNotNull();
		assertThat((Serializer) externalWideMapProperties.getIdSerializer()).isEqualTo(
				SerializerUtils.LONG_SRZ);
		assertThat(externalWideMapProperties.getExternalColumnFamilyName()).isEqualTo("tablename");
	}

	@Test
	public void should_parse_external_widemap_with_non_primitive_value() throws Exception
	{
		class Test
		{
			@Column(table = "tablename")
			private WideMap<UUID, UserBean> users;

			public WideMap<UUID, UserBean> getUsers()
			{
				return users;
			}

		}

		Keyspace keyspace = mock(ExecutingKeyspace.class);
		PropertyMeta<Void, Long> idMeta = mock(PropertyMeta.class);
		when(idMeta.getValueSerializer()).thenReturn(SerializerUtils.LONG_SRZ);

		PropertyMeta<?, ?> meta = parser.parseExternalWideMapProperty(keyspace, idMeta, Test.class,
				Test.class.getDeclaredField("users"), "users", objectMapper);

		GenericCompositeDao<?, ?> dao = meta.getExternalWideMapProperties().getExternalWideMapDao();
		assertThat(Whitebox.getInternalState(dao, "valueSerializer")).isEqualTo(STRING_SRZ);
	}

	@Test
	public void should_exception_when_invalid_wide_map_key() throws Exception
	{
		class Test
		{
			@Column
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

		expectedEx.expect(AchillesException.class);
		expectedEx.expectMessage("The class '" + Void.class.getCanonicalName()
				+ "' is not allowed as WideMap key");

		parser.parse(propertyMetas, externalWideMaps, Test.class,
				Test.class.getDeclaredField("tweets"), false, objectMapper, counterDao);
	}

	@Test
	public void should_parse_multi_key_wide_map() throws Exception
	{

		class Test
		{
			@Column
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

		PropertyMeta<?, ?> meta = parser.parse(propertyMetas, externalWideMaps, Test.class,
				Test.class.getDeclaredField("tweets"), false, objectMapper, counterDao);

		assertThat(meta.getPropertyName()).isEqualTo("tweets");
		assertThat((Class) meta.getValueClass()).isEqualTo(String.class);
		assertThat((Serializer) meta.getValueSerializer()).isEqualTo(SerializerUtils.STRING_SRZ);
		assertThat(meta.type()).isEqualTo(PropertyType.WIDE_MAP);
		assertThat(meta.isSingleKey()).isFalse();

		assertThat((Class) meta.getKeyClass()).isEqualTo(CorrectMultiKey.class);

		MultiKeyProperties multiKeyProperties = meta.getMultiKeyProperties();

		assertThat(multiKeyProperties.getComponentGetters()).hasSize(2);
		assertThat(multiKeyProperties.getComponentGetters().get(0).getName()).isEqualTo("getName");
		assertThat(multiKeyProperties.getComponentGetters().get(1).getName()).isEqualTo("getRank");

		assertThat(multiKeyProperties.getComponentSetters()).hasSize(2);
		assertThat(multiKeyProperties.getComponentSetters().get(0).getName()).isEqualTo("setName");
		assertThat(multiKeyProperties.getComponentSetters().get(1).getName()).isEqualTo("setRank");

		assertThat(multiKeyProperties.getComponentSerializers()).hasSize(2);
		assertThat((Serializer) multiKeyProperties.getComponentSerializers().get(0)).isEqualTo(
				SerializerUtils.STRING_SRZ);
		assertThat((Serializer) multiKeyProperties.getComponentSerializers().get(1)).isEqualTo(
				SerializerUtils.INT_SRZ);
	}

	@Test
	public void should_parse_multi_key_wide_map_unordered_keys() throws Exception
	{

		class Test
		{
			@Column
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

		PropertyMeta<?, ?> meta = parser.parse(propertyMetas, externalWideMaps, Test.class,
				Test.class.getDeclaredField("tweets"), false, objectMapper, counterDao);

		assertThat(meta.getPropertyName()).isEqualTo("tweets");
		assertThat((Class) meta.getValueClass()).isEqualTo(String.class);
		assertThat((Serializer) meta.getValueSerializer()).isEqualTo(SerializerUtils.STRING_SRZ);
		assertThat(meta.type()).isEqualTo(PropertyType.WIDE_MAP);
		assertThat(meta.isSingleKey()).isFalse();

		assertThat((Class) meta.getKeyClass()).isEqualTo(CorrectMultiKeyUnorderedKeys.class);

		MultiKeyProperties multiKeyProperties = meta.getMultiKeyProperties();

		assertThat(multiKeyProperties.getComponentGetters()).hasSize(2);
		assertThat(multiKeyProperties.getComponentGetters().get(0).getName()).isEqualTo("getName");
		assertThat(multiKeyProperties.getComponentGetters().get(1).getName()).isEqualTo("getRank");

		assertThat(multiKeyProperties.getComponentSerializers()).hasSize(2);
		assertThat((Serializer) multiKeyProperties.getComponentSerializers().get(0)).isEqualTo(
				SerializerUtils.STRING_SRZ);
		assertThat((Serializer) multiKeyProperties.getComponentSerializers().get(1)).isEqualTo(
				SerializerUtils.INT_SRZ);
	}

	@Test
	public void should_exception_when_invalid_multi_key_negative_order() throws Exception
	{
		class Test
		{
			@Column
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

		expectedEx.expect(AchillesException.class);
		expectedEx.expectMessage("The key orders is wrong for MultiKey class '"
				+ MultiKeyWithNegativeOrder.class.getCanonicalName() + "'");

		parser.parse(propertyMetas, externalWideMaps, Test.class,
				Test.class.getDeclaredField("tweets"), false, objectMapper, counterDao);

	}

	@Test
	public void should_exception_when_no_annotation_in_multi_key() throws Exception
	{
		class Test
		{
			@Column
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

		expectedEx.expect(AchillesException.class);
		expectedEx.expectMessage("No field with @Key annotation found in the class '"
				+ MultiKeyWithNoAnnotation.class.getCanonicalName() + "'");

		parser.parse(propertyMetas, externalWideMaps, Test.class,
				Test.class.getDeclaredField("tweets"), false, objectMapper, counterDao);
	}

	@Test
	public void should_exception_when_multi_key_not_instantiable() throws Exception
	{
		class Test
		{
			@Column
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

		expectedEx.expect(AchillesException.class);
		expectedEx.expectMessage("The class '" + MultiKeyNotInstantiable.class.getCanonicalName()
				+ "' should have a public default constructor");

		parser.parse(propertyMetas, externalWideMaps, Test.class,
				Test.class.getDeclaredField("tweets"), false, objectMapper, counterDao);
	}

	@Test
	public void should_exception_when_field_not_serializable() throws Exception
	{

		class Test
		{
			@Column
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

		expectedEx.expect(BeanMappingException.class);
		expectedEx.expectMessage("Value of 'parser' should be Serializable");

		parser.parse(propertyMetas, externalWideMaps, Test.class,
				Test.class.getDeclaredField("parser"), false, objectMapper, counterDao);
	}

	@Test
	public void should_exception_when_value_of_list_not_serializable() throws Exception
	{

		class Test
		{
			@Column
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

		expectedEx.expect(BeanMappingException.class);
		expectedEx.expectMessage("List value type of 'parsers' should be Serializable");

		parser.parse(propertyMetas, externalWideMaps, Test.class,
				Test.class.getDeclaredField("parsers"), false, objectMapper, counterDao);
	}

	@Test
	public void should_exception_when_value_of_set_not_serializable() throws Exception
	{

		class Test
		{
			@Column
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

		expectedEx.expect(BeanMappingException.class);
		expectedEx.expectMessage("Set value type of 'parsers' should be Serializable");

		parser.parse(propertyMetas, externalWideMaps, Test.class,
				Test.class.getDeclaredField("parsers"), false, objectMapper, counterDao);
	}

	@Test
	public void should_exception_when_value_and_key_of_map_not_serializable() throws Exception
	{

		class Test
		{
			@Column
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

		expectedEx.expect(BeanMappingException.class);
		expectedEx.expectMessage("Map value type of 'parsers' should be Serializable");

		parser.parse(propertyMetas, externalWideMaps, Test.class,
				Test.class.getDeclaredField("parsers"), false, objectMapper, counterDao);
	}

}
