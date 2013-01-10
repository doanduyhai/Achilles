package fr.doan.achilles.entity.parser;

import static fr.doan.achilles.entity.metadata.PropertyType.EXTERNAL_JOIN_WIDE_MAP;
import static fr.doan.achilles.serializer.SerializerUtils.OBJECT_SRZ;
import static fr.doan.achilles.serializer.SerializerUtils.STRING_SRZ;
import static javax.persistence.CascadeType.MERGE;
import static javax.persistence.CascadeType.PERSIST;
import static javax.persistence.CascadeType.REMOVE;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.FetchType;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;

import mapping.entity.UserBean;
import me.prettyprint.cassandra.model.ExecutingKeyspace;
import me.prettyprint.hector.api.Keyspace;
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
import fr.doan.achilles.entity.metadata.ExternalWideMapProperties;
import fr.doan.achilles.entity.metadata.JoinProperties;
import fr.doan.achilles.entity.metadata.MultiKeyProperties;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.metadata.PropertyType;
import fr.doan.achilles.entity.type.WideMap;
import fr.doan.achilles.exception.AchillesException;
import fr.doan.achilles.exception.BeanMappingException;
import fr.doan.achilles.serializer.SerializerUtils;

/**
 * PropertyParserTest
 * 
 * @author DuyHai DOAN
 * 
 */
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

		PropertyMeta<Void, String> meta = parser.parse(Test.class,
				Test.class.getDeclaredField("name"), "name");

		assertThat(meta.getPropertyName()).isEqualTo("name");
		assertThat(meta.getValueClass()).isEqualTo(String.class);
		assertThat((Serializer<String>) meta.getValueSerializer()).isEqualTo(STRING_SRZ);

		assertThat(meta.getGetter().getName()).isEqualTo("getName");
		assertThat((Class) meta.getGetter().getReturnType()).isEqualTo(String.class);
		assertThat(meta.getSetter().getName()).isEqualTo("setName");
		assertThat((Class) meta.getSetter().getParameterTypes()[0]).isEqualTo(String.class);

		assertThat(meta.type()).isEqualTo(PropertyType.SIMPLE);
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

		PropertyMeta<Void, String> meta = parser.parse(Test.class,
				Test.class.getDeclaredField("name"), "firstname");

		assertThat(meta.getPropertyName()).isEqualTo("firstname");
	}

	@Test
	public void should_parse_simple_join_property() throws Exception
	{
		class Test
		{
			@ManyToOne(cascade =
			{
					PERSIST,
					MERGE
			})
			@JoinColumn
			private UserBean user;

			public UserBean getUser()
			{
				return user;
			}

			public void setUser(UserBean user)
			{
				this.user = user;
			}

		}

		PropertyMeta<Void, UserBean> meta = parser.parse(Test.class,
				Test.class.getDeclaredField("user"), "user");

		assertThat(meta.type()).isEqualTo(PropertyType.JOIN_SIMPLE);
		assertThat(meta.isSingleKey()).isTrue();
		assertThat(meta.isLazy()).isTrue();
		assertThat(meta.isJoinColumn()).isTrue();

		assertThat(meta.getPropertyName()).isEqualTo("user");
		assertThat(meta.getValueClass()).isEqualTo(UserBean.class);
		assertThat((Serializer) meta.getValueSerializer()).isEqualTo(OBJECT_SRZ);

		assertThat(meta.getGetter().getName()).isEqualTo("getUser");
		assertThat((Class) meta.getGetter().getReturnType()).isEqualTo(UserBean.class);
		assertThat(meta.getSetter().getName()).isEqualTo("setUser");
		assertThat((Class) meta.getSetter().getParameterTypes()[0]).isEqualTo(UserBean.class);

		assertThat(meta.type()).isEqualTo(PropertyType.JOIN_SIMPLE);

		JoinProperties joinProperties = meta.getJoinProperties();
		assertThat(joinProperties.getCascadeTypes()).containsExactly(PERSIST, MERGE);
	}

	@Test
	public void should_exception_when_simple_join_property_has_incorrect_annotation()
			throws Exception
	{
		class Test
		{
			@OneToOne(cascade =
			{
					PERSIST,
					MERGE
			})
			@JoinColumn
			private UserBean user;

			public UserBean getUser()
			{
				return user;
			}

			public void setUser(UserBean user)
			{
				this.user = user;
			}

		}

		expectedEx.expect(BeanMappingException.class);
		expectedEx
				.expectMessage("Incorrect annotation. Only @ManyToOne is allowed for the join property 'user'");

		PropertyMeta<Void, UserBean> meta = parser.parse(Test.class,
				Test.class.getDeclaredField("user"), "user");
	}

	@Test
	public void should_exception_when_simple_join_property_has_cascade_remove() throws Exception
	{
		class Test
		{
			@ManyToOne(cascade =
			{
					PERSIST,
					REMOVE
			})
			@JoinColumn
			private UserBean user;

			public UserBean getUser()
			{
				return user;
			}

			public void setUser(UserBean user)
			{
				this.user = user;
			}

		}

		expectedEx.expect(BeanMappingException.class);
		expectedEx.expectMessage("CascadeType.REMOVE is not supported for join columns");

		PropertyMeta<Void, UserBean> meta = parser.parse(Test.class,
				Test.class.getDeclaredField("user"), "user");
	}

	@Test
	public void should_exception_when_missing_ManyToOne_annotation_fot_simple_join_property()
			throws Exception
	{
		class Test
		{
			@JoinColumn
			private UserBean user;

			public UserBean getUser()
			{
				return user;
			}

			public void setUser(UserBean user)
			{
				this.user = user;
			}

		}

		expectedEx.expect(BeanMappingException.class);
		expectedEx.expectMessage("Missing @ManyToOne annotation for the join property 'user'");

		PropertyMeta<Void, UserBean> meta = parser.parse(Test.class,
				Test.class.getDeclaredField("user"), "user");
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

		PropertyMeta<Void, String> meta = parser.parse(Test.class,
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

		PropertyMeta<Void, String> meta = parser.parse(Test.class,
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

		PropertyMeta<Void, String> meta = parser.parse(Test.class,
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

		PropertyMeta<Void, String> meta = parser.parse(Test.class,
				Test.class.getDeclaredField("friends"), "friends");

		assertThat(meta.getPropertyName()).isEqualTo("friends");
		assertThat(meta.getValueClass()).isEqualTo(String.class);
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

		PropertyMeta<Void, Long> meta = parser.parse(Test.class,
				Test.class.getDeclaredField("followers"), "followers");

		assertThat(meta.getPropertyName()).isEqualTo("followers");
		assertThat(meta.getValueClass()).isEqualTo(Long.class);
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

		PropertyMeta<Integer, String> meta = parser.parse(Test.class,
				Test.class.getDeclaredField("preferences"), "preferences");

		assertThat(meta.getPropertyName()).isEqualTo("preferences");
		assertThat(meta.getValueClass()).isEqualTo(String.class);
		assertThat((Serializer) meta.getValueSerializer()).isEqualTo(SerializerUtils.STRING_SRZ);
		assertThat(meta.type()).isEqualTo(PropertyType.MAP);

		assertThat(meta.getKeyClass()).isEqualTo(Integer.class);

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

		PropertyMeta<UUID, String> meta = parser.parse(Test.class,
				Test.class.getDeclaredField("tweets"), "tweets");

		assertThat(meta.type()).isEqualTo(PropertyType.WIDE_MAP);
		assertThat(meta.getPropertyName()).isEqualTo("tweets");
		assertThat(meta.getValueClass()).isEqualTo(String.class);
		assertThat((Serializer) meta.getValueSerializer()).isEqualTo(SerializerUtils.STRING_SRZ);

		assertThat(meta.getKeyClass()).isEqualTo(UUID.class);

		assertThat((Serializer) meta.getKeySerializer()).isEqualTo(SerializerUtils.UUID_SRZ);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_parse_external_wide_map() throws Exception
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

		PropertyMeta<UUID, String> meta = parser.parseExternalWideMapProperty(keyspace, idMeta,
				Test.class, Test.class.getDeclaredField("tweets"), "tweets");

		assertThat(meta.type()).isEqualTo(PropertyType.EXTERNAL_WIDE_MAP);
		assertThat(meta.getPropertyName()).isEqualTo("tweets");
		assertThat(meta.getValueClass()).isEqualTo(String.class);
		assertThat((Serializer) meta.getValueSerializer()).isEqualTo(SerializerUtils.STRING_SRZ);

		assertThat(meta.getKeyClass()).isEqualTo(UUID.class);
		assertThat((Serializer) meta.getKeySerializer()).isEqualTo(SerializerUtils.UUID_SRZ);
		ExternalWideMapProperties<?> externalWideMapProperties = meta
				.getExternalWideMapProperties();
		assertThat(externalWideMapProperties.getExternalWideMapDao()).isNotNull();
		assertThat((Serializer) externalWideMapProperties.getIdSerializer()).isEqualTo(
				SerializerUtils.LONG_SRZ);
		assertThat(externalWideMapProperties.getExternalColumnFamilyName()).isEqualTo("tablename");
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

		expectedEx.expect(AchillesException.class);
		expectedEx.expectMessage("The class '" + Void.class.getCanonicalName()
				+ "' is not allowed as WideMap key");

		parser.parse(Test.class, Test.class.getDeclaredField("tweets"), "tweets");
	}

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

		PropertyMeta<CorrectMultiKey, String> meta = parser.parse(Test.class,
				Test.class.getDeclaredField("tweets"), "tweets");

		assertThat(meta.getPropertyName()).isEqualTo("tweets");
		assertThat(meta.getValueClass()).isEqualTo(String.class);
		assertThat((Serializer) meta.getValueSerializer()).isEqualTo(SerializerUtils.STRING_SRZ);
		assertThat(meta.type()).isEqualTo(PropertyType.WIDE_MAP);
		assertThat(meta.isSingleKey()).isFalse();

		assertThat(meta.getKeyClass()).isEqualTo(CorrectMultiKey.class);

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

		PropertyMeta<CorrectMultiKeyUnorderedKeys, String> meta = parser.parse(Test.class,
				Test.class.getDeclaredField("tweets"), "tweets");

		assertThat(meta.getPropertyName()).isEqualTo("tweets");
		assertThat(meta.getValueClass()).isEqualTo(String.class);
		assertThat((Serializer) meta.getValueSerializer()).isEqualTo(SerializerUtils.STRING_SRZ);
		assertThat(meta.type()).isEqualTo(PropertyType.WIDE_MAP);
		assertThat(meta.isSingleKey()).isFalse();

		assertThat(meta.getKeyClass()).isEqualTo(CorrectMultiKeyUnorderedKeys.class);

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

		expectedEx.expect(AchillesException.class);
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

		expectedEx.expect(AchillesException.class);
		expectedEx.expectMessage("The class '" + MultiKeyNotInstantiable.class.getCanonicalName()
				+ "' should have a public default constructor");

		parser.parse(Test.class, Test.class.getDeclaredField("tweets"), "tweets");
	}

	@Test
	public void should_parse_join_wide_map() throws Exception
	{
		class Test
		{
			@ManyToMany(cascade =
			{
					PERSIST,
					MERGE
			})
			@JoinColumn
			private WideMap<UUID, UserBean> users;

			public WideMap<UUID, UserBean> getUsers()
			{
				return users;
			}

			public void setUsers(WideMap<UUID, UserBean> users)
			{
				this.users = users;
			}

		}

		PropertyMeta<UUID, UserBean> meta = parser.parse(Test.class,
				Test.class.getDeclaredField("users"), "users");

		assertThat(meta.type()).isEqualTo(PropertyType.JOIN_WIDE_MAP);
		assertThat(meta.isSingleKey()).isTrue();
		assertThat(meta.isLazy()).isTrue();
		assertThat(meta.isJoinColumn()).isTrue();

		assertThat(meta.getPropertyName()).isEqualTo("users");
		assertThat(meta.getValueClass()).isEqualTo(UserBean.class);
		assertThat((Serializer) meta.getValueSerializer()).isEqualTo(OBJECT_SRZ);
		assertThat(meta.type()).isEqualTo(PropertyType.JOIN_WIDE_MAP);

		assertThat(meta.getKeyClass()).isEqualTo(UUID.class);
		assertThat((Serializer) meta.getKeySerializer()).isEqualTo(SerializerUtils.UUID_SRZ);

		JoinProperties joinProperties = meta.getJoinProperties();
		assertThat(joinProperties).isNotNull();

		assertThat(joinProperties.getCascadeTypes()).containsExactly(PERSIST, MERGE);

	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_parse_external_join_wide_map() throws Exception
	{
		class Test
		{
			@ManyToMany
			@JoinColumn(table = "tablename")
			private WideMap<Integer, UserBean> users;

			public WideMap<Integer, UserBean> getUsers()
			{
				return users;
			}
		}

		Keyspace keyspace = mock(ExecutingKeyspace.class);
		PropertyMeta<Void, Long> idMeta = mock(PropertyMeta.class);
		when(idMeta.getValueSerializer()).thenReturn(SerializerUtils.LONG_SRZ);

		PropertyMeta<Integer, UserBean> meta = parser.parseExternalJoinWideMapProperty(keyspace,
				idMeta, Test.class, Test.class.getDeclaredField("users"), "users");

		assertThat(meta.type()).isEqualTo(EXTERNAL_JOIN_WIDE_MAP);
		assertThat(meta.getPropertyName()).isEqualTo("users");
		assertThat(meta.getValueClass()).isEqualTo(UserBean.class);
		assertThat((Serializer) meta.getValueSerializer()).isEqualTo(SerializerUtils.OBJECT_SRZ);

		assertThat(meta.getKeyClass()).isEqualTo(Integer.class);
		assertThat((Serializer) meta.getKeySerializer()).isEqualTo(SerializerUtils.INT_SRZ);
		ExternalWideMapProperties<?> externalWideMapProperties = meta
				.getExternalWideMapProperties();
		assertThat(externalWideMapProperties.getExternalWideMapDao()).isNull();
		assertThat((Serializer) externalWideMapProperties.getIdSerializer()).isEqualTo(
				SerializerUtils.LONG_SRZ);
		assertThat(externalWideMapProperties.getExternalColumnFamilyName()).isEqualTo("tablename");
	}

	@Test
	public void should_exception_when_join_wide_map_has_incorrect_annotation() throws Exception
	{
		class Test
		{
			@OneToMany(cascade =
			{
					PERSIST,
					MERGE
			})
			@JoinColumn
			private WideMap<UUID, UserBean> users;

			public WideMap<UUID, UserBean> getUsers()
			{
				return users;
			}

			public void setUsers(WideMap<UUID, UserBean> users)
			{
				this.users = users;
			}
		}

		expectedEx.expect(BeanMappingException.class);
		expectedEx
				.expectMessage("Incorrect annotation. Only @ManyToMany is allowed for the join property 'users'");

		PropertyMeta<UUID, UserBean> meta = parser.parse(Test.class,
				Test.class.getDeclaredField("users"), "users");

	}

	@Test
	public void should_exception_when_missing_ManyToMany_annotation_for_join_wide_map()
			throws Exception
	{
		class Test
		{
			@JoinColumn
			private WideMap<UUID, UserBean> users;

			public WideMap<UUID, UserBean> getUsers()
			{
				return users;
			}

			public void setUsers(WideMap<UUID, UserBean> users)
			{
				this.users = users;
			}
		}

		expectedEx.expect(BeanMappingException.class);
		expectedEx.expectMessage("Missing @ManyToMany annotation for the join property 'users'");

		PropertyMeta<UUID, UserBean> meta = parser.parse(Test.class,
				Test.class.getDeclaredField("users"), "users");

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

		expectedEx.expect(AchillesException.class);
		expectedEx.expectMessage("Value of 'parser' should be Serializable");

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

		expectedEx.expect(AchillesException.class);
		expectedEx.expectMessage("List value type of 'parsers' should be Serializable");

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

		expectedEx.expect(AchillesException.class);
		expectedEx.expectMessage("Set value type of 'parsers' should be Serializable");

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

		expectedEx.expect(AchillesException.class);
		expectedEx.expectMessage("Map value type of 'parsers' should be Serializable");

		parser.parse(Test.class, Test.class.getDeclaredField("parsers"), "parsers");
	}

}
