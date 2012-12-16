package fr.doan.achilles.entity.parser;

import static fr.doan.achilles.entity.metadata.PropertyType.JOIN_WIDE_MAP;
import static fr.doan.achilles.serializer.Utils.INT_SRZ;
import static fr.doan.achilles.serializer.Utils.LONG_SRZ;
import static fr.doan.achilles.serializer.Utils.STRING_SRZ;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.persistence.Basic;
import javax.persistence.FetchType;

import me.prettyprint.cassandra.model.ExecutingKeyspace;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import parser.entity.Bean;
import parser.entity.BeanWithJoinColumnAsEntity;
import parser.entity.BeanWithJoinColumnAsWideRow;
import parser.entity.BeanWithMultiKeyJoinColumnAsEntity;
import parser.entity.BeanWithMultiKeyJoinColumnAsWideRow;
import parser.entity.CorrectMultiKey;
import parser.entity.CorrectMultiKeyUnorderedKeys;
import parser.entity.MultiKeyNotInstantiable;
import parser.entity.MultiKeyWithNegativeOrder;
import parser.entity.MultiKeyWithNoAnnotation;
import fr.doan.achilles.annotations.Lazy;
import fr.doan.achilles.columnFamily.ColumnFamilyHelper;
import fr.doan.achilles.entity.metadata.EntityMeta;
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

		PropertyMeta<Void, String> meta = parser.parse(Test.class,
				Test.class.getDeclaredField("name"), "name");

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

		PropertyMeta<Void, String> meta = parser.parse(Test.class,
				Test.class.getDeclaredField("name"), "firstname");

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

		PropertyMeta<Void, Long> meta = parser.parse(Test.class,
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

		// parser.parse(Test.class, Test.class.getDeclaredField("tweets"), "tweets");
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

	@SuppressWarnings("unchecked")
	@Test
	public void should_parse_join_wide_map_with_entity() throws Exception
	{
		Field wide = BeanWithJoinColumnAsEntity.class.getDeclaredField("wide");
		Method idGetter = Bean.class.getDeclaredMethod("getId");
		Map<Class<?>, EntityMeta<?>> entityMetaMap = new HashMap<Class<?>, EntityMeta<?>>();
		Keyspace keyspace = mock(ExecutingKeyspace.class);
		EntityParser entityParser = new EntityParser();
		ColumnFamilyHelper columnFamilyHelper = mock(ColumnFamilyHelper.class);

		PropertyMeta<Integer, BeanWithJoinColumnAsEntity> meta = //
		parser.parseJoinColum( //
				BeanWithJoinColumnAsEntity.class, //
				wide, //
				entityMetaMap, //
				keyspace, //
				entityParser, //
				columnFamilyHelper, true);

		assertThat(meta.propertyType()).isEqualTo(JOIN_WIDE_MAP);
		assertThat(meta.getPropertyName()).isEqualTo("wide");
		assertThat(meta.isSingleKey()).isTrue();
		assertThat(meta.isInsertable()).isTrue();
		assertThat(meta.isEntityValue()).isTrue();
		assertThat(meta.getJoinColumnFamily()).isEqualTo("parser_entity_Bean");
		assertThat(meta.getIdGetter()).isEqualTo(idGetter);
		assertThat(meta.getIdClass()).isEqualTo((Class) Long.class);
		assertThat(meta.getIdSerializer()).isEqualTo((Serializer) LONG_SRZ);

		assertThat(entityMetaMap).hasSize(1);
		EntityMeta<?> entityMeta = entityMetaMap.get(Bean.class);
		assertThat(entityMeta.getColumnFamilyName()).isEqualTo("parser_entity_Bean");
		assertThat(entityMeta.getIdMeta().getValueClass()).isEqualTo((Class) Long.class);
		assertThat(entityMeta.getIdMeta().getValueSerializer()).isEqualTo((Serializer) LONG_SRZ);
		assertThat(entityMeta.getIdMeta().getGetter()).isEqualTo(idGetter);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_parse_join_wide_map_with_wide_row() throws Exception
	{
		Field wideRow = BeanWithJoinColumnAsWideRow.class.getDeclaredField("wideRow");
		Method idGetter = BeanWithJoinColumnAsWideRow.class.getDeclaredMethod("getId");
		Map<Class<?>, EntityMeta<?>> entityMetaMap = new HashMap<Class<?>, EntityMeta<?>>();
		Keyspace keyspace = mock(ExecutingKeyspace.class);
		EntityParser entityParser = new EntityParser();
		ColumnFamilyHelper columnFamilyHelper = mock(ColumnFamilyHelper.class);

		PropertyMeta<Integer, String> meta = //
		parser.parseJoinColum( //
				BeanWithJoinColumnAsWideRow.class, //
				wideRow, //
				entityMetaMap, //
				keyspace, //
				entityParser, //
				columnFamilyHelper, true);

		assertThat(meta.propertyType()).isEqualTo(JOIN_WIDE_MAP);
		assertThat(meta.getPropertyName()).isEqualTo("wideRow");
		assertThat(meta.isSingleKey()).isTrue();
		assertThat(meta.isInsertable()).isTrue();
		assertThat(meta.isEntityValue()).isFalse();
		assertThat(meta.getJoinColumnFamily()).isEqualTo("my_wide_row_cf");
		assertThat(meta.getIdGetter()).isEqualTo(idGetter);
		assertThat(meta.getIdClass()).isEqualTo((Class) Long.class);
		assertThat(meta.getIdSerializer()).isEqualTo((Serializer) LONG_SRZ);

		assertThat(entityMetaMap).hasSize(0);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_parse_join_wide_map_with_multikey_entity() throws Exception
	{
		Field wide = BeanWithMultiKeyJoinColumnAsEntity.class.getDeclaredField("wide");
		Method idGetter = Bean.class.getDeclaredMethod("getId");
		Map<Class<?>, EntityMeta<?>> entityMetaMap = new HashMap<Class<?>, EntityMeta<?>>();
		Keyspace keyspace = mock(ExecutingKeyspace.class);
		EntityParser entityParser = new EntityParser();
		ColumnFamilyHelper columnFamilyHelper = mock(ColumnFamilyHelper.class);

		PropertyMeta<Integer, BeanWithMultiKeyJoinColumnAsEntity> meta = //
		parser.parseJoinColum( //
				BeanWithMultiKeyJoinColumnAsEntity.class, //
				wide, //
				entityMetaMap, //
				keyspace, //
				entityParser, //
				columnFamilyHelper, true);

		assertThat(meta.propertyType()).isEqualTo(JOIN_WIDE_MAP);
		assertThat(meta.getPropertyName()).isEqualTo("wide");
		assertThat(meta.isSingleKey()).isFalse();
		assertThat(meta.isInsertable()).isTrue();
		assertThat(meta.isEntityValue()).isTrue();
		assertThat(meta.getJoinColumnFamily()).isEqualTo("parser_entity_Bean");
		assertThat(meta.getIdGetter()).isEqualTo(idGetter);
		assertThat(meta.getIdClass()).isEqualTo((Class) Long.class);
		assertThat(meta.getIdSerializer()).isEqualTo((Serializer) LONG_SRZ);

		assertThat(entityMetaMap).hasSize(1);
		EntityMeta<?> entityMeta = entityMetaMap.get(Bean.class);
		assertThat(entityMeta.getColumnFamilyName()).isEqualTo("parser_entity_Bean");
		assertThat(entityMeta.getIdMeta().getValueClass()).isEqualTo((Class) Long.class);
		assertThat(entityMeta.getIdMeta().getValueSerializer()).isEqualTo((Serializer) LONG_SRZ);
		assertThat(entityMeta.getIdMeta().getGetter()).isEqualTo(idGetter);

		assertThat(meta.getComponentClasses()).hasSize(2);
		assertThat(meta.getComponentClasses().get(0)).isEqualTo((Class) String.class);
		assertThat(meta.getComponentClasses().get(1)).isEqualTo((Class) int.class);

		assertThat(meta.getComponentSerializers()).hasSize(2);
		assertThat(meta.getComponentSerializers().get(0)).isEqualTo((Serializer) STRING_SRZ);
		assertThat(meta.getComponentSerializers().get(1)).isEqualTo((Serializer) INT_SRZ);

		assertThat(meta.getComponentGetters()).hasSize(2);
		assertThat(meta.getComponentGetters().get(0).getName()).isEqualTo("getName");
		assertThat(meta.getComponentGetters().get(1).getName()).isEqualTo("getRank");

		assertThat(meta.getComponentSetters()).hasSize(2);
		assertThat(meta.getComponentSetters().get(0).getName()).isEqualTo("setName");
		assertThat(meta.getComponentSetters().get(1).getName()).isEqualTo("setRank");
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_parse_join_wide_map_with_multikey_wide_row() throws Exception
	{
		Field wideRow = BeanWithMultiKeyJoinColumnAsWideRow.class.getDeclaredField("wideRow");
		Method idGetter = BeanWithMultiKeyJoinColumnAsWideRow.class.getDeclaredMethod("getId");
		Map<Class<?>, EntityMeta<?>> entityMetaMap = new HashMap<Class<?>, EntityMeta<?>>();
		Keyspace keyspace = mock(ExecutingKeyspace.class);
		EntityParser entityParser = new EntityParser();
		ColumnFamilyHelper columnFamilyHelper = mock(ColumnFamilyHelper.class);

		PropertyMeta<Integer, String> meta = //
		parser.parseJoinColum( //
				BeanWithMultiKeyJoinColumnAsWideRow.class, //
				wideRow, //
				entityMetaMap, //
				keyspace, //
				entityParser, //
				columnFamilyHelper, true);

		assertThat(meta.propertyType()).isEqualTo(JOIN_WIDE_MAP);
		assertThat(meta.getPropertyName()).isEqualTo("wideRow");
		assertThat(meta.isSingleKey()).isFalse();
		assertThat(meta.isInsertable()).isTrue();
		assertThat(meta.isEntityValue()).isFalse();
		assertThat(meta.getJoinColumnFamily()).isEqualTo("my_wide_row_cf");
		assertThat(meta.getIdGetter()).isEqualTo(idGetter);
		assertThat(meta.getIdClass()).isEqualTo((Class) Long.class);
		assertThat(meta.getIdSerializer()).isEqualTo((Serializer) LONG_SRZ);

		assertThat(entityMetaMap).hasSize(0);

		assertThat(meta.getComponentClasses()).hasSize(2);
		assertThat(meta.getComponentClasses().get(0)).isEqualTo((Class) String.class);
		assertThat(meta.getComponentClasses().get(1)).isEqualTo((Class) int.class);

		assertThat(meta.getComponentSerializers()).hasSize(2);
		assertThat(meta.getComponentSerializers().get(0)).isEqualTo((Serializer) STRING_SRZ);
		assertThat(meta.getComponentSerializers().get(1)).isEqualTo((Serializer) INT_SRZ);

		assertThat(meta.getComponentGetters()).hasSize(2);
		assertThat(meta.getComponentGetters().get(0).getName()).isEqualTo("getName");
		assertThat(meta.getComponentGetters().get(1).getName()).isEqualTo("getRank");

		assertThat(meta.getComponentSetters()).hasSize(2);
		assertThat(meta.getComponentSetters().get(0).getName()).isEqualTo("setName");
		assertThat(meta.getComponentSetters().get(1).getName()).isEqualTo("setRank");
	}
}
