package info.archinnov.achilles.entity.parser;

import static info.archinnov.achilles.entity.metadata.PropertyType.*;
import static info.archinnov.achilles.entity.type.ConsistencyLevel.*;
import static info.archinnov.achilles.serializer.SerializerUtils.*;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.annotations.Consistency;
import info.archinnov.achilles.annotations.Counter;
import info.archinnov.achilles.annotations.Lazy;
import info.archinnov.achilles.consistency.AchillesConfigurableConsistencyLevelPolicy;
import info.archinnov.achilles.dao.CounterDao;
import info.archinnov.achilles.dao.GenericColumnFamilyDao;
import info.archinnov.achilles.dao.GenericEntityDao;
import info.archinnov.achilles.entity.metadata.MultiKeyProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.parser.context.EntityParsingContext;
import info.archinnov.achilles.entity.parser.context.PropertyParsingContext;
import info.archinnov.achilles.entity.type.WideMap;
import info.archinnov.achilles.exception.AchillesBeanMappingException;
import info.archinnov.achilles.json.ObjectMapperFactory;
import info.archinnov.achilles.serializer.SerializerUtils;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.JoinColumn;

import mapping.entity.CompleteBean;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

import parser.entity.CorrectMultiKey;
import parser.entity.CorrectMultiKeyUnorderedKeys;
import parser.entity.MultiKeyNotInstantiable;
import parser.entity.MultiKeyWithNegativeOrder;
import parser.entity.MultiKeyWithNoAnnotation;
import testBuilders.PropertyMetaTestBuilder;

/**
 * PropertyParserTest
 * 
 * @author DuyHai DOAN
 * 
 */
@SuppressWarnings("unchecked")
@RunWith(MockitoJUnitRunner.class)
public class PropertyParserTest
{

	@Rule
	public ExpectedException expectedEx = ExpectedException.none();

	private PropertyParser parser = new PropertyParser();

	private Map<PropertyMeta<?, ?>, Class<?>> joinPropertyMetaToBeFilled = new HashMap<PropertyMeta<?, ?>, Class<?>>();
	private Map<String, GenericEntityDao<?>> entityDaosMap = new HashMap<String, GenericEntityDao<?>>();
	private Map<String, GenericColumnFamilyDao<?, ?>> columnFamilyDaosMap = new HashMap<String, GenericColumnFamilyDao<?, ?>>();
	private Map<String, HConsistencyLevel> readConsistencyMap = new HashMap<String, HConsistencyLevel>();
	private Map<String, HConsistencyLevel> writeConsistencyMap = new HashMap<String, HConsistencyLevel>();
	private EntityParsingContext entityContext;
	private AchillesConfigurableConsistencyLevelPolicy configurableCLPolicy = new AchillesConfigurableConsistencyLevelPolicy(
			ONE, ALL, readConsistencyMap, writeConsistencyMap);

	@Mock
	private Cluster cluster;

	@Mock
	private Keyspace keyspace;

	@Mock
	private ObjectMapperFactory objectMapperFactory;

	@Mock
	private CounterDao counterDao;

	@Before
	public void setUp()
	{
		joinPropertyMetaToBeFilled.clear();
		entityDaosMap.clear();
		columnFamilyDaosMap.clear();
	}

	@Test
	public void should_parse_primary_key() throws Exception
	{
		@SuppressWarnings("unused")
		class Test
		{
			@Id
			private Long id;

			public Long getId()
			{
				return id;
			}

			public void setId(Long id)
			{
				this.id = id;
			}
		}

		PropertyParsingContext context = newContext(Test.class, Test.class.getDeclaredField("id"));
		context.setPrimaryKey(true);

		PropertyMeta<Void, Long> meta = (PropertyMeta<Void, Long>) parser.parse(context);

		assertThat(meta.getPropertyName()).isEqualTo("id");
		assertThat((Class<Long>) meta.getValueClass()).isEqualTo(Long.class);
		assertThat((Serializer<Long>) meta.getValueSerializer()).isEqualTo(LONG_SRZ);
		assertThat(context.getPropertyMetas()).isEmpty();

	}

	@Test
	public void should_parse_simple_property_string() throws Exception
	{

		@SuppressWarnings("unused")
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

		PropertyParsingContext context = newContext(Test.class, Test.class.getDeclaredField("name"));

		PropertyMeta<Void, String> meta = (PropertyMeta<Void, String>) parser.parse(context);

		assertThat(meta.getPropertyName()).isEqualTo("name");
		assertThat((Class<String>) meta.getValueClass()).isEqualTo(String.class);
		assertThat((Serializer<String>) meta.getValueSerializer()).isEqualTo(STRING_SRZ);

		assertThat(meta.getGetter().getName()).isEqualTo("getName");
		assertThat((Class<String>) meta.getGetter().getReturnType()).isEqualTo(String.class);
		assertThat(meta.getSetter().getName()).isEqualTo("setName");
		assertThat((Class<String>) meta.getSetter().getParameterTypes()[0]).isEqualTo(String.class);

		assertThat(meta.type()).isEqualTo(PropertyType.SIMPLE);

		assertThat((PropertyMeta<Void, String>) context.getPropertyMetas().get("name")).isSameAs(
				meta);
	}

	@Test
	public void should_parse_simple_property_and_override_name() throws Exception
	{
		@SuppressWarnings("unused")
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
		PropertyParsingContext context = newContext(Test.class, Test.class.getDeclaredField("name"));

		PropertyMeta<?, ?> meta = parser.parse(context);

		assertThat(meta.getPropertyName()).isEqualTo("firstname");
	}

	@Test
	public void should_parse_primitive_property() throws Exception
	{
		@SuppressWarnings("unused")
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
		PropertyParsingContext context = newContext(Test.class,
				Test.class.getDeclaredField("active"));
		PropertyMeta<?, ?> meta = parser.parse(context);

		assertThat((Class<Boolean>) meta.getValueClass()).isEqualTo(boolean.class);
	}

	@Test
	public void should_parse_counter_property() throws Exception
	{
		@SuppressWarnings("unused")
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
		PropertyParsingContext context = newContext(Test.class,
				Test.class.getDeclaredField("counter"));

		PropertyMeta<Void, Long> meta = (PropertyMeta<Void, Long>) parser.parse(context);

		assertThat(meta.type()).isEqualTo(PropertyType.COUNTER);
		assertThat(meta.getCounterProperties()).isNotNull();
		assertThat(meta.getCounterProperties().getFqcn()).isEqualTo(Test.class.getCanonicalName());
		assertThat((PropertyMeta<Void, Long>) context.getCounterMetas().get(0)).isSameAs(meta);
	}

	@Test
	public void should_parse_counter_property_with_long_primitive() throws Exception
	{
		@SuppressWarnings("unused")
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
		PropertyParsingContext context = newContext(Test.class,
				Test.class.getDeclaredField("counter"));
		PropertyMeta<?, ?> meta = parser.parse(context);

		assertThat(meta.type()).isEqualTo(PropertyType.COUNTER);
		assertThat(meta.getCounterProperties()).isNotNull();
		assertThat(meta.getCounterProperties().getFqcn()).isEqualTo(Test.class.getCanonicalName());
	}

	@Test
	public void should_parse_counter_property_with_consistency_level() throws Exception
	{
		@SuppressWarnings("unused")
		class Test
		{
			@Consistency(read = ONE, write = ALL)
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
		PropertyParsingContext context = newContext(Test.class,
				Test.class.getDeclaredField("counter"));
		PropertyMeta<?, ?> meta = parser.parse(context);

		assertThat(meta.type()).isEqualTo(PropertyType.COUNTER);
		assertThat(meta.getReadConsistencyLevel()).isEqualTo(ONE);
		assertThat(meta.getWriteConsistencyLevel()).isEqualTo(ALL);
	}

	@Test
	public void should_exception_when_counter_type_is_not_long() throws Exception
	{
		@SuppressWarnings("unused")
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

		expectedEx.expect(AchillesBeanMappingException.class);
		expectedEx
				.expectMessage("Wrong counter type for the field 'counter'. Only java.lang.Long and primitive long are allowed for @Counter types");
		PropertyParsingContext context = newContext(Test.class,
				Test.class.getDeclaredField("counter"));
		parser.parse(context);
	}

	@Test
	public void should_exception_when_counter_consistency_is_any_for_read() throws Exception
	{
		@SuppressWarnings("unused")
		class Test
		{
			@Consistency(read = ANY, write = ALL)
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

		expectedEx.expect(AchillesBeanMappingException.class);
		expectedEx
				.expectMessage("Counter field 'counter' of entity 'null' cannot have ANY as read/write consistency level. All consistency levels except ANY are allowed");
		PropertyParsingContext context = newContext(Test.class,
				Test.class.getDeclaredField("counter"));
		parser.parse(context);
	}

	@Test
	public void should_exception_when_counter_consistency_is_any_for_write() throws Exception
	{
		@SuppressWarnings("unused")
		class Test
		{
			@Consistency(read = ONE, write = ANY)
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

		expectedEx.expect(AchillesBeanMappingException.class);
		expectedEx
				.expectMessage("Counter field 'counter' of entity 'null' cannot have ANY as read/write consistency level. All consistency levels except ANY are allowed");
		PropertyParsingContext context = newContext(Test.class,
				Test.class.getDeclaredField("counter"));
		parser.parse(context);
	}

	@Test
	public void should_parse_enum_property() throws Exception
	{
		@SuppressWarnings("unused")
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
		PropertyParsingContext context = newContext(Test.class, Test.class.getDeclaredField("type"));
		PropertyMeta<?, ?> meta = parser.parse(context);

		assertThat((Class<PropertyType>) meta.getValueClass()).isEqualTo(PropertyType.class);
	}

	@Test
	public void should_parse_allowed_type_property() throws Exception
	{
		@SuppressWarnings("unused")
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
		PropertyParsingContext context = newContext(Test.class, Test.class.getDeclaredField("uuid"));

		PropertyMeta<?, ?> meta = parser.parse(context);

		assertThat((Class<UUID>) meta.getValueClass()).isEqualTo(UUID.class);
	}

	@Test
	public void should_parse_lazy() throws Exception
	{
		@SuppressWarnings("unused")
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
		PropertyParsingContext context = newContext(Test.class,
				Test.class.getDeclaredField("friends"));
		PropertyMeta<?, ?> meta = parser.parse(context);
		assertThat(meta.type().isLazy()).isTrue();
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void should_parse_list() throws Exception
	{
		@SuppressWarnings("unused")
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
		PropertyParsingContext context = newContext(Test.class,
				Test.class.getDeclaredField("friends"));
		PropertyMeta<?, ?> meta = parser.parse(context);

		assertThat(meta.getPropertyName()).isEqualTo("friends");
		assertThat((Class<String>) meta.getValueClass()).isEqualTo(String.class);
		assertThat((Serializer<String>) meta.getValueSerializer()).isEqualTo(
				SerializerUtils.STRING_SRZ);

		assertThat(meta.getGetter().getName()).isEqualTo("getFriends");
		assertThat((Class<List>) meta.getGetter().getReturnType()).isEqualTo(List.class);
		assertThat(meta.getSetter().getName()).isEqualTo("setFriends");
		assertThat((Class<List>) meta.getSetter().getParameterTypes()[0]).isEqualTo(List.class);

		assertThat(meta.type()).isEqualTo(PropertyType.LIST);
		assertThat(meta.isLazy()).isFalse();
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void should_parse_set() throws Exception
	{
		@SuppressWarnings("unused")
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
		PropertyParsingContext context = newContext(Test.class,
				Test.class.getDeclaredField("followers"));
		PropertyMeta<?, ?> meta = parser.parse(context);

		assertThat(meta.getPropertyName()).isEqualTo("followers");
		assertThat((Class<Long>) meta.getValueClass()).isEqualTo(Long.class);
		assertThat((Serializer<Long>) meta.getValueSerializer())
				.isEqualTo(SerializerUtils.LONG_SRZ);

		assertThat(meta.getGetter().getName()).isEqualTo("getFollowers");
		assertThat((Class<Set>) meta.getGetter().getReturnType()).isEqualTo(Set.class);
		assertThat(meta.getSetter().getName()).isEqualTo("setFollowers");
		assertThat((Class<Set>) meta.getSetter().getParameterTypes()[0]).isEqualTo(Set.class);

		assertThat(meta.type()).isEqualTo(PropertyType.SET);
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void should_parse_map() throws Exception
	{
		@SuppressWarnings("unused")
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
		PropertyParsingContext context = newContext(Test.class,
				Test.class.getDeclaredField("preferences"));
		PropertyMeta<?, ?> meta = parser.parse(context);

		assertThat(meta.getPropertyName()).isEqualTo("preferences");
		assertThat((Class<String>) meta.getValueClass()).isEqualTo(String.class);
		assertThat((Serializer<String>) meta.getValueSerializer()).isEqualTo(
				SerializerUtils.STRING_SRZ);
		assertThat(meta.type()).isEqualTo(PropertyType.MAP);

		assertThat((Class<Integer>) meta.getKeyClass()).isEqualTo(Integer.class);

		assertThat(meta.getGetter().getName()).isEqualTo("getPreferences");
		assertThat((Class<Map>) meta.getGetter().getReturnType()).isEqualTo(Map.class);
		assertThat(meta.getSetter().getName()).isEqualTo("setPreferences");
		assertThat((Class<Map>) meta.getSetter().getParameterTypes()[0]).isEqualTo(Map.class);

		assertThat((Serializer<Integer>) meta.getKeySerializer())
				.isEqualTo(SerializerUtils.INT_SRZ);
	}

	@Test
	public void should_parse_wide_map() throws Exception
	{
		@SuppressWarnings("unused")
		class Test
		{
			@Column(table = "xxx")
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
		PropertyParsingContext context = newContext(Test.class,
				Test.class.getDeclaredField("tweets"));
		PropertyMeta<?, ?> meta = parser.parse(context);

		assertThat(meta.type()).isEqualTo(PropertyType.WIDE_MAP);
		assertThat(meta.getExternalCFName()).isEqualTo("xxx");
		assertThat(meta.getPropertyName()).isEqualTo("tweets");
		assertThat((Class<String>) meta.getValueClass()).isEqualTo(String.class);
		assertThat((Serializer<String>) meta.getValueSerializer()).isEqualTo(
				SerializerUtils.STRING_SRZ);

		assertThat((Class<UUID>) meta.getKeyClass()).isEqualTo(UUID.class);

		assertThat((Serializer<UUID>) meta.getKeySerializer()).isEqualTo(SerializerUtils.UUID_SRZ);
	}

	@Test
	public void should_exception_when_no_external_cf_defined_for_widemap() throws Exception
	{
		@SuppressWarnings("unused")
		class Test
		{
			@Column
			private WideMap<UUID, String> wideMap;

			public WideMap<UUID, String> getWideMap()
			{
				return wideMap;
			}
		}

		PropertyParsingContext context = newContext(Test.class,
				Test.class.getDeclaredField("wideMap"));

		expectedEx.expect(AchillesBeanMappingException.class);
		expectedEx
				.expectMessage("External Column Family should be defined for WideMap property 'wideMap' of entity '"
						+ Test.class.getCanonicalName()
						+ "'. Did you forget to add 'table' attribute to @Column/@JoinColumn annotation ?");
		parser.parse(context);
	}

	@Test
	public void should_parse_counter_widemap() throws Exception
	{
		@SuppressWarnings("unused")
		class Test
		{
			@Counter
			@Column(table = "counter_xxx")
			private WideMap<UUID, Long> counters;

			public WideMap<UUID, Long> getCounters()
			{
				return counters;
			}

			public void setCounters(WideMap<UUID, Long> counters)
			{
				this.counters = counters;
			}
		}
		PropertyParsingContext context = newContext(Test.class,
				Test.class.getDeclaredField("counters"));
		PropertyMeta<UUID, Long> meta = (PropertyMeta<UUID, Long>) parser.parse(context);

		assertThat(meta.type()).isEqualTo(WIDE_MAP_COUNTER);
		assertThat(meta.getPropertyName()).isEqualTo("counters");
		assertThat(meta.getExternalCFName()).isEqualTo("counter_xxx");
		assertThat((Class<Long>) meta.getValueClass()).isEqualTo(Long.class);
		assertThat((Serializer<Long>) meta.getValueSerializer()).isEqualTo(LONG_SRZ);
		assertThat((Class<UUID>) meta.getKeyClass()).isEqualTo(UUID.class);
		assertThat((Serializer<UUID>) meta.getKeySerializer()).isEqualTo(UUID_SRZ);

		assertThat((PropertyMeta<UUID, Long>) context.getCounterMetas().get(0)).isSameAs(meta);
	}

	@Test
	public void should_exception_when_no_external_cf_defined_for_counter_widemap() throws Exception
	{
		@SuppressWarnings("unused")
		class Test
		{
			@Counter
			@Column
			private WideMap<UUID, Long> counterWideMap;

			public WideMap<UUID, Long> getCounterWideMap()
			{
				return counterWideMap;
			}
		}

		PropertyParsingContext context = newContext(Test.class,
				Test.class.getDeclaredField("counterWideMap"));

		expectedEx.expect(AchillesBeanMappingException.class);
		expectedEx
				.expectMessage("External Column Family should be defined for WideMap property 'counterWideMap' of entity '"
						+ Test.class.getCanonicalName()
						+ "'. Did you forget to add 'table' attribute to @Column/@JoinColumn annotation ?");
		parser.parse(context);
	}

	@Test
	public void should_exception_when_cf_direct_mapping_has_external_wide_map() throws Exception
	{
		@SuppressWarnings("unused")
		class Test
		{
			@Column(table = "external")
			private WideMap<UUID, Long> external;

			public WideMap<UUID, Long> getExternal()
			{
				return external;
			}

			public void setExternal(WideMap<UUID, Long> external)
			{
				this.external = external;
			}
		}

		PropertyParsingContext context = newContext(Test.class,
				Test.class.getDeclaredField("external"));
		entityContext.setColumnFamilyDirectMapping(true);

		expectedEx.expect(AchillesBeanMappingException.class);
		expectedEx
				.expectMessage("Error for field 'external' of entity 'null'. Direct Column Family mapping cannot have external WideMap. It does not make sense");

		parser.parse(context);
	}

	@Test
	public void should_fill_widemap_hashmap() throws Exception
	{
		@SuppressWarnings("unused")
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
		PropertyParsingContext context = newContext(Test.class,
				Test.class.getDeclaredField("tweets"));
		parser.parse(context);

		PropertyMeta<?, ?> propertyMeta = context.getWideMaps().keySet().iterator().next();
		assertThat(context.getJoinWideMaps()).isEmpty();
		assertThat(propertyMeta.type()).isEqualTo(PropertyType.WIDE_MAP);
		assertThat(propertyMeta.getPropertyName()).isEqualTo("tweets");
	}

	@Test
	public void should_fill_join_widemap_hashmap() throws Exception
	{
		@SuppressWarnings("unused")
		class Test
		{
			@JoinColumn(table = "tablename")
			private WideMap<UUID, CompleteBean> beans;

			public WideMap<UUID, CompleteBean> getBeans()
			{
				return beans;
			}

		}
		PropertyParsingContext context = newContext(Test.class,
				Test.class.getDeclaredField("beans"));
		context.setJoinColumn(true);

		parser.parse(context);

		PropertyMeta<?, ?> propertyMeta = context.getJoinWideMaps().keySet().iterator().next();
		assertThat(context.getWideMaps()).isEmpty();
		assertThat(propertyMeta.type()).isEqualTo(PropertyType.WIDE_MAP);
		assertThat(propertyMeta.getPropertyName()).isEqualTo("beans");
	}

	@Test
	public void should_fill_widemap_hashmap_when_direct_cf_mapping() throws Exception
	{
		@SuppressWarnings("unused")
		class Test
		{
			@Column
			private WideMap<UUID, String> tweets;

			public WideMap<UUID, String> getTweets()
			{
				return tweets;
			}

		}
		PropertyParsingContext context = newContext(Test.class,
				Test.class.getDeclaredField("tweets"));

		entityContext.setColumnFamilyDirectMapping(true);

		parser.parse(context);

		Entry<PropertyMeta<?, ?>, String> entry = context.getWideMaps().entrySet().iterator()
				.next();

		PropertyMeta<?, ?> propertyMeta = entry.getKey();
		assertThat(propertyMeta.type()).isEqualTo(PropertyType.WIDE_MAP);
		assertThat(propertyMeta.getPropertyName()).isEqualTo("tweets");
		assertThat(entry.getValue()).isNull();
	}

	@Test
	public void should_set_widemap_consistency_level() throws Exception
	{
		@SuppressWarnings("unused")
		class Test
		{
			@Column(table = "tablename")
			@Consistency(read = QUORUM, write = ALL)
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
		PropertyParsingContext context = newContext(Test.class,
				Test.class.getDeclaredField("tweets"));

		PropertyMeta<?, ?> propertyMeta = parser.parse(context);
		assertThat(propertyMeta.getReadConsistencyLevel()).isEqualTo(QUORUM);
		assertThat(propertyMeta.getWriteConsistencyLevel()).isEqualTo(ALL);
	}

	@Test
	public void should_parse_widemap() throws Exception
	{
		PropertyMeta<Void, Long> idMeta = PropertyMetaTestBuilder.valueClass(Long.class).build();
		PropertyMeta<Long, UUID> propertyMeta = PropertyMetaTestBuilder//
				.noClass(Long.class, UUID.class) //
				.type(WIDE_MAP) //
				.build();

		initEntityParsingContext();

		parser.fillWideMap(entityContext, idMeta, propertyMeta, "externalTableName");

		assertThat((Serializer<Long>) propertyMeta.getIdSerializer()).isEqualTo(LONG_SRZ);
		GenericColumnFamilyDao<?, ?> externalWideMapDao = entityContext.getColumnFamilyDaosMap()
				.get("externalTableName");
		assertThat(externalWideMapDao.getColumnFamily()).isEqualTo("externalTableName");
		assertThat(Whitebox.getInternalState(externalWideMapDao, "valueSerializer")).isEqualTo(
				UUID_SRZ);
	}

	@Test
	public void should_parse_widemap_with_non_primitive_value() throws Exception
	{

		PropertyMeta<Void, Long> idMeta = PropertyMetaTestBuilder.valueClass(Long.class).build();

		PropertyMeta<Long, CompleteBean> propertyMeta = PropertyMetaTestBuilder//
				.noClass(Long.class, CompleteBean.class)//
				.type(WIDE_MAP) //
				.build();

		initEntityParsingContext();

		parser.fillWideMap(entityContext, idMeta, propertyMeta, "externalTableName");

		GenericColumnFamilyDao<?, ?> externalWideMapDao = entityContext.getColumnFamilyDaosMap()
				.get("externalTableName");
		assertThat(Whitebox.getInternalState(externalWideMapDao, "valueSerializer")).isEqualTo(
				STRING_SRZ);
	}

	@Test
	public void should_exception_when_invalid_wide_map_key() throws Exception
	{
		@SuppressWarnings("unused")
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

		expectedEx.expect(AchillesBeanMappingException.class);
		expectedEx.expectMessage("The class '" + Void.class.getCanonicalName()
				+ "' is not allowed as WideMap key");
		PropertyParsingContext context = newContext(Test.class,
				Test.class.getDeclaredField("tweets"));

		parser.parse(context);
	}

	@Test
	public void should_parse_multi_key_wide_map() throws Exception
	{
		@SuppressWarnings("unused")
		class Test
		{
			@Column(table = "tweets_xxx")
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

		PropertyParsingContext context = newContext(Test.class,
				Test.class.getDeclaredField("tweets"));

		PropertyMeta<?, ?> meta = parser.parse(context);

		assertThat(meta.getPropertyName()).isEqualTo("tweets");
		assertThat((Class<String>) meta.getValueClass()).isEqualTo(String.class);
		assertThat((Serializer<String>) meta.getValueSerializer()).isEqualTo(
				SerializerUtils.STRING_SRZ);
		assertThat(meta.type()).isEqualTo(PropertyType.WIDE_MAP);
		assertThat(meta.isSingleKey()).isFalse();

		assertThat((Class<CorrectMultiKey>) meta.getKeyClass()).isEqualTo(CorrectMultiKey.class);

		MultiKeyProperties multiKeyProperties = meta.getMultiKeyProperties();

		assertThat(multiKeyProperties.getComponentGetters()).hasSize(2);
		assertThat(multiKeyProperties.getComponentGetters().get(0).getName()).isEqualTo("getName");
		assertThat(multiKeyProperties.getComponentGetters().get(1).getName()).isEqualTo("getRank");

		assertThat(multiKeyProperties.getComponentSetters()).hasSize(2);
		assertThat(multiKeyProperties.getComponentSetters().get(0).getName()).isEqualTo("setName");
		assertThat(multiKeyProperties.getComponentSetters().get(1).getName()).isEqualTo("setRank");

		assertThat(multiKeyProperties.getComponentSerializers()).hasSize(2);
		assertThat((Serializer<String>) multiKeyProperties.getComponentSerializers().get(0))
				.isEqualTo(SerializerUtils.STRING_SRZ);
		assertThat((Serializer<Integer>) multiKeyProperties.getComponentSerializers().get(1))
				.isEqualTo(SerializerUtils.INT_SRZ);
	}

	@Test
	public void should_parse_multi_key_wide_map_unordered_keys() throws Exception
	{
		@SuppressWarnings("unused")
		class Test
		{
			@Column(table = "tweets_xxx")
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
		PropertyParsingContext context = newContext(Test.class,
				Test.class.getDeclaredField("tweets"));

		PropertyMeta<?, ?> meta = parser.parse(context);

		assertThat(meta.getPropertyName()).isEqualTo("tweets");
		assertThat((Class<String>) meta.getValueClass()).isEqualTo(String.class);
		assertThat((Serializer<String>) meta.getValueSerializer()).isEqualTo(
				SerializerUtils.STRING_SRZ);
		assertThat(meta.type()).isEqualTo(PropertyType.WIDE_MAP);
		assertThat(meta.isSingleKey()).isFalse();

		assertThat((Class<CorrectMultiKeyUnorderedKeys>) meta.getKeyClass()).isEqualTo(
				CorrectMultiKeyUnorderedKeys.class);

		MultiKeyProperties multiKeyProperties = meta.getMultiKeyProperties();

		assertThat(multiKeyProperties.getComponentGetters()).hasSize(2);
		assertThat(multiKeyProperties.getComponentGetters().get(0).getName()).isEqualTo("getName");
		assertThat(multiKeyProperties.getComponentGetters().get(1).getName()).isEqualTo("getRank");

		assertThat(multiKeyProperties.getComponentSerializers()).hasSize(2);
		assertThat((Serializer<String>) multiKeyProperties.getComponentSerializers().get(0))
				.isEqualTo(SerializerUtils.STRING_SRZ);
		assertThat((Serializer<Integer>) multiKeyProperties.getComponentSerializers().get(1))
				.isEqualTo(SerializerUtils.INT_SRZ);
	}

	@Test
	public void should_exception_when_invalid_multi_key_negative_order() throws Exception
	{
		@SuppressWarnings("unused")
		class Test
		{
			@Column
			private WideMap<MultiKeyWithNegativeOrder, String> tweets;

			public WideMap<MultiKeyWithNegativeOrder, String> getTweets()
			{
				return tweets;
			}
		}

		expectedEx.expect(AchillesBeanMappingException.class);
		expectedEx.expectMessage("The key orders is wrong for MultiKey class '"
				+ MultiKeyWithNegativeOrder.class.getCanonicalName() + "'");
		PropertyParsingContext context = newContext(Test.class,
				Test.class.getDeclaredField("tweets"));

		parser.parse(context);

	}

	@Test
	public void should_exception_when_no_annotation_in_multi_key() throws Exception
	{
		@SuppressWarnings("unused")
		class Test
		{
			@Column
			private WideMap<MultiKeyWithNoAnnotation, String> tweets;

			public WideMap<MultiKeyWithNoAnnotation, String> getTweets()
			{
				return tweets;
			}
		}

		expectedEx.expect(AchillesBeanMappingException.class);
		expectedEx.expectMessage("No field with @Key annotation found in the class '"
				+ MultiKeyWithNoAnnotation.class.getCanonicalName() + "'");

		PropertyParsingContext context = newContext(Test.class,
				Test.class.getDeclaredField("tweets"));

		parser.parse(context);
	}

	@Test
	public void should_exception_when_multi_key_not_instantiable() throws Exception
	{
		@SuppressWarnings("unused")
		class Test
		{
			@Column
			private WideMap<MultiKeyNotInstantiable, String> tweets;

			public WideMap<MultiKeyNotInstantiable, String> getTweets()
			{
				return tweets;
			}
		}

		expectedEx.expect(AchillesBeanMappingException.class);
		expectedEx.expectMessage("The class '" + MultiKeyNotInstantiable.class.getCanonicalName()
				+ "' should have a public default constructor");

		PropertyParsingContext context = newContext(Test.class,
				Test.class.getDeclaredField("tweets"));

		parser.parse(context);
	}

	@Test
	public void should_exception_when_field_not_serializable() throws Exception
	{
		@SuppressWarnings("unused")
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

		expectedEx.expect(AchillesBeanMappingException.class);
		expectedEx.expectMessage("Value of 'parser' should be Serializable");

		PropertyParsingContext context = newContext(Test.class,
				Test.class.getDeclaredField("parser"));

		parser.parse(context);
	}

	@Test
	public void should_exception_when_value_of_list_not_serializable() throws Exception
	{
		@SuppressWarnings("unused")
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

		expectedEx.expect(AchillesBeanMappingException.class);
		expectedEx.expectMessage("List value type of 'parsers' should be Serializable");

		PropertyParsingContext context = newContext(Test.class,
				Test.class.getDeclaredField("parsers"));

		parser.parse(context);
	}

	@Test
	public void should_exception_when_value_of_set_not_serializable() throws Exception
	{
		@SuppressWarnings("unused")
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

		expectedEx.expect(AchillesBeanMappingException.class);
		expectedEx.expectMessage("Set value type of 'parsers' should be Serializable");

		PropertyParsingContext context = newContext(Test.class,
				Test.class.getDeclaredField("parsers"));

		parser.parse(context);
	}

	@Test
	public void should_exception_when_value_and_key_of_map_not_serializable() throws Exception
	{
		@SuppressWarnings("unused")
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

		expectedEx.expect(AchillesBeanMappingException.class);
		expectedEx.expectMessage("Map value type of 'parsers' should be Serializable");

		PropertyParsingContext context = newContext(Test.class,
				Test.class.getDeclaredField("parsers"));

		parser.parse(context);
	}

	private <T> PropertyParsingContext newContext(Class<T> entityClass, Field field)
	{
		entityContext = new EntityParsingContext( //
				joinPropertyMetaToBeFilled, //
				entityDaosMap, //
				columnFamilyDaosMap, //
				configurableCLPolicy, //
				cluster, keyspace, //
				objectMapperFactory, entityClass);

		return entityContext.newPropertyContext(field);
	}

	private void initEntityParsingContext()
	{
		entityContext = new EntityParsingContext( //
				joinPropertyMetaToBeFilled, //
				entityDaosMap, //
				columnFamilyDaosMap, //
				configurableCLPolicy, //
				cluster, keyspace, //
				objectMapperFactory, CompleteBean.class);
	}
}
