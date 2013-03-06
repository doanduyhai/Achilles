package info.archinnov.achilles.entity.parser;

import static info.archinnov.achilles.serializer.SerializerUtils.LONG_SRZ;
import static javax.persistence.CascadeType.MERGE;
import static javax.persistence.CascadeType.PERSIST;
import static javax.persistence.CascadeType.REFRESH;
import static javax.persistence.CascadeType.REMOVE;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.annotations.ColumnFamily;
import info.archinnov.achilles.dao.CounterDao;
import info.archinnov.achilles.entity.manager.ThriftEntityManagerFactoryImpl;
import info.archinnov.achilles.entity.metadata.ExternalWideMapProperties;
import info.archinnov.achilles.entity.metadata.JoinProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.type.WideMap;
import info.archinnov.achilles.exception.BeanMappingException;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.persistence.JoinColumn;
import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;

import me.prettyprint.cassandra.model.ExecutingKeyspace;
import me.prettyprint.hector.api.Keyspace;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

import parser.entity.UserBean;

/**
 * JoinPropertyParserTest
 * 
 * @author DuyHai DOAN
 * 
 */
@SuppressWarnings("unused")
public class JoinPropertyParserTest
{
	@Rule
	public ExpectedException expectedEx = ExpectedException.none();

	@Mock
	private CounterDao counterDao;

	private JoinPropertyParser parser = new JoinPropertyParser();

	private ObjectMapper objectMapper = new ObjectMapper();

	private Map<String, PropertyMeta<?, ?>> propertyMetas = new HashMap<String, PropertyMeta<?, ?>>();
	private Map<PropertyMeta<?, ?>, String> joinExternalWideMaps = new HashMap<PropertyMeta<?, ?>, String>();
	private Map<PropertyMeta<?, ?>, Class<?>> joinPropertyMetaToBeFilled = new HashMap<PropertyMeta<?, ?>, Class<?>>();

	@Before
	public void setUp()
	{
		EntityParser.joinExternalWideMapTL.set(joinExternalWideMaps);
		EntityParser.externalWideMapTL.set(new HashMap<PropertyMeta<?, ?>, String>());
		EntityParser.propertyMetasTL.set(propertyMetas);
		EntityParser.objectMapperTL.set(objectMapper);
		ThriftEntityManagerFactoryImpl.counterDaoTL.set(counterDao);
		ThriftEntityManagerFactoryImpl.joinPropertyMetaToBeFilledTL.set(joinPropertyMetaToBeFilled);
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void should_parse_join_simple_property() throws Exception
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
		EntityParser.entityClassTL.set(Test.class);
		PropertyMeta<?, ?> meta = parser.parseJoin(Test.class.getDeclaredField("user"));

		assertThat(meta.type()).isEqualTo(PropertyType.JOIN_SIMPLE);
		JoinProperties joinProperties = meta.getJoinProperties();
		assertThat(joinProperties.getCascadeTypes()).containsExactly(PERSIST, MERGE);

		assertThat((PropertyMeta) propertyMetas.get("user")).isSameAs(meta);
		assertThat(joinExternalWideMaps).isEmpty();
		assertThat((Class) joinPropertyMetaToBeFilled.get(meta)).isEqualTo(UserBean.class);
	}

	@Test
	public void should_parse_join_property_no_cascade() throws Exception
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
		EntityParser.entityClassTL.set(Test.class);
		PropertyMeta<?, ?> meta = parser.parseJoin(Test.class.getDeclaredField("user"));

		JoinProperties joinProperties = meta.getJoinProperties();
		assertThat(joinProperties.getCascadeTypes()).isEmpty();
		assertThat(joinExternalWideMaps).isEmpty();
	}

	@Test
	public void should_exception_when_join_simple_property_has_cascade_remove() throws Exception
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
		EntityParser.entityClassTL.set(Test.class);
		parser.parseJoin(Test.class.getDeclaredField("user"));
	}

	@Test
	public void should_parse_join_list_property() throws Exception
	{
		class Test
		{
			@OneToMany(cascade =
			{
					PERSIST,
					MERGE
			})
			@JoinColumn
			private List<UserBean> users;

			public List<UserBean> getUsers()
			{
				return users;
			}

			public void setUsers(List<UserBean> users)
			{
				this.users = users;
			}
		}
		EntityParser.entityClassTL.set(Test.class);
		PropertyMeta<?, ?> meta = parser.parseJoin(Test.class.getDeclaredField("users"));

		assertThat(meta.type()).isEqualTo(PropertyType.JOIN_LIST);
		JoinProperties joinProperties = meta.getJoinProperties();
		assertThat(joinProperties.getCascadeTypes()).containsExactly(PERSIST, MERGE);
		assertThat(joinExternalWideMaps).isEmpty();
	}

	@Test
	public void should_parse_join_set_property() throws Exception
	{
		class Test
		{
			@ManyToMany(cascade =
			{
					PERSIST,
					MERGE
			})
			@JoinColumn
			private Set<UserBean> users;

			public Set<UserBean> getUsers()
			{
				return users;
			}

			public void setUsers(Set<UserBean> users)
			{
				this.users = users;
			}
		}
		EntityParser.entityClassTL.set(Test.class);
		PropertyMeta<?, ?> meta = parser.parseJoin(Test.class.getDeclaredField("users"));

		assertThat(meta.type()).isEqualTo(PropertyType.JOIN_SET);
		JoinProperties joinProperties = meta.getJoinProperties();
		assertThat(joinProperties.getCascadeTypes()).containsExactly(PERSIST, MERGE);
		assertThat(joinExternalWideMaps).isEmpty();
	}

	@Test
	public void should_parse_join_map_property() throws Exception
	{
		class Test
		{
			@ManyToOne(cascade =
			{
					PERSIST,
					REFRESH
			})
			@JoinColumn
			private Map<Integer, UserBean> users;

			public Map<Integer, UserBean> getUsers()
			{
				return users;
			}

			public void setUsers(Map<Integer, UserBean> users)
			{
				this.users = users;
			}
		}
		EntityParser.entityClassTL.set(Test.class);
		PropertyMeta<?, ?> meta = parser.parseJoin(Test.class.getDeclaredField("users"));

		assertThat(meta.type()).isEqualTo(PropertyType.JOIN_MAP);
		JoinProperties joinProperties = meta.getJoinProperties();
		assertThat(joinProperties.getCascadeTypes()).containsExactly(PERSIST, REFRESH);
		assertThat(joinExternalWideMaps).isEmpty();
	}

	@Test
	public void should_parse_join_widemap() throws Exception
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
		EntityParser.entityClassTL.set(Test.class);
		PropertyMeta<?, ?> meta = parser.parseJoin(Test.class.getDeclaredField("users"));

		assertThat(meta.type()).isEqualTo(PropertyType.JOIN_WIDE_MAP);
		JoinProperties joinProperties = meta.getJoinProperties();
		assertThat(joinProperties.getCascadeTypes()).containsExactly(PERSIST, MERGE);
		assertThat(joinExternalWideMaps).isEmpty();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_fill_external_widemap_hashmap() throws Exception
	{
		@ColumnFamily
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
		EntityParser.entityClassTL.set(Test.class);
		Field usersField = Test.class.getDeclaredField("users");
		PropertyMeta<Integer, UserBean> meta = (PropertyMeta<Integer, UserBean>) parser
				.parseJoin(usersField);

		assertThat(joinExternalWideMaps).hasSize(1);
		assertThat(
				(PropertyMeta<Integer, UserBean>) joinExternalWideMaps.keySet().iterator().next())
				.isSameAs(meta);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_parse_external_join_wide_map() throws Exception
	{
		Keyspace keyspace = mock(ExecutingKeyspace.class);
		PropertyMeta<Void, Long> idMeta = mock(PropertyMeta.class);
		when(idMeta.getValueSerializer()).thenReturn(LONG_SRZ);
		PropertyMeta<Integer, UserBean> propertyMeta = new PropertyMeta<Integer, UserBean>();

		parser.fillExternalJoinWideMap(keyspace, idMeta, propertyMeta, "externalTableName");

		assertThat(propertyMeta.type()).isEqualTo(PropertyType.EXTERNAL_JOIN_WIDE_MAP);

		assertThat(propertyMeta.getExternalWideMapProperties()).isNotNull();
		ExternalWideMapProperties<Long> externalWideMapProperties = (ExternalWideMapProperties<Long>) propertyMeta
				.getExternalWideMapProperties();
		assertThat(externalWideMapProperties.getExternalWideMapDao()).isNull();
		assertThat(externalWideMapProperties.getIdSerializer()).isEqualTo(LONG_SRZ);
		assertThat(externalWideMapProperties.getExternalColumnFamilyName()).isEqualTo(
				"externalTableName");

		assertThat(propertyMetas).hasSize(1);
		assertThat((PropertyMeta<Integer, UserBean>) propertyMetas.values().iterator().next())
				.isSameAs(propertyMeta);

		assertThat(joinPropertyMetaToBeFilled).hasSize(1);
		assertThat(
				(PropertyMeta<Integer, UserBean>) joinPropertyMetaToBeFilled.keySet().iterator()
						.next()).isSameAs(propertyMeta);
	}

}
