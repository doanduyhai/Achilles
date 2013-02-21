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
import info.archinnov.achilles.entity.metadata.ExternalWideMapProperties;
import info.archinnov.achilles.entity.metadata.JoinProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.type.WideMap;
import info.archinnov.achilles.exception.BeanMappingException;
import info.archinnov.achilles.serializer.SerializerUtils;

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
import javax.persistence.Table;

import me.prettyprint.cassandra.model.ExecutingKeyspace;
import me.prettyprint.hector.api.Keyspace;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

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

	private JoinPropertyParser parser = new JoinPropertyParser();

	private ObjectMapper objectMapper = new ObjectMapper();

	private Map<String, PropertyMeta<?, ?>> propertyMetas = new HashMap<String, PropertyMeta<?, ?>>();
	private Map<Field, String> joinExternalWideMaps = new HashMap<Field, String>();
	private Map<PropertyMeta<?, ?>, Class<?>> joinPropertyMetaToBeFilled = new HashMap<PropertyMeta<?, ?>, Class<?>>();

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

		PropertyMeta<?, ?> meta = parser.parseJoin(propertyMetas, //
				joinExternalWideMaps, //
				joinPropertyMetaToBeFilled, //
				Test.class, //
				Test.class.getDeclaredField("user"), objectMapper);

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

		PropertyMeta<?, ?> meta = parser.parseJoin(propertyMetas, //
				joinExternalWideMaps, //
				joinPropertyMetaToBeFilled, //
				Test.class, //
				Test.class.getDeclaredField("user"), objectMapper);

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

		parser.parseJoin(propertyMetas, //
				joinExternalWideMaps, //
				joinPropertyMetaToBeFilled, //
				Test.class, //
				Test.class.getDeclaredField("user"), objectMapper);
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

		PropertyMeta<?, ?> meta = parser.parseJoin(propertyMetas, //
				joinExternalWideMaps, //
				joinPropertyMetaToBeFilled, //
				Test.class, //
				Test.class.getDeclaredField("users"), objectMapper);

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

		PropertyMeta<?, ?> meta = parser.parseJoin(propertyMetas, //
				joinExternalWideMaps, //
				joinPropertyMetaToBeFilled, //
				Test.class, //
				Test.class.getDeclaredField("users"), objectMapper);

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

		PropertyMeta<?, ?> meta = parser.parseJoin(propertyMetas, //
				joinExternalWideMaps, //
				joinPropertyMetaToBeFilled, //
				Test.class, //
				Test.class.getDeclaredField("users"), objectMapper);

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

		PropertyMeta<?, ?> meta = parser.parseJoin(propertyMetas, //
				joinExternalWideMaps, //
				joinPropertyMetaToBeFilled, //
				Test.class, //
				Test.class.getDeclaredField("users"), objectMapper);

		assertThat(meta.type()).isEqualTo(PropertyType.JOIN_WIDE_MAP);
		JoinProperties joinProperties = meta.getJoinProperties();
		assertThat(joinProperties.getCascadeTypes()).containsExactly(PERSIST, MERGE);
		assertThat(joinExternalWideMaps).isEmpty();
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
		when(idMeta.getValueSerializer()).thenReturn(LONG_SRZ);

		PropertyMeta<Integer, UserBean> meta = (PropertyMeta<Integer, UserBean>) parser
				.parseExternalJoinWideMapProperty(keyspace, idMeta, Test.class,
						Test.class.getDeclaredField("users"), "users", "cf", objectMapper);

		assertThat(meta.type()).isEqualTo(PropertyType.EXTERNAL_JOIN_WIDE_MAP);
		JoinProperties joinProperties = meta.getJoinProperties();
		assertThat(joinProperties.getCascadeTypes()).isEmpty();

		ExternalWideMapProperties<Long> externalWideMapProperties = (ExternalWideMapProperties<Long>) meta
				.getExternalWideMapProperties();
		assertThat(externalWideMapProperties.getExternalWideMapDao()).isNull();
		assertThat(externalWideMapProperties.getIdSerializer()).isEqualTo(SerializerUtils.LONG_SRZ);
		assertThat(externalWideMapProperties.getExternalColumnFamilyName()).isEqualTo("tablename");
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

		Field usersField = Test.class.getDeclaredField("users");
		PropertyMeta<Integer, UserBean> meta = (PropertyMeta<Integer, UserBean>) parser.parseJoin(
				propertyMetas, //
				joinExternalWideMaps, joinPropertyMetaToBeFilled, //
				Test.class, usersField, objectMapper);

		assertThat(joinExternalWideMaps).hasSize(1);
		assertThat(joinExternalWideMaps.get(usersField)).isEqualTo("users");
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_parse_join_widemap_for_column_family() throws Exception
	{
		@ColumnFamily
		@Table(name = "columnFamily")
		class Test
		{
			@ManyToMany
			@JoinColumn
			private WideMap<Integer, UserBean> users;

			public WideMap<Integer, UserBean> getUsers()
			{
				return users;
			}
		}

		Keyspace keyspace = mock(ExecutingKeyspace.class);
		PropertyMeta<Void, Long> idMeta = mock(PropertyMeta.class);
		when(idMeta.getValueSerializer()).thenReturn(LONG_SRZ);

		PropertyMeta<?, ?> meta = parser.parseExternalJoinWideMapProperty(keyspace, idMeta,
				Test.class, Test.class.getDeclaredField("users"), "users", "columnFamily",
				objectMapper);

		assertThat(meta.type()).isEqualTo(PropertyType.EXTERNAL_JOIN_WIDE_MAP);
		JoinProperties joinProperties = meta.getJoinProperties();
		assertThat(joinProperties.getCascadeTypes()).isEmpty();
		ExternalWideMapProperties<Long> externalWideMapProperties = (ExternalWideMapProperties<Long>) meta
				.getExternalWideMapProperties();
		assertThat(externalWideMapProperties.getExternalWideMapDao()).isNull();
		assertThat(externalWideMapProperties.getIdSerializer()).isEqualTo(LONG_SRZ);
		assertThat(externalWideMapProperties.getExternalColumnFamilyName()).isEqualTo(
				"columnFamily");
	}
}
