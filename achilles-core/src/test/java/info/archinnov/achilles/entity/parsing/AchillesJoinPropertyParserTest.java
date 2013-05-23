package info.archinnov.achilles.entity.parsing;

import static info.archinnov.achilles.type.ConsistencyLevel.QUORUM;
import static javax.persistence.CascadeType.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.annotations.Consistency;
import info.archinnov.achilles.annotations.WideRow;
import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.context.AchillesConfigurationContext;
import info.archinnov.achilles.entity.metadata.JoinProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.parsing.context.AchillesEntityParsingContext;
import info.archinnov.achilles.entity.parsing.context.AchillesPropertyParsingContext;
import info.archinnov.achilles.exception.AchillesBeanMappingException;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.WideMap;

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

import mapping.entity.CompleteBean;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import parser.entity.UserBean;
import testBuilders.PropertyMetaTestBuilder;

/**
 * AchillesJoinPropertyParserTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class AchillesJoinPropertyParserTest
{
	@Rule
	public ExpectedException expectedEx = ExpectedException.none();

	private AchillesJoinPropertyParser parser = new AchillesJoinPropertyParser();

	private Map<PropertyMeta<?, ?>, Class<?>> joinPropertyMetaToBeFilled = new HashMap<PropertyMeta<?, ?>, Class<?>>();
	private AchillesEntityParsingContext entityContext;

	@Mock
	private AchillesConsistencyLevelPolicy policy;

	private AchillesConfigurationContext configContext;

	@Before
	public void setUp()
	{
		joinPropertyMetaToBeFilled.clear();
		configContext = new AchillesConfigurationContext();
		configContext.setConsistencyPolicy(policy);

		when(policy.getDefaultGlobalReadConsistencyLevel()).thenReturn(ConsistencyLevel.ONE);
		when(policy.getDefaultGlobalWriteConsistencyLevel()).thenReturn(ConsistencyLevel.ALL);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_parse_join_simple_property() throws Exception
	{
		@SuppressWarnings("unused")
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

		AchillesPropertyParsingContext context = newContext(Test.class,
				Test.class.getDeclaredField("user"));

		PropertyMeta<Void, UserBean> meta = (PropertyMeta<Void, UserBean>) parser
				.parseJoin(context);

		assertThat(meta.type()).isEqualTo(PropertyType.JOIN_SIMPLE);
		JoinProperties joinProperties = meta.getJoinProperties();
		assertThat(joinProperties.getCascadeTypes()).contains(PERSIST, MERGE);

		assertThat((PropertyMeta<Void, UserBean>) context.getPropertyMetas().get("user")).isSameAs(
				meta);

		assertThat((Class<UserBean>) joinPropertyMetaToBeFilled.get(meta))
				.isEqualTo(UserBean.class);
	}

	@Test
	public void should_parse_join_property_no_cascade() throws Exception
	{
		@SuppressWarnings("unused")
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
		AchillesPropertyParsingContext context = newContext(Test.class,
				Test.class.getDeclaredField("user"));
		PropertyMeta<?, ?> meta = parser.parseJoin(context);

		JoinProperties joinProperties = meta.getJoinProperties();
		assertThat(joinProperties.getCascadeTypes()).isEmpty();
		assertThat(context.getJoinWideMaps()).isEmpty();
	}

	@Test
	public void should_exception_when_join_simple_property_has_cascade_remove() throws Exception
	{
		@SuppressWarnings("unused")
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

		expectedEx.expect(AchillesBeanMappingException.class);
		expectedEx.expectMessage("CascadeType.REMOVE is not supported for join columns");
		AchillesPropertyParsingContext context = newContext(Test.class,
				Test.class.getDeclaredField("user"));
		parser.parseJoin(context);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_parse_join_list_property() throws Exception
	{
		@SuppressWarnings("unused")
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
		AchillesPropertyParsingContext context = newContext(Test.class,
				Test.class.getDeclaredField("users"));

		PropertyMeta<?, ?> meta = parser.parseJoin(context);

		assertThat(meta.type()).isEqualTo(PropertyType.JOIN_LIST);
		JoinProperties joinProperties = meta.getJoinProperties();
		assertThat(joinProperties.getCascadeTypes()).contains(PERSIST, MERGE);
		assertThat(context.getJoinWideMaps()).isEmpty();
		assertThat((Class<UserBean>) joinPropertyMetaToBeFilled.get(meta))
				.isEqualTo(UserBean.class);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_parse_join_set_property() throws Exception
	{
		@SuppressWarnings("unused")
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
		AchillesPropertyParsingContext context = newContext(Test.class,
				Test.class.getDeclaredField("users"));
		PropertyMeta<?, ?> meta = parser.parseJoin(context);

		assertThat(meta.type()).isEqualTo(PropertyType.JOIN_SET);
		JoinProperties joinProperties = meta.getJoinProperties();
		assertThat(joinProperties.getCascadeTypes()).contains(PERSIST, MERGE);
		assertThat(context.getJoinWideMaps()).isEmpty();
		assertThat((Class<UserBean>) joinPropertyMetaToBeFilled.get(meta))
				.isEqualTo(UserBean.class);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_parse_join_map_property() throws Exception
	{
		@SuppressWarnings("unused")
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
		AchillesPropertyParsingContext context = newContext(Test.class,
				Test.class.getDeclaredField("users"));
		PropertyMeta<?, ?> meta = parser.parseJoin(context);

		assertThat(meta.type()).isEqualTo(PropertyType.JOIN_MAP);
		JoinProperties joinProperties = meta.getJoinProperties();
		assertThat(joinProperties.getCascadeTypes()).contains(PERSIST, REFRESH);
		assertThat(context.getJoinWideMaps()).isEmpty();
		assertThat((Class<UserBean>) joinPropertyMetaToBeFilled.get(meta))
				.isEqualTo(UserBean.class);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_parse_join_widemap() throws Exception
	{
		@SuppressWarnings("unused")
		class Test
		{
			@ManyToMany(cascade =
			{
					PERSIST,
					MERGE
			})
			@JoinColumn(table = "join_users_xxx")
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
		AchillesPropertyParsingContext context = newContext(Test.class,
				Test.class.getDeclaredField("users"));
		PropertyMeta<?, ?> meta = parser.parseJoin(context);

		assertThat(meta.type()).isEqualTo(PropertyType.JOIN_WIDE_MAP);
		JoinProperties joinProperties = meta.getJoinProperties();
		assertThat(joinProperties.getCascadeTypes()).contains(PERSIST, MERGE);
		assertThat(context.getJoinWideMaps().get(meta)).isEqualTo("join_users_xxx");
		assertThat((Class<UserBean>) joinPropertyMetaToBeFilled.get(meta))
				.isEqualTo(UserBean.class);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_fill_external_widemap_hashmap() throws Exception
	{
		@SuppressWarnings("unused")
		@WideRow
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
		AchillesPropertyParsingContext context = newContext(Test.class,
				Test.class.getDeclaredField("users"));
		PropertyMeta<Integer, UserBean> meta = (PropertyMeta<Integer, UserBean>) parser
				.parseJoin(context);

		Map<PropertyMeta<?, ?>, String> joinExternalWideMaps = context.getJoinWideMaps();
		assertThat(
				(PropertyMeta<Integer, UserBean>) joinExternalWideMaps.keySet().iterator().next())
				.isSameAs(meta);
	}

	@Test
	public void should_set_external_join_widemap_consistency_level() throws Exception
	{
		@SuppressWarnings("unused")
		class Test
		{
			@ManyToMany
			@JoinColumn(table = "tablename")
			@Consistency(read = QUORUM, write = ConsistencyLevel.ALL)
			private WideMap<Integer, UserBean> users;

			public WideMap<Integer, UserBean> getUsers()
			{
				return users;
			}
		}
		AchillesPropertyParsingContext context = newContext(Test.class,
				Test.class.getDeclaredField("users"));
		PropertyMeta<?, ?> propertyMeta = parser.parseJoin(context);
		assertThat(propertyMeta.getReadConsistencyLevel()).isEqualTo(QUORUM);
		assertThat(propertyMeta.getWriteConsistencyLevel()).isEqualTo(ConsistencyLevel.ALL);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_parse_external_join_wide_map() throws Exception
	{

		PropertyMeta<Void, Long> idMeta = PropertyMetaTestBuilder.valueClass(Long.class).build();
		PropertyMeta<Integer, UserBean> propertyMeta = PropertyMetaTestBuilder.noClass(
				Integer.class, UserBean.class).build();

		initEntityParsingContext();

		parser.fillJoinWideMap(entityContext, idMeta, propertyMeta, "externalTableName");

		assertThat(propertyMeta.getExternalCFName()).isEqualTo("externalTableName");
		assertThat((Class<Long>) propertyMeta.getIdClass()).isEqualTo(Long.class);

		assertThat(
				(PropertyMeta<Integer, UserBean>) entityContext
						.getPropertyMetas()
						.values()
						.iterator()
						.next()).isSameAs(propertyMeta);

		assertThat(joinPropertyMetaToBeFilled).hasSize(1);
		assertThat(
				(PropertyMeta<Integer, UserBean>) joinPropertyMetaToBeFilled
						.keySet()
						.iterator()
						.next()).isSameAs(propertyMeta);
	}

	private <T> AchillesPropertyParsingContext newContext(Class<T> entityClass, Field field)
	{
		entityContext = new AchillesEntityParsingContext( //
				joinPropertyMetaToBeFilled, //
				configContext, entityClass);

		AchillesPropertyParsingContext context = entityContext.newPropertyContext(field);
		context.setJoinColumn(true);

		return context;
	}

	private void initEntityParsingContext()
	{
		entityContext = new AchillesEntityParsingContext( //
				joinPropertyMetaToBeFilled, //
				configContext, CompleteBean.class);
	}
}
