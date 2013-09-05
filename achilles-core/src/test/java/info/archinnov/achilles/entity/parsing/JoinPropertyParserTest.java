/**
 *
 * Copyright (C) 2012-2013 DuyHai DOAN
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package info.archinnov.achilles.entity.parsing;

import static javax.persistence.CascadeType.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.context.ConfigurationContext;
import info.archinnov.achilles.entity.metadata.JoinProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.parsing.context.EntityParsingContext;
import info.archinnov.achilles.entity.parsing.context.PropertyParsingContext;
import info.archinnov.achilles.exception.AchillesBeanMappingException;
import info.archinnov.achilles.test.parser.entity.UserBean;
import info.archinnov.achilles.type.ConsistencyLevel;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.JoinColumn;
import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class JoinPropertyParserTest {
	@Rule
	public ExpectedException expectedEx = ExpectedException.none();

	private JoinPropertyParser parser = new JoinPropertyParser();

	private Map<PropertyMeta, Class<?>> joinPropertyMetaToBeFilled = new HashMap<PropertyMeta, Class<?>>();
	private EntityParsingContext entityContext;

	@Mock
	private AchillesConsistencyLevelPolicy policy;

	private ConfigurationContext configContext;

	@Before
	public void setUp() {
		joinPropertyMetaToBeFilled.clear();
		configContext = new ConfigurationContext();
		configContext.setConsistencyPolicy(policy);

		when(policy.getDefaultGlobalReadConsistencyLevel()).thenReturn(
				ConsistencyLevel.ONE);
		when(policy.getDefaultGlobalWriteConsistencyLevel()).thenReturn(
				ConsistencyLevel.ALL);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_parse_join_simple_property() throws Exception {
		@SuppressWarnings("unused")
		class Test {
			@OneToOne(cascade = { PERSIST, MERGE })
			@JoinColumn
			private UserBean user;

			public UserBean getUser() {
				return user;
			}

			public void setUser(UserBean user) {
				this.user = user;
			}
		}

		PropertyParsingContext context = newJoinParsingContext(Test.class,
				Test.class.getDeclaredField("user"));

		PropertyMeta meta = (PropertyMeta) parser.parseJoin(context);

		assertThat(meta.type()).isEqualTo(PropertyType.JOIN_SIMPLE);
		JoinProperties joinProperties = meta.getJoinProperties();
		assertThat(joinProperties.getCascadeTypes()).contains(PERSIST, MERGE);

		assertThat((PropertyMeta) context.getPropertyMetas().get("user"))
				.isSameAs(meta);

		assertThat((Class<UserBean>) joinPropertyMetaToBeFilled.get(meta))
				.isEqualTo(UserBean.class);
	}

	@Test
	public void should_parse_join_property_no_cascade() throws Exception {
		@SuppressWarnings("unused")
		class Test {
			@JoinColumn
			private UserBean user;

			public UserBean getUser() {
				return user;
			}

			public void setUser(UserBean user) {
				this.user = user;
			}
		}
		PropertyParsingContext context = newJoinParsingContext(Test.class,
				Test.class.getDeclaredField("user"));
		PropertyMeta meta = parser.parseJoin(context);

		JoinProperties joinProperties = meta.getJoinProperties();
		assertThat(joinProperties.getCascadeTypes()).isEmpty();
	}

	@Test
	public void should_exception_when_join_simple_property_has_cascade_remove()
			throws Exception {
		@SuppressWarnings("unused")
		class Test {
			@ManyToOne(cascade = { PERSIST, REMOVE })
			@JoinColumn
			private UserBean user;

			public UserBean getUser() {
				return user;
			}

			public void setUser(UserBean user) {
				this.user = user;
			}

		}

		expectedEx.expect(AchillesBeanMappingException.class);
		expectedEx
				.expectMessage("CascadeType.REMOVE is not supported for join columns");
		PropertyParsingContext context = newJoinParsingContext(Test.class,
				Test.class.getDeclaredField("user"));
		parser.parseJoin(context);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_parse_join_list_property() throws Exception {
		@SuppressWarnings("unused")
		class Test {
			@OneToMany(cascade = { PERSIST, MERGE })
			@JoinColumn
			private List<UserBean> users;

			public List<UserBean> getUsers() {
				return users;
			}

			public void setUsers(List<UserBean> users) {
				this.users = users;
			}
		}
		PropertyParsingContext context = newJoinParsingContext(Test.class,
				Test.class.getDeclaredField("users"));

		PropertyMeta meta = parser.parseJoin(context);

		assertThat(meta.type()).isEqualTo(PropertyType.JOIN_LIST);
		JoinProperties joinProperties = meta.getJoinProperties();
		assertThat(joinProperties.getCascadeTypes()).contains(PERSIST, MERGE);
		assertThat((Class<UserBean>) joinPropertyMetaToBeFilled.get(meta))
				.isEqualTo(UserBean.class);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_parse_join_set_property() throws Exception {
		@SuppressWarnings("unused")
		class Test {
			@ManyToMany(cascade = { PERSIST, MERGE })
			@JoinColumn
			private Set<UserBean> users;

			public Set<UserBean> getUsers() {
				return users;
			}

			public void setUsers(Set<UserBean> users) {
				this.users = users;
			}
		}
		PropertyParsingContext context = newJoinParsingContext(Test.class,
				Test.class.getDeclaredField("users"));
		PropertyMeta meta = parser.parseJoin(context);

		assertThat(meta.type()).isEqualTo(PropertyType.JOIN_SET);
		JoinProperties joinProperties = meta.getJoinProperties();
		assertThat(joinProperties.getCascadeTypes()).contains(PERSIST, MERGE);
		assertThat((Class<UserBean>) joinPropertyMetaToBeFilled.get(meta))
				.isEqualTo(UserBean.class);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_parse_join_map_property() throws Exception {
		@SuppressWarnings("unused")
		class Test {
			@ManyToOne(cascade = { PERSIST, REFRESH })
			@JoinColumn
			private Map<Integer, UserBean> users;

			public Map<Integer, UserBean> getUsers() {
				return users;
			}

			public void setUsers(Map<Integer, UserBean> users) {
				this.users = users;
			}
		}
		PropertyParsingContext context = newJoinParsingContext(Test.class,
				Test.class.getDeclaredField("users"));
		PropertyMeta meta = parser.parseJoin(context);

		assertThat(meta.type()).isEqualTo(PropertyType.JOIN_MAP);
		JoinProperties joinProperties = meta.getJoinProperties();
		assertThat(joinProperties.getCascadeTypes()).contains(PERSIST, REFRESH);
		assertThat((Class<UserBean>) joinPropertyMetaToBeFilled.get(meta))
				.isEqualTo(UserBean.class);
	}

	private <T> PropertyParsingContext newJoinParsingContext(
			Class<T> entityClass, Field field) {
		entityContext = new EntityParsingContext( //
				joinPropertyMetaToBeFilled, //
				configContext, entityClass);

		PropertyParsingContext context = entityContext
				.newPropertyContext(field);
		context.setJoinColumn(true);

		return context;
	}
}
