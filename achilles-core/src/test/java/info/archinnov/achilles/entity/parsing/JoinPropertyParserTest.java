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
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
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

/**
 * AchillesJoinPropertyParserTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class JoinPropertyParserTest
{
    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    private JoinPropertyParser parser = new JoinPropertyParser();

    private Map<PropertyMeta<?, ?>, Class<?>> joinPropertyMetaToBeFilled = new HashMap<PropertyMeta<?, ?>, Class<?>>();
    private EntityParsingContext entityContext;

    @Mock
    private AchillesConsistencyLevelPolicy policy;

    private ConfigurationContext configContext;

    @Before
    public void setUp()
    {
        joinPropertyMetaToBeFilled.clear();
        configContext = new ConfigurationContext();
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

        PropertyParsingContext context = newJoinParsingContext(Test.class,
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
        PropertyParsingContext context = newJoinParsingContext(Test.class,
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
        PropertyParsingContext context = newJoinParsingContext(Test.class,
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
        PropertyParsingContext context = newJoinParsingContext(Test.class,
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
        PropertyParsingContext context = newJoinParsingContext(Test.class,
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
        PropertyParsingContext context = newJoinParsingContext(Test.class,
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
    public void should_parse_external_join_wide_map() throws Exception
    {

        PropertyMeta<Void, Long> idMeta = PropertyMetaTestBuilder.valueClass(Long.class).build();
        PropertyMeta<Integer, UserBean> propertyMeta = PropertyMetaTestBuilder.keyValueClass(
                Integer.class, UserBean.class).build();

        initEntityParsingContext();

        parser.fillJoinWideMap(entityContext, idMeta, propertyMeta, "externalTableName");

        assertThat(propertyMeta.getExternalTableName()).isEqualTo("externalTableName");
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

    private <T> PropertyParsingContext newJoinParsingContext(Class<T> entityClass, Field field)
    {
        entityContext = new EntityParsingContext( //
                joinPropertyMetaToBeFilled, //
                configContext, entityClass);

        PropertyParsingContext context = entityContext.newPropertyContext(field);
        context.setJoinColumn(true);

        return context;
    }

    private void initEntityParsingContext()
    {
        entityContext = new EntityParsingContext( //
                joinPropertyMetaToBeFilled, //
                configContext, CompleteBean.class);
    }
}
