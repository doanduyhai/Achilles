package fr.doan.achilles.operations;

import static fr.doan.achilles.metadata.builder.EntityMetaBuilder.entityMetaBuilder;
import static fr.doan.achilles.metadata.builder.ListPropertyMetaBuilder.listPropertyMetaBuilder;
import static fr.doan.achilles.metadata.builder.SetPropertyMetaBuilder.setPropertyMetaBuilder;
import static fr.doan.achilles.metadata.builder.SimplePropertyMetaBuilder.simplePropertyMetaBuilder;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.persistence.Column;
import javax.persistence.Id;
import me.prettyprint.cassandra.model.ExecutingKeyspace;
import me.prettyprint.cassandra.model.MutatorImpl;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.mutation.Mutator;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;
import fr.doan.achilles.bean.BeanPropertyHelper;
import fr.doan.achilles.dao.GenericDao;
import fr.doan.achilles.holder.KeyValueHolder;
import fr.doan.achilles.metadata.EntityMeta;
import fr.doan.achilles.metadata.PropertyMeta;
import fr.doan.achilles.metadata.PropertyType;
import fr.doan.achilles.metadata.builder.MapPropertyMetaBuilder;

@RunWith(MockitoJUnitRunner.class)
public class EntityPersisterTest {

    @InjectMocks
    private EntityPersister persister = new EntityPersister();

    @Mock
    private BeanPropertyHelper helper;

    @Mock
    private GenericDao<Long> dao;

    @Mock
    private ExecutingKeyspace keyspace;

    private MutatorImpl<Long> mutator = new MutatorImpl<Long>(keyspace);

    @Before
    public void setUp() throws Exception {

    }

    @Test
    public void should_persist_simple_property() throws Exception {

        Bean entity = new Bean();

        PropertyMeta<Long> idMeta = buildIdMeta();

        Method[] nameAccessors = new Method[2];
        nameAccessors[0] = Bean.class.getDeclaredMethod("getName", (Class<?>[]) null);
        nameAccessors[1] = Bean.class.getDeclaredMethod("setName", String.class);

        PropertyMeta<String> nameMeta = simplePropertyMetaBuilder(String.class).propertyName("name")
                .accessors(nameAccessors).build();

        Map<String, PropertyMeta<?>> propertyMetas = new HashMap<String, PropertyMeta<?>>();
        propertyMetas.put("name", nameMeta);

        EntityMeta<Long> entityMeta = buildEntityMeta(idMeta, propertyMetas);

        when(helper.getKey(entity, idMeta)).thenReturn(2L);
        when(dao.buildMutator()).thenReturn(mutator);
        when(helper.getValueFromField(entity, nameAccessors[0])).thenReturn("foo");

        Composite composite = new Composite();
        when(dao.buildCompositeForProperty("name", PropertyType.SIMPLE, 0)).thenReturn(composite);

        persister.persist(entity, entityMeta);

        verify(dao).insertColumnBatch(2L, composite, "foo", mutator);
    }

    @Test
    public void should_persist_list_property() throws Exception {
        Bean entity = new Bean();

        PropertyMeta<Long> idMeta = buildIdMeta();

        Method[] friendsAccessors = new Method[2];
        friendsAccessors[0] = Bean.class.getDeclaredMethod("getFriends", (Class<?>[]) null);
        friendsAccessors[1] = Bean.class.getDeclaredMethod("setFriends", List.class);
        PropertyMeta<String> friendsMeta = listPropertyMetaBuilder(String.class).listClass(List.class)
                .propertyName("friends").accessors(friendsAccessors).build();

        Map<String, PropertyMeta<?>> propertyMetas = new HashMap<String, PropertyMeta<?>>();
        propertyMetas.put("friends", friendsMeta);

        EntityMeta<Long> entityMeta = buildEntityMeta(idMeta, propertyMetas);

        when(helper.getKey(entity, idMeta)).thenReturn(2L);
        when(dao.buildMutator()).thenReturn(mutator);

        ArrayList<String> friends = new ArrayList<String>();
        friends.add("foo");
        friends.add("bar");
        when(helper.getValueFromField(entity, friendsAccessors[0])).thenReturn(friends);

        Composite composite = new Composite();
        when(dao.buildCompositeForProperty("friends", PropertyType.LIST, 0)).thenReturn(composite);
        when(dao.buildCompositeForProperty("friends", PropertyType.LIST, 1)).thenReturn(composite);

        persister.persist(entity, entityMeta);

        verify(dao).insertColumnBatch(2L, composite, "foo", mutator);
        verify(dao).insertColumnBatch(2L, composite, "bar", mutator);
    }

    @Test
    public void should_persist_set_property() throws Exception {
        Bean entity = new Bean();

        PropertyMeta<Long> idMeta = buildIdMeta();

        Method[] followersAccessors = new Method[2];
        followersAccessors[0] = Bean.class.getDeclaredMethod("getFollowers", (Class<?>[]) null);
        followersAccessors[1] = Bean.class.getDeclaredMethod("setFollowers", Set.class);
        PropertyMeta<String> followersMeta = setPropertyMetaBuilder(String.class).setClass(Set.class)
                .propertyName("followers").accessors(followersAccessors).build();

        Map<String, PropertyMeta<?>> propertyMetas = new HashMap<String, PropertyMeta<?>>();
        propertyMetas.put("followers", followersMeta);

        EntityMeta<Long> entityMeta = buildEntityMeta(idMeta, propertyMetas);

        when(helper.getKey(entity, idMeta)).thenReturn(2L);
        when(dao.buildMutator()).thenReturn(mutator);

        Set<String> followers = new HashSet<String>();

        followers.add("George");
        followers.add("Paul");
        when(helper.getValueFromField(entity, followersAccessors[0])).thenReturn(followers);

        Composite composite = new Composite();
        when(dao.buildCompositeForProperty("followers", PropertyType.SET, "George".hashCode())).thenReturn(composite);
        when(dao.buildCompositeForProperty("followers", PropertyType.SET, "Paul".hashCode())).thenReturn(composite);

        persister.persist(entity, entityMeta);

        verify(dao).insertColumnBatch(2L, composite, "George", mutator);
        verify(dao).insertColumnBatch(2L, composite, "Paul", mutator);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void should_persist_map_property() throws Exception {
        Bean entity = new Bean();

        PropertyMeta<Long> idMeta = buildIdMeta();

        Method[] preferencesAccessors = new Method[2];
        preferencesAccessors[0] = Bean.class.getDeclaredMethod("getPreferences", (Class<?>[]) null);
        preferencesAccessors[1] = Bean.class.getDeclaredMethod("setPreferences", Map.class);
        PropertyMeta<String> preferencesMeta = MapPropertyMetaBuilder.mapPropertyMetaBuilder(String.class)
                .keyClass(Integer.class).mapClass(Map.class).propertyName("preferences")
                .accessors(preferencesAccessors).build();

        Map<String, PropertyMeta<?>> propertyMetas = new HashMap<String, PropertyMeta<?>>();
        propertyMetas.put("preferences", preferencesMeta);

        EntityMeta<Long> entityMeta = buildEntityMeta(idMeta, propertyMetas);

        when(helper.getKey(entity, idMeta)).thenReturn(2L);
        when(dao.buildMutator()).thenReturn(mutator);

        Map<Integer, String> preferences = new HashMap<Integer, String>();
        preferences.put(1, "FR");
        preferences.put(2, "Paris");
        preferences.put(3, "75014");
        when(helper.getValueFromField(entity, preferencesAccessors[0])).thenReturn(preferences);

        Composite composite = new Composite();
        when(dao.buildCompositeForProperty("preferences", PropertyType.MAP, 1)).thenReturn(composite);
        when(dao.buildCompositeForProperty("preferences", PropertyType.MAP, 2)).thenReturn(composite);
        when(dao.buildCompositeForProperty("preferences", PropertyType.MAP, 3)).thenReturn(composite);

        persister.persist(entity, entityMeta);

        verify(dao, times(3)).insertColumnBatch(eq(2L), eq(composite), any(KeyValueHolder.class), any(Mutator.class));
    }

    ////////////////////////////////////////////////////////

    private PropertyMeta<Long> buildIdMeta() throws NoSuchMethodException {
        Method[] idAccessors = new Method[2];
        idAccessors[0] = Bean.class.getDeclaredMethod("getId", (Class<?>[]) null);
        idAccessors[1] = Bean.class.getDeclaredMethod("setId", Long.class);
        PropertyMeta<Long> idMeta = simplePropertyMetaBuilder(Long.class).propertyName("id").accessors(idAccessors)
                .build();
        return idMeta;
    }

    @SuppressWarnings("unchecked")
    private EntityMeta<Long> buildEntityMeta(PropertyMeta<Long> idMeta, Map<String, PropertyMeta<?>> propertyMetas) {
        EntityMeta<Long> entityMeta = entityMetaBuilder(idMeta).keyspace(keyspace).canonicalClassName("bean")
                .serialVersionUID(1L).propertyMetas(propertyMetas).build();
        ReflectionTestUtils.setField(entityMeta, "dao", this.dao);
        return entityMeta;
    }

    class Bean {
        @Id
        private Long id;

        @Column
        private String name;

        @Column
        private List<String> friends;

        @Column
        private Set<String> followers;

        @Column
        private Map<Integer, String> preferences;

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public List<String> getFriends() {
            return friends;
        }

        public void setFriends(List<String> friends) {
            this.friends = friends;
        }

        public Set<String> getFollowers() {
            return followers;
        }

        public void setFollowers(Set<String> followers) {
            this.followers = followers;
        }

        public Map<Integer, String> getPreferences() {
            return preferences;
        }

        public void setPreferences(Map<Integer, String> preferences) {
            this.preferences = preferences;
        }
    }
}
