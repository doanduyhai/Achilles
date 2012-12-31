package fr.doan.achilles.entity.factory;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.persistence.EntityManager;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;
import fr.doan.achilles.columnFamily.ColumnFamilyHelper;
import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.entity.metadata.JoinProperties;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.metadata.SimpleMeta;
import fr.doan.achilles.entity.parser.EntityExplorer;
import fr.doan.achilles.entity.parser.EntityParser;
import fr.doan.achilles.exception.BeanMappingException;

@RunWith(MockitoJUnitRunner.class)
@SuppressWarnings({ "unchecked", "rawtypes" })
public class ThriftEntityManagerFactoryImplTest {
    @InjectMocks
    private ThriftEntityManagerFactoryImpl factory = new ThriftEntityManagerFactoryImpl();

    @Mock
    private Cluster cluster;

    @Mock
    private Keyspace keyspace;

    @Mock
    private List<String> entityPackages;

    @Mock
    private Map<Class<?>, EntityMeta<?>> entityMetaMap;

    @Mock
    private Map<PropertyMeta<?, ?>, Class<?>> joinPropertyMetaToBeFilled;

    @Mock
    private EntityMeta entityMeta1;

    @Mock
    private EntityMeta entityMeta2;

    @Mock
    private SimpleMeta<Long> longPropertyMeta;

    @Mock
    private EntityParser entityParser;

    @Mock
    private EntityExplorer entityExplorer;

    @Mock
    private ColumnFamilyHelper columnFamilyHelper;

    @Test
    public void should_bootstrap() throws Exception {
        List<Class<?>> classes = new ArrayList<Class<?>>();
        classes.add(Long.class);
        classes.add(String.class);
        when(entityExplorer.discoverEntities(entityPackages)).thenReturn(classes);
        when(entityParser.parseEntity(eq(keyspace), eq(Long.class), any(Map.class))).thenReturn(entityMeta1);
        when(entityParser.parseEntity(eq(keyspace), eq(String.class), any(Map.class))).thenReturn(entityMeta2);

        ReflectionTestUtils.setField(factory, "forceColumnFamilyCreation", true);
        factory.bootstrap();

        verify(entityMetaMap).put(Long.class, entityMeta1);
        verify(entityMetaMap).put(String.class, entityMeta2);
        verify(columnFamilyHelper).validateColumnFamilies(entityMetaMap, true);

    }

    @Test(expected = IllegalArgumentException.class)
    public void should_exception_when_no_entity_found() throws Exception {
        when(entityExplorer.discoverEntities(entityPackages)).thenReturn(new ArrayList<Class<?>>());
        factory.bootstrap();
    }

    @Test
    public void should_discover_join_entities() throws Exception {
        Map<PropertyMeta<?, ?>, Class<?>> joinPropertyMetaToBeFilled = new HashMap<PropertyMeta<?, ?>, Class<?>>();
        joinPropertyMetaToBeFilled.put(longPropertyMeta, Long.class);

        List<Class<?>> classes = new ArrayList<Class<?>>();
        classes.add(Long.class);
        classes.add(String.class);
        when(entityExplorer.discoverEntities(entityPackages)).thenReturn(classes);
        when(entityParser.parseEntity(eq(keyspace), eq(Long.class), any(Map.class))).thenReturn(entityMeta1);
        when(entityParser.parseEntity(eq(keyspace), eq(String.class), any(Map.class))).thenReturn(entityMeta2);

        JoinProperties joinProperties = new JoinProperties();
        when(longPropertyMeta.getJoinProperties()).thenReturn(joinProperties);

        when(entityMetaMap.containsKey(Long.class)).thenReturn(true);
        when(entityMetaMap.get(Long.class)).thenReturn(entityMeta1);

        factory.discoverEntities(joinPropertyMetaToBeFilled);

        assertThat(joinProperties.getEntityMeta()).isSameAs(entityMeta1);
        verify(entityMetaMap).put(Long.class, entityMeta1);
        verify(entityMetaMap).put(String.class, entityMeta2);

    }

    @Test(expected = BeanMappingException.class)
    public void should_throw_exception_when_no_entity_meta_found_for_join_entity() throws Exception {
        Map<PropertyMeta<?, ?>, Class<?>> joinPropertyMetaToBeFilled = new HashMap<PropertyMeta<?, ?>, Class<?>>();
        joinPropertyMetaToBeFilled.put(longPropertyMeta, Long.class);

        List<Class<?>> classes = new ArrayList<Class<?>>();
        classes.add(Long.class);
        classes.add(String.class);
        when(entityExplorer.discoverEntities(entityPackages)).thenReturn(classes);
        when(entityParser.parseEntity(eq(keyspace), eq(Long.class), any(Map.class))).thenReturn(entityMeta1);
        when(entityParser.parseEntity(eq(keyspace), eq(String.class), any(Map.class))).thenReturn(entityMeta2);

        JoinProperties joinProperties = new JoinProperties();
        when(longPropertyMeta.getJoinProperties()).thenReturn(joinProperties);

        when(entityMetaMap.containsKey(Long.class)).thenReturn(false);

        factory.discoverEntities(joinPropertyMetaToBeFilled);
    }

    @Test
    public void should_create_entity_manager() throws Exception {
        EntityManager em = factory.createEntityManager();

        assertThat(em).isNotNull();
    }

    @Test
    public void should_create_entity_manager_with_parameters() throws Exception {
        EntityManager em = factory.createEntityManager(new HashMap());

        assertThat(em).isNotNull();
    }

    @Test
    public void should_return_true_when_open_called() throws Exception {
        assertThat(factory.isOpen()).isTrue();
    }

    @Test
    public void should_do_nothing_when_close_called() throws Exception {
        factory.close();
    }
}
