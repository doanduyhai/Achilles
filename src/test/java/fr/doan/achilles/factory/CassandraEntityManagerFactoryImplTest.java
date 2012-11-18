package fr.doan.achilles.factory;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;
import fr.doan.achilles.columnFamily.ColumnFamilyHelper;
import fr.doan.achilles.metadata.EntityMeta;
import fr.doan.achilles.parser.EntityExplorer;
import fr.doan.achilles.parser.EntityParser;

@RunWith(MockitoJUnitRunner.class)
@SuppressWarnings({ "unchecked", "rawtypes" })
public class CassandraEntityManagerFactoryImplTest {
    @InjectMocks
    private final CassandraEntityManagerFactoryImpl factory = new CassandraEntityManagerFactoryImpl();

    @Mock
    private Cluster cluster;

    @Mock
    private Keyspace keyspace;

    @Mock
    private List<String> entityPackages;

    @Mock
    private Map<Class<?>, EntityMeta<?>> entityMetaMap;

    @Mock
    private EntityMeta meta;

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
        when(entityParser.parseEntity(keyspace, Long.class)).thenReturn(meta);
        when(entityParser.parseEntity(keyspace, String.class)).thenReturn(meta);
        when(entityExplorer.discoverEntities(entityPackages)).thenReturn(classes);

        ReflectionTestUtils.setField(factory, "forceColumnFamilyCreation", true);
        ReflectionTestUtils.invokeMethod(factory, "bootstrap", (Object[]) null);
        verify(entityMetaMap).put(Long.class, meta);
        verify(entityMetaMap).put(String.class, meta);
        verify(columnFamilyHelper).validateColumnFamilies(entityMetaMap, true);

    }

    @Test(expected = IllegalArgumentException.class)
    public void should_exception_when_no_entity_found() throws Exception {
        when(entityExplorer.discoverEntities(entityPackages)).thenReturn(null);

        ReflectionTestUtils.invokeMethod(factory, "bootstrap", (Object[]) null);
    }

}
