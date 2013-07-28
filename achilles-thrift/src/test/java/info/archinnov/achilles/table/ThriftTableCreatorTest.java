package info.archinnov.achilles.table;

import static info.archinnov.achilles.entity.metadata.EntityMetaBuilder.entityMetaBuilder;
import static info.archinnov.achilles.entity.metadata.PropertyType.SIMPLE;
import static info.archinnov.achilles.type.ConsistencyLevel.ONE;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.context.ConfigurationContext;
import info.archinnov.achilles.counter.AchillesCounter;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.exception.AchillesInvalidTableException;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Pair;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import me.prettyprint.cassandra.model.BasicColumnFamilyDefinition;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

/**
 * ThriftTableCreatorTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class ThriftTableCreatorTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private ThriftTableCreator creator;

    @Mock
    private Cluster cluster;

    @Mock
    private Keyspace keyspace;

    @Mock
    private KeyspaceDefinition keyspaceDefinition;

    @Mock
    private ThriftColumnFamilyFactory columnFamilyFactory;

    @Mock
    private ThriftColumnFamilyValidator columnFamilyValidator;

    private Set<String> columnFamilyNames = new HashSet<String>();

    private Map<Class<?>, EntityMeta> entityMetaMap;

    private EntityMeta meta;

    private PropertyMeta<Void, String> simplePropertyMeta;

    private PropertyMeta<Void, Long> idMeta;

    private ConfigurationContext configContext = new ConfigurationContext();

    @Before
    public void setUp() throws Exception {
        idMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).type(SIMPLE).field("id").accessors()
                .build();

        columnFamilyNames.clear();
        configContext.setForceColumnFamilyCreation(true);
        when(keyspace.getKeyspaceName()).thenReturn("keyspace");
        when(cluster.describeKeyspace("keyspace")).thenReturn(keyspaceDefinition);

        creator = new ThriftTableCreator(cluster, keyspace);
        Whitebox.setInternalState(creator, ThriftColumnFamilyFactory.class, columnFamilyFactory);
        Whitebox.setInternalState(creator, ThriftColumnFamilyValidator.class, columnFamilyValidator);
        Whitebox.setInternalState(creator, "columnFamilyNames", columnFamilyNames);
    }

    @Test
    public void should_exception_when_keyspace_does_not_exist() throws Exception {

        when(cluster.describeKeyspace("keyspace")).thenReturn(null);

        exception.expect(AchillesException.class);
        exception.expectMessage("The keyspace 'keyspace' provided by configuration does not exist");

        new ThriftTableCreator(cluster, keyspace);

    }

    @Test
    public void should_list_all_column_families_on_initialization() throws Exception {
        ArrayList<ColumnFamilyDefinition> cfDefs = new ArrayList<ColumnFamilyDefinition>();
        when(keyspace.getKeyspaceName()).thenReturn("keyspace");
        when(cluster.describeKeyspace("keyspace")).thenReturn(keyspaceDefinition);
        when(keyspaceDefinition.getCfDefs()).thenReturn(cfDefs);

        ThriftTableCreator creator = new ThriftTableCreator(cluster, keyspace);

        assertThat(Whitebox.getInternalState(creator, "cfDefs")).isSameAs(cfDefs);

    }

    @Test
    public void should_discover_column_family() throws Exception {

        BasicColumnFamilyDefinition cfDef = new BasicColumnFamilyDefinition();
        cfDef.setName("testCF");
        cfDef.setKeyValidationClass("keyValidationClass");
        cfDef.setComment("comment");

        Whitebox.setInternalState(creator, "cfDefs", Arrays.asList((ColumnFamilyDefinition) cfDef));

        ColumnFamilyDefinition discoveredCfDef = creator.discoverTable("testCF");

        assertThat(discoveredCfDef).isNotNull();
        assertThat(discoveredCfDef.getName()).isEqualTo("testCF");
        assertThat(discoveredCfDef.getKeyValidationClass()).isEqualTo("keyValidationClass");
        assertThat(discoveredCfDef.getComment()).isEqualTo("comment");
    }

    @Test
    public void should_add_column_family() throws Exception {
        BasicColumnFamilyDefinition cfDef = new BasicColumnFamilyDefinition();
        creator.addTable(cfDef);

        verify(cluster).addColumnFamily(cfDef, true);
    }

    @Test
    public void should_not_add_column_family_if_already_added() throws Exception {
        BasicColumnFamilyDefinition cfDef = new BasicColumnFamilyDefinition();
        cfDef.setName("name");
        columnFamilyNames.add("name");

        creator.addTable(cfDef);

        verify(cluster, never()).addColumnFamily(cfDef, true);
    }

    @Test
    public void should_create_column_family_for_entity() throws Exception {
        prepareData();
        BasicColumnFamilyDefinition cfDef = new BasicColumnFamilyDefinition();
        when(columnFamilyFactory.createEntityCF(meta, "keyspace")).thenReturn(cfDef);

        creator.createTable(meta);

        verify(cluster).addColumnFamily(cfDef, true);

    }

    @Test
    public void should_create_column_family_for_clustered_entity() throws Exception {
        prepareData();
        meta.setClusteredEntity(true);
        idMeta.setValueClass(Long.class);

        BasicColumnFamilyDefinition cfDef = new BasicColumnFamilyDefinition();
        meta.setClassName("entity");
        when(columnFamilyFactory.createClusteredEntityCF("keyspace", meta)).thenReturn(cfDef);

        creator.createTable(meta);

        verify(cluster).addColumnFamily(cfDef, true);
    }

    @Test
    public void should_create_column_family_for_counter() throws Exception {

        Whitebox.setInternalState(creator, "cfDefs", new ArrayList<ColumnFamilyDefinition>());
        ColumnFamilyDefinition cfDef = mock(ColumnFamilyDefinition.class);
        when(keyspace.getKeyspaceName()).thenReturn("keyspace");
        when(columnFamilyFactory.createCounterCF("keyspace")).thenReturn(cfDef);

        creator.validateOrCreateTableForCounter(true);

        verify(cluster).addColumnFamily(cfDef, true);
    }

    @Test
    public void should_validate_column_family() throws Exception {
        meta = new EntityMeta();
        meta.setTableName("testCF");

        BasicColumnFamilyDefinition cfDef = new BasicColumnFamilyDefinition();
        cfDef.setName("testCF");
        Whitebox.setInternalState(creator, "cfDefs", Arrays.asList((ColumnFamilyDefinition) cfDef));

        creator.validateOrCreateTableForEntity(meta, false);
        verify(columnFamilyValidator).validateCFForEntity(cfDef, meta);
    }

    //    @Test
    //    public void should_validate_column_family_for_wide_map() throws Exception {
    //        PropertyMeta<Integer, String> wideMapMeta = PropertyMetaTestBuilder
    //                .keyValueClass(Integer.class, String.class)
    //                .field("externalWideMap")
    //                .externalTable("externalCF")
    //                .type(PropertyType.WIDE_MAP)
    //                .idClass(Long.class)
    //                .build();
    //
    //        EntityMeta meta = new EntityMeta();
    //        meta.setClassName("TestBean");
    //
    //        BasicColumnFamilyDefinition externalCFDef = new BasicColumnFamilyDefinition();
    //        externalCFDef.setName("externalCF");
    //
    //        BasicColumnFamilyDefinition cfDef = new BasicColumnFamilyDefinition();
    //        cfDef.setName("testCF");
    //
    //        Whitebox.setInternalState(creator, "cfDefs", Arrays.asList((ColumnFamilyDefinition) cfDef, externalCFDef));
    //
    //        creator.validateOrCreateTableForWideMap(meta, wideMapMeta, false);
    //        verify(columnFamilyValidator).validateWideRowForProperty(externalCFDef, wideMapMeta, "externalCF");
    //    }

    @Test
    public void should_validate_counter_column_family() throws Exception {

        ColumnFamilyDefinition cfDef = mock(ColumnFamilyDefinition.class);
        when(cfDef.getName()).thenReturn(AchillesCounter.THRIFT_COUNTER_CF);
        Whitebox.setInternalState(creator, "cfDefs", Arrays.asList(cfDef));

        when(keyspace.getKeyspaceName()).thenReturn("keyspace");

        creator.validateOrCreateTableForCounter(true);

        verify(columnFamilyValidator).validateCounterCF(cfDef);
    }

    @Test
    public void should_validate_then_create_column_family_when_not_matching() throws Exception {
        meta = new EntityMeta();
        meta.setTableName("testCF");
        meta.setClusteredEntity(false);

        BasicColumnFamilyDefinition cfDef = new BasicColumnFamilyDefinition();
        cfDef.setName("testCF2");

        Whitebox.setInternalState(creator, "cfDefs", Arrays.asList((ColumnFamilyDefinition) cfDef));
        when(columnFamilyFactory.createEntityCF(meta, "keyspace")).thenReturn(cfDef);

        creator.validateOrCreateTableForEntity(meta, true);

        verify(cluster).addColumnFamily(cfDef, true);
        assertThat(columnFamilyNames).containsOnly("testCF2");
    }

    @Test
    public void should_validate_then_create_column_family_when_null() throws Exception {
        meta = new EntityMeta();
        meta.setTableName("testCF");
        meta.setClusteredEntity(false);

        Whitebox.setInternalState(creator, "cfDefs", new ArrayList<ColumnFamilyDefinition>());
        ColumnFamilyDefinition cfDef = mock(ColumnFamilyDefinition.class);
        when(cfDef.getName()).thenReturn("mocked_cfDef");
        when(columnFamilyFactory.createEntityCF(meta, "keyspace")).thenReturn(cfDef);

        creator.validateOrCreateTableForEntity(meta, true);

        verify(cluster).addColumnFamily(cfDef, true);
        assertThat(columnFamilyNames).containsOnly("mocked_cfDef");
    }

    @Test
    public void should_exception_because_column_family_not_found() throws Exception {
        prepareData();
        Whitebox.setInternalState(creator, "cfDefs", new ArrayList<ColumnFamilyDefinition>());
        configContext.setForceColumnFamilyCreation(false);
        exception.expect(AchillesInvalidTableException.class);
        exception.expectMessage("The required column family 'testCF' does not exist for entity 'TestBean'");

        creator.validateOrCreateTableForEntity(meta, false);
    }

    @Test
    public void should_exception_when_no_column_family_for_counter() throws Exception {

        Whitebox.setInternalState(creator, "cfDefs", new ArrayList<ColumnFamilyDefinition>());
        configContext.setForceColumnFamilyCreation(false);

        exception.expect(AchillesInvalidTableException.class);
        exception.expectMessage("The required column family '" + AchillesCounter.THRIFT_COUNTER_CF
                + "' does not exist");

        creator.validateOrCreateTables(new HashMap<Class<?>, EntityMeta>(), configContext, true);

    }

    private void prepareData(PropertyMeta<?, ?>... extraPropertyMetas) throws Exception {
        Map<String, PropertyMeta<?, ?>> propertyMetas = new HashMap<String, PropertyMeta<?, ?>>();

        for (PropertyMeta<?, ?> propertyMeta : extraPropertyMetas) {
            propertyMetas.put(propertyMeta.getPropertyName(), propertyMeta);
        }

        simplePropertyMeta = PropertyMetaTestBuilder.keyValueClass(Void.class, String.class).type(SIMPLE)
                .field("name")
                .build();

        propertyMetas.put("name", simplePropertyMeta);

        meta = entityMetaBuilder(idMeta).className("TestBean").columnFamilyName("testCF")
                .propertyMetas(propertyMetas)
                .consistencyLevels(new Pair<ConsistencyLevel, ConsistencyLevel>(ONE, ONE)).build();

        entityMetaMap = new HashMap<Class<?>, EntityMeta>();
        entityMetaMap.put(this.getClass(), meta);
    }

    class TestBean {

        private Long id;

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

    }
}
