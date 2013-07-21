package info.archinnov.achilles.dao;

import static info.archinnov.achilles.serializer.ThriftSerializerUtils.COMPOSITE_SRZ;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.consistency.ThriftConsistencyLevelPolicy;
import info.archinnov.achilles.context.ConfigurationContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.UserBean;
import info.archinnov.achilles.test.parser.entity.CompoundKey;
import info.archinnov.achilles.type.Counter;
import info.archinnov.achilles.type.Pair;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.Composite;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;
import com.google.common.collect.ImmutableMap;

@RunWith(MockitoJUnitRunner.class)
public class ThriftDaoFactoryTest {

    @InjectMocks
    private ThriftDaoFactory factory;

    @Mock
    private Cluster cluster;

    @Mock
    private Keyspace keyspace;

    @Mock
    private ThriftConsistencyLevelPolicy consistencyPolicy;

    private ConfigurationContext configContext = new ConfigurationContext();

    private Map<String, ThriftGenericEntityDao> entityDaosMap = new HashMap<String, ThriftGenericEntityDao>();
    private Map<String, ThriftGenericWideRowDao> wideRowDaosMap = new HashMap<String, ThriftGenericWideRowDao>();

    @Before
    public void setUp() {
        configContext.setConsistencyPolicy(consistencyPolicy);
        entityDaosMap.clear();
        wideRowDaosMap.clear();
    }

    @Test
    public void should_create_counter_dao() throws Exception {
        ThriftCounterDao thriftCounterDao = factory.createCounterDao(cluster, keyspace, configContext);

        assertThat(thriftCounterDao).isNotNull();
        assertThat(Whitebox.getInternalState(thriftCounterDao, "policy")).isSameAs(consistencyPolicy);
        assertThat(Whitebox.getInternalState(thriftCounterDao, "cluster")).isSameAs(cluster);
        assertThat(Whitebox.getInternalState(thriftCounterDao, "keyspace")).isSameAs(keyspace);
        assertThat(Whitebox.getInternalState(thriftCounterDao, "columnNameSerializer")).isSameAs(COMPOSITE_SRZ);
        Pair<Class<Composite>, Class<Long>> rowAndValueClases = Whitebox.getInternalState(thriftCounterDao,
                "rowkeyAndValueClasses");
        assertThat(rowAndValueClases.left).isSameAs(Composite.class);
        assertThat(rowAndValueClases.right).isSameAs(Long.class);
    }

    @Test
    public void should_create_entity_dao() throws Exception {
        PropertyMeta<Void, Long> idMeta = PropertyMetaTestBuilder //
                .completeBean(Void.class, Long.class).field("id").build();

        EntityMeta entityMeta = new EntityMeta();
        entityMeta.setClusteredEntity(false);
        entityMeta.setTableName("cf");
        entityMeta.setIdMeta(idMeta);
        entityMeta.setIdClass(Long.class);
        entityMeta.setPropertyMetas(new HashMap<String, PropertyMeta<?, ?>>());

        factory.createDaosForEntity(cluster, keyspace, configContext, entityMeta, entityDaosMap, wideRowDaosMap);

        ThriftGenericEntityDao entityDao = entityDaosMap.get("cf");

        assertThat(entityDao).isNotNull();
        assertThat(entityDao.getColumnFamily()).isEqualTo("cf");
        assertThat(Whitebox.getInternalState(entityDao, "policy")).isSameAs(consistencyPolicy);
        assertThat(Whitebox.getInternalState(entityDao, "cluster")).isSameAs(cluster);
        assertThat(Whitebox.getInternalState(entityDao, "keyspace")).isSameAs(keyspace);
        assertThat(Whitebox.getInternalState(entityDao, "columnNameSerializer")).isSameAs(COMPOSITE_SRZ);

        Pair<Class<Long>, Class<String>> rowAndValueClases = Whitebox.getInternalState(entityDao,
                "rowkeyAndValueClasses");
        assertThat(rowAndValueClases.left).isSameAs(Long.class);
        assertThat(rowAndValueClases.right).isSameAs(String.class);
    }

    @Test
    public void should_create_entity_dao_with_wide_map() throws Exception {
        PropertyMeta<Void, Long> idMeta = PropertyMetaTestBuilder //
                .completeBean(Void.class, Long.class).field("id").build();

        PropertyMeta<?, ?> pm = PropertyMetaTestBuilder.noClass(Integer.class, Date.class)
                .externalTable("externalCf").type(PropertyType.WIDE_MAP).build();

        EntityMeta entityMeta = new EntityMeta();
        entityMeta.setClusteredEntity(false);
        entityMeta.setTableName("cf");
        entityMeta.setIdMeta(idMeta);
        entityMeta.setIdClass(Long.class);
        entityMeta.setPropertyMetas(ImmutableMap.<String, PropertyMeta<?, ?>> of("pm", pm));

        factory.createDaosForEntity(cluster, keyspace, configContext, entityMeta, entityDaosMap, wideRowDaosMap);

        ThriftGenericWideRowDao wideRowDao = wideRowDaosMap.get("externalCf");

        assertThat(wideRowDao).isNotNull();
        assertThat(wideRowDao.getColumnFamily()).isEqualTo("externalCf");
        assertThat(Whitebox.getInternalState(wideRowDao, "policy")).isSameAs(consistencyPolicy);
        assertThat(Whitebox.getInternalState(wideRowDao, "cluster")).isSameAs(cluster);
        assertThat(Whitebox.getInternalState(wideRowDao, "keyspace")).isSameAs(keyspace);
        assertThat(Whitebox.getInternalState(wideRowDao, "columnNameSerializer")).isSameAs(COMPOSITE_SRZ);

        Pair<Class<Long>, Class<Date>> rowAndValueClases = Whitebox.getInternalState(wideRowDao,
                "rowkeyAndValueClasses");
        assertThat(rowAndValueClases.left).isSameAs(Long.class);
        assertThat(rowAndValueClases.right).isSameAs(Date.class);
    }

    @Test
    public void should_create_entity_dao_with_counter_wide_map() throws Exception {
        PropertyMeta<Void, Integer> idMeta = PropertyMetaTestBuilder //
                .valueClass(Integer.class).build();

        PropertyMeta<?, ?> pm = PropertyMetaTestBuilder.noClass(Integer.class, Counter.class)
                .externalTable("externalCf").type(PropertyType.COUNTER_WIDE_MAP).build();

        EntityMeta entityMeta = new EntityMeta();
        entityMeta.setClusteredEntity(false);
        entityMeta.setTableName("cf");
        entityMeta.setIdMeta(idMeta);
        entityMeta.setIdClass(Integer.class);
        entityMeta.setPropertyMetas(ImmutableMap.<String, PropertyMeta<?, ?>> of("pm", pm));

        factory.createDaosForEntity(cluster, keyspace, configContext, entityMeta, entityDaosMap, wideRowDaosMap);

        ThriftGenericWideRowDao wideRowDao = wideRowDaosMap.get("externalCf");

        Pair<Class<Integer>, Class<Long>> rowAndValueClases = Whitebox.getInternalState(wideRowDao,
                "rowkeyAndValueClasses");
        assertThat(rowAndValueClases.left).isSameAs(Integer.class);
        assertThat(rowAndValueClases.right).isSameAs(Long.class);
    }

    @Test
    public void should_create_entity_dao_with_object_type_wide_map() throws Exception {
        PropertyMeta<Void, Integer> idMeta = PropertyMetaTestBuilder //
                .valueClass(Integer.class).build();

        PropertyMeta<?, ?> pm = PropertyMetaTestBuilder.noClass(Integer.class, UserBean.class)
                .externalTable("externalCf").type(PropertyType.WIDE_MAP).build();

        EntityMeta entityMeta = new EntityMeta();
        entityMeta.setClusteredEntity(false);
        entityMeta.setTableName("cf");
        entityMeta.setIdMeta(idMeta);
        entityMeta.setIdClass(Integer.class);
        entityMeta.setPropertyMetas(ImmutableMap.<String, PropertyMeta<?, ?>> of("pm", pm));

        factory.createDaosForEntity(cluster, keyspace, configContext, entityMeta, entityDaosMap, wideRowDaosMap);

        ThriftGenericWideRowDao wideRowDao = wideRowDaosMap.get("externalCf");

        Pair<Class<Integer>, Class<String>> rowAndValueClases = Whitebox.getInternalState(wideRowDao,
                "rowkeyAndValueClasses");
        assertThat(rowAndValueClases.left).isSameAs(Integer.class);
        assertThat(rowAndValueClases.right).isSameAs(String.class);
    }

    @Test
    public void should_create_entity_dao_with_join_wide_map() throws Exception {
        PropertyMeta<Void, Integer> idMeta = PropertyMetaTestBuilder //
                .valueClass(Integer.class).build();

        PropertyMeta<?, ?> joinIdMeta = PropertyMetaTestBuilder.valueClass(UUID.class).build();

        EntityMeta joinMeta = new EntityMeta();
        joinMeta.setIdMeta(joinIdMeta);

        PropertyMeta<?, ?> pm = PropertyMetaTestBuilder.noClass(Integer.class, UserBean.class)
                .externalTable("externalCf").type(PropertyType.JOIN_WIDE_MAP).joinMeta(joinMeta).build();

        EntityMeta entityMeta = new EntityMeta();
        entityMeta.setClusteredEntity(false);
        entityMeta.setTableName("cf");
        entityMeta.setIdMeta(idMeta);
        entityMeta.setIdClass(Integer.class);
        entityMeta.setPropertyMetas(ImmutableMap.<String, PropertyMeta<?, ?>> of("pm", pm));

        factory.createDaosForEntity(cluster, keyspace, configContext, entityMeta, entityDaosMap, wideRowDaosMap);

        ThriftGenericWideRowDao wideRowDao = wideRowDaosMap.get("externalCf");

        Pair<Class<Integer>, Class<UUID>> rowAndValueClases = Whitebox.getInternalState(wideRowDao,
                "rowkeyAndValueClasses");
        assertThat(rowAndValueClases.left).isSameAs(Integer.class);
        assertThat(rowAndValueClases.right).isSameAs(UUID.class);
    }

    @Test
    public void should_create_clustered_entity_dao() throws Exception {

        PropertyMeta<?, ?> idMeta = PropertyMetaTestBuilder.valueClass(CompoundKey.class)
                .compClasses(Long.class, String.class, UUID.class).build();

        PropertyMeta<?, ?> pm = PropertyMetaTestBuilder.valueClass(Date.class).type(PropertyType.SIMPLE).build();

        EntityMeta entityMeta = new EntityMeta();
        entityMeta.setClusteredEntity(true);
        entityMeta.setIdMeta(idMeta);
        entityMeta.setTableName("cf");
        entityMeta.setPropertyMetas(ImmutableMap.<String, PropertyMeta<?, ?>> of("pm", pm));

        factory.createClusteredEntityDao(cluster, keyspace, configContext, entityMeta, wideRowDaosMap);

        ThriftGenericWideRowDao clusteredEntityDao = wideRowDaosMap.get("cf");

        assertThat(clusteredEntityDao).isNotNull();
        assertThat(clusteredEntityDao.getColumnFamily()).isEqualTo("cf");
        assertThat(Whitebox.getInternalState(clusteredEntityDao, "policy")).isSameAs(consistencyPolicy);
        assertThat(Whitebox.getInternalState(clusteredEntityDao, "cluster")).isSameAs(cluster);
        assertThat(Whitebox.getInternalState(clusteredEntityDao, "keyspace")).isSameAs(keyspace);
        assertThat(Whitebox.getInternalState(clusteredEntityDao, "columnNameSerializer")).isSameAs(COMPOSITE_SRZ);

        Pair<Class<Long>, Class<Date>> rowAndValueClases = Whitebox.getInternalState(clusteredEntityDao,
                "rowkeyAndValueClasses");
        assertThat(rowAndValueClases.left).isSameAs(Long.class);
        assertThat(rowAndValueClases.right).isSameAs(Date.class);
    }

    @Test
    public void should_create_counter_clustered_entity_dao() throws Exception {

        PropertyMeta<?, ?> idMeta = PropertyMetaTestBuilder.valueClass(CompoundKey.class)
                .compClasses(Integer.class, String.class, UUID.class).build();

        PropertyMeta<?, ?> pm = PropertyMetaTestBuilder.valueClass(Counter.class).type(PropertyType.COUNTER).build();

        EntityMeta entityMeta = new EntityMeta();
        entityMeta.setClusteredEntity(true);
        entityMeta.setIdMeta(idMeta);
        entityMeta.setTableName("cf");
        entityMeta.setPropertyMetas(ImmutableMap.<String, PropertyMeta<?, ?>> of("pm", pm));

        factory.createClusteredEntityDao(cluster, keyspace, configContext, entityMeta, wideRowDaosMap);

        ThriftGenericWideRowDao clusteredEntityDao = wideRowDaosMap.get("cf");

        Pair<Class<Integer>, Class<Long>> rowAndValueClases = Whitebox.getInternalState(clusteredEntityDao,
                "rowkeyAndValueClasses");
        assertThat(rowAndValueClases.left).isSameAs(Integer.class);
        assertThat(rowAndValueClases.right).isSameAs(Long.class);
    }

    @Test
    public void should_create_object_type_clustered_entity_dao() throws Exception {

        PropertyMeta<?, ?> idMeta = PropertyMetaTestBuilder.valueClass(CompoundKey.class)
                .compClasses(Integer.class, String.class, UUID.class).build();

        PropertyMeta<?, ?> pm = PropertyMetaTestBuilder.valueClass(UserBean.class).type(PropertyType.SIMPLE).build();

        EntityMeta entityMeta = new EntityMeta();
        entityMeta.setClusteredEntity(true);
        entityMeta.setIdMeta(idMeta);
        entityMeta.setTableName("cf");
        entityMeta.setPropertyMetas(ImmutableMap.<String, PropertyMeta<?, ?>> of("pm", pm));

        factory.createClusteredEntityDao(cluster, keyspace, configContext, entityMeta, wideRowDaosMap);

        ThriftGenericWideRowDao clusteredEntityDao = wideRowDaosMap.get("cf");

        Pair<Class<Integer>, Class<String>> rowAndValueClases = Whitebox.getInternalState(clusteredEntityDao,
                "rowkeyAndValueClasses");
        assertThat(rowAndValueClases.left).isSameAs(Integer.class);
        assertThat(rowAndValueClases.right).isSameAs(String.class);
    }

    @Test
    public void should_create_join_clustered_entity_dao() throws Exception {

        PropertyMeta<?, ?> idMeta = PropertyMetaTestBuilder.valueClass(CompoundKey.class)
                .compClasses(Integer.class, String.class, Long.class).build();

        PropertyMeta<?, ?> joinIdMeta = PropertyMetaTestBuilder.valueClass(UUID.class).build();

        EntityMeta joinMeta = new EntityMeta();
        joinMeta.setIdMeta(joinIdMeta);

        PropertyMeta<?, ?> pm = PropertyMetaTestBuilder.valueClass(UserBean.class).type(PropertyType.JOIN_SIMPLE)
                .joinMeta(joinMeta).build();

        EntityMeta entityMeta = new EntityMeta();
        entityMeta.setClusteredEntity(true);
        entityMeta.setIdMeta(idMeta);
        entityMeta.setTableName("cf");
        entityMeta.setPropertyMetas(ImmutableMap.<String, PropertyMeta<?, ?>> of("pm", pm));

        factory.createClusteredEntityDao(cluster, keyspace, configContext, entityMeta, wideRowDaosMap);

        ThriftGenericWideRowDao clusteredEntityDao = wideRowDaosMap.get("cf");

        Pair<Class<Integer>, Class<UUID>> rowAndValueClases = Whitebox.getInternalState(clusteredEntityDao,
                "rowkeyAndValueClasses");
        assertThat(rowAndValueClases.left).isSameAs(Integer.class);
        assertThat(rowAndValueClases.right).isSameAs(UUID.class);
    }
}
