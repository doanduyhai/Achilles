package info.archinnov.achilles.context;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.consistency.ThriftConsistencyLevelPolicy;
import info.archinnov.achilles.dao.ThriftCounterDao;
import info.archinnov.achilles.dao.ThriftDaoFactory;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import java.util.HashMap;
import java.util.Map;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * ThriftDaoContextBuilderTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class ThriftDaoContextBuilderTest {

    @InjectMocks
    private ThriftDaoContextBuilder builder;

    @Mock
    private ThriftDaoFactory daoFactory;

    @Mock
    private Cluster cluster;

    @Mock
    private Keyspace keyspace;

    @Mock
    private ThriftConsistencyLevelPolicy consistencyPolicy;

    @Mock
    private ThriftCounterDao counterDao;

    private ConfigurationContext configContext = new ConfigurationContext();

    private Map<Class<?>, EntityMeta> entityMetaMap = new HashMap<Class<?>, EntityMeta>();

    @Before
    public void setUp() {
        configContext.setConsistencyPolicy(consistencyPolicy);
        entityMetaMap.clear();
    }

    @Test
    public void should_build_counter_dao() throws Exception {
        when(daoFactory.createCounterDao(cluster, keyspace, configContext)).thenReturn(counterDao);
        ThriftDaoContext context = builder.buildDao(cluster, keyspace, entityMetaMap, configContext, true);

        ThriftCounterDao counterDao = context.getCounterDao();
        assertThat(counterDao).isSameAs(this.counterDao);
    }

    @Test
    public void should_build_entity_dao() throws Exception {
        PropertyMeta<Void, Long> idMeta = PropertyMetaTestBuilder //
                .completeBean(Void.class, Long.class).field("id").build();

        EntityMeta entityMeta = new EntityMeta();
        entityMeta.setClusteredEntity(false);
        entityMeta.setTableName("cf");
        entityMeta.setIdMeta(idMeta);
        entityMeta.setIdClass(Long.class);
        entityMeta.setPropertyMetas(new HashMap<String, PropertyMeta<?, ?>>());

        entityMetaMap.put(CompleteBean.class, entityMeta);

        builder.buildDao(cluster, keyspace, entityMetaMap, configContext, false);
        verify(daoFactory).createDaosForEntity(eq(cluster), eq(keyspace), eq(configContext), eq(entityMeta),
                any(Map.class), any(Map.class));

    }

    @Test
    public void should_build_clustered_entity_dao() throws Exception {
        PropertyMeta<Void, Long> idMeta = PropertyMetaTestBuilder //
                .completeBean(Void.class, Long.class).field("id").build();

        EntityMeta entityMeta = new EntityMeta();
        entityMeta.setClusteredEntity(true);
        entityMeta.setTableName("cf");
        entityMeta.setIdMeta(idMeta);
        entityMeta.setIdClass(Long.class);
        entityMeta.setPropertyMetas(new HashMap<String, PropertyMeta<?, ?>>());

        entityMetaMap.put(CompleteBean.class, entityMeta);

        builder.buildDao(cluster, keyspace, entityMetaMap, configContext, false);
        verify(daoFactory).createClusteredEntityDao(eq(cluster), eq(keyspace), eq(configContext), eq(entityMeta),
                any(Map.class));

    }

}
