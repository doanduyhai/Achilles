package info.archinnov.achilles.entity.operations;

import static info.archinnov.achilles.entity.metadata.PropertyType.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.clustered.ClusteredEntityFactory;
import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.context.ConfigurationContext;
import info.archinnov.achilles.context.ThriftDaoContext;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.impl.ThriftQueryExecutorImpl;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.iterator.ThriftClusteredEntityIterator;
import info.archinnov.achilles.iterator.ThriftCounterClusteredEntityIterator;
import info.archinnov.achilles.iterator.ThriftCounterSliceIterator;
import info.archinnov.achilles.iterator.ThriftJoinSliceIterator;
import info.archinnov.achilles.iterator.ThriftSliceIterator;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.query.SliceQuery;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.parser.entity.BeanWithClusteredId;
import info.archinnov.achilles.test.parser.entity.CompoundKey;
import info.archinnov.achilles.type.ConsistencyLevel;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HCounterColumn;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;
import com.google.common.collect.ImmutableMap;

/**
 * ThriftQueryExecutorTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class ThriftSliceQueryExecutorTest
{
    private ThriftSliceQueryExecutor executor;

    @Mock
    private ConfigurationContext configContext;

    @Mock
    private ThriftDaoContext daoContext;

    @Mock
    private AchillesConsistencyLevelPolicy consistencyPolicy;

    @Mock
    private ClusteredEntityFactory factory;

    @Mock
    private ThriftEntityProxifier proxifier;

    @Mock
    private ReflectionInvoker invoker;

    @Mock
    private ThriftQueryExecutorImpl executorImpl;

    @Mock
    private SliceQuery<BeanWithClusteredId> query;

    private PropertyMeta<?, ?> idMeta;

    @Mock
    private PropertyMeta<?, ?> pm;

    private EntityMeta meta;

    private BeanWithClusteredId entity;

    private Class<BeanWithClusteredId> entityClass = BeanWithClusteredId.class;

    private Long partitionKey = RandomUtils.nextLong();

    private ConsistencyLevel consistencyLevel = ConsistencyLevel.ALL;

    private CompoundKey compoundKey = new CompoundKey(partitionKey, "name");

    @Before
    public void setUp() throws Exception
    {
        when(configContext.getConsistencyPolicy()).thenReturn(consistencyPolicy);
        when(consistencyPolicy.getDefaultGlobalReadConsistencyLevel()).thenReturn(ConsistencyLevel.EACH_QUORUM);

        executor = new ThriftSliceQueryExecutor(configContext, daoContext);

        Whitebox.setInternalState(executor, ClusteredEntityFactory.class, factory);
        Whitebox.setInternalState(executor, ThriftEntityProxifier.class, proxifier);
        Whitebox.setInternalState(executor, ReflectionInvoker.class, invoker);
        Whitebox.setInternalState(executor, ThriftQueryExecutorImpl.class, executorImpl);

        entity = new BeanWithClusteredId();
        entity.setId(compoundKey);
        Method idGetter = BeanWithClusteredId.class.getDeclaredMethod("getId");

        idMeta = PropertyMetaTestBuilder
                .valueClass(CompoundKey.class)
                .build();
        idMeta.setGetter(idGetter);

        meta = new EntityMeta();
        meta.setIdMeta(idMeta);
        meta.setPropertyMetas(ImmutableMap.<String, PropertyMeta<?, ?>> of("pm", pm));

        when(query.getMeta()).thenReturn(meta);
        when(query.getConsistencyLevel()).thenReturn(consistencyLevel);
        when(query.getPartitionKey()).thenReturn(partitionKey);

        when(query.getEntityClass()).thenReturn(entityClass);

        Method valueGetter = BeanWithClusteredId.class.getDeclaredMethod("getName");
        when(pm.getGetter()).thenReturn(valueGetter);
        when(invoker.instanciateEmbeddedIdWithPartitionKey(idMeta, partitionKey)).thenReturn(compoundKey);
        when(invoker.getPrimaryKey(entity, idMeta)).thenReturn(compoundKey);

    }

    @Test
    public void should_get_clustered_entities() throws Exception
    {
        when(pm.type()).thenReturn(SIMPLE);

        List<HColumn<Composite, Object>> columns = new ArrayList<HColumn<Composite, Object>>();

        when(executorImpl.findColumns(eq(query), any(ThriftPersistenceContext.class))).thenReturn(
                columns);

        List<BeanWithClusteredId> entities = Arrays.asList(entity);
        when(factory.buildClusteredEntities(eq(entityClass), any(ThriftPersistenceContext.class),
                eq(columns))).thenReturn(entities);

        when(proxifier.buildProxy(eq(entity), any(ThriftPersistenceContext.class), any(Set.class)))
                .thenReturn(entity);
        when(invoker.getPrimaryKey(entity, idMeta)).thenReturn(compoundKey);
        List<BeanWithClusteredId> actual = executor.get(query);

        assertThat(actual).containsOnly(entity);
    }

    @Test
    public void should_get_join_clustered_entities() throws Exception
    {
        when(pm.type()).thenReturn(JOIN_SIMPLE);

        List<HColumn<Composite, Object>> columns = new ArrayList<HColumn<Composite, Object>>();

        when(executorImpl.findColumns(eq(query), any(ThriftPersistenceContext.class))).thenReturn(
                columns);

        List<BeanWithClusteredId> entities = Arrays.asList(entity);
        when(factory.buildClusteredEntities(eq(entityClass), any(ThriftPersistenceContext.class),
                eq(columns))).thenReturn(entities);

        when(proxifier.buildProxy(eq(entity), any(ThriftPersistenceContext.class), any(Set.class)))
                .thenReturn(entity);
        when(invoker.getPrimaryKey(entity, idMeta)).thenReturn(compoundKey);
        List<BeanWithClusteredId> actual = executor.get(query);

        assertThat(actual).containsOnly(entity);
    }

    @Test
    public void should_get_counter_clustered_entities() throws Exception
    {
        when(pm.type()).thenReturn(COUNTER);

        List<HCounterColumn<Composite>> columns = new ArrayList<HCounterColumn<Composite>>();

        when(executorImpl.findCounterColumns(eq(query), any(ThriftPersistenceContext.class)))
                .thenReturn(columns);

        List<BeanWithClusteredId> entities = Arrays.asList(entity);
        when(factory.buildCounterClusteredEntities(eq(entityClass),
                any(ThriftPersistenceContext.class), eq(columns))).thenReturn(entities);

        when(proxifier.buildProxy(eq(entity), any(ThriftPersistenceContext.class), any(Set.class)))
                .thenReturn(entity);
        when(invoker.getPrimaryKey(entity, idMeta)).thenReturn(compoundKey);
        List<BeanWithClusteredId> actual = executor.get(query);

        assertThat(actual).containsOnly(entity);
    }

    @Test(expected = AchillesException.class)
    public void should_exception_when_get_clustered_entities_of_wrong_value_type() throws Exception
    {
        when(pm.type()).thenReturn(WIDE_MAP);

        executor.get(query);

    }

    @Test
    public void should_get_iterator() throws Exception
    {
        when(pm.type()).thenReturn(SIMPLE);

        ThriftSliceIterator<Object, Object> columnsIterator = mock(ThriftSliceIterator.class);

        when(executorImpl.getColumnsIterator(eq(query), any(ThriftPersistenceContext.class)))
                .thenReturn(columnsIterator);

        Iterator<BeanWithClusteredId> iterator = executor.iterator(query);

        assertThat(iterator).isInstanceOf(ThriftClusteredEntityIterator.class);
    }

    @Test
    public void should_get_join_iterator() throws Exception
    {
        when(pm.type()).thenReturn(JOIN_SIMPLE);

        ThriftJoinSliceIterator<Object, Object, Object> columnsIterator = mock(ThriftJoinSliceIterator.class);

        when(executorImpl.getJoinColumnsIterator(eq(query), any(ThriftPersistenceContext.class)))
                .thenReturn(columnsIterator);

        Iterator<BeanWithClusteredId> iterator = executor.iterator(query);

        assertThat(iterator).isInstanceOf(ThriftClusteredEntityIterator.class);
    }

    @Test
    public void should_get_counter_iterator() throws Exception
    {
        when(pm.type()).thenReturn(COUNTER);

        ThriftCounterSliceIterator<Object> columnsIterator = mock(ThriftCounterSliceIterator.class);

        when(executorImpl.getCounterColumnsIterator(eq(query), any(ThriftPersistenceContext.class)))
                .thenReturn(columnsIterator);

        Iterator<BeanWithClusteredId> iterator = executor.iterator(query);

        assertThat(iterator).isInstanceOf(ThriftCounterClusteredEntityIterator.class);
    }

    @Test(expected = AchillesException.class)
    public void should_exception_when_get_iterator_on_wrong_valuetype() throws Exception
    {
        when(pm.type()).thenReturn(WIDE_MAP);

        executor.iterator(query);

    }

    @Test
    public void should_remove_columns() throws Exception
    {
        when(pm.type()).thenReturn(SIMPLE);

        List<HColumn<Composite, Object>> columns = new ArrayList<HColumn<Composite, Object>>();

        when(executorImpl.findColumns(eq(query), any(ThriftPersistenceContext.class))).thenReturn(
                columns);

        executor.remove(query);

        verify(executorImpl).removeColumns(eq(columns), eq(consistencyLevel),
                any(ThriftPersistenceContext.class));
    }

    @Test
    public void should_remove_join_columns() throws Exception
    {
        when(pm.type()).thenReturn(JOIN_SIMPLE);

        List<HColumn<Composite, Object>> columns = new ArrayList<HColumn<Composite, Object>>();

        when(executorImpl.findColumns(eq(query), any(ThriftPersistenceContext.class))).thenReturn(
                columns);

        executor.remove(query);

        verify(executorImpl).removeColumns(eq(columns), eq(consistencyLevel),
                any(ThriftPersistenceContext.class));
    }

    @Test
    public void should_remove_counter_columns() throws Exception
    {
        when(pm.type()).thenReturn(COUNTER);

        List<HCounterColumn<Composite>> columns = new ArrayList<HCounterColumn<Composite>>();

        when(executorImpl.findCounterColumns(eq(query), any(ThriftPersistenceContext.class)))
                .thenReturn(columns);

        executor.remove(query);

        verify(executorImpl).removeCounterColumns(eq(columns), eq(consistencyLevel),
                any(ThriftPersistenceContext.class));
    }

    @Test(expected = AchillesException.class)
    public void should_exception_when_remove_clustered_entities_of_wrong_value_type()
            throws Exception
    {
        when(pm.type()).thenReturn(WIDE_MAP);

        executor.remove(query);

    }
}
