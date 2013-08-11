package info.archinnov.achilles.entity.operations;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.composite.ThriftCompositeFactory;
import info.archinnov.achilles.consistency.ThriftConsistencyLevelPolicy;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.dao.ThriftCounterDao;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.entity.context.ThriftPersistenceContextTestBuilder;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.operations.impl.ThriftJoinLoaderImpl;
import info.archinnov.achilles.entity.operations.impl.ThriftLoaderImpl;
import info.archinnov.achilles.helper.EntityMapper;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.test.builders.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import me.prettyprint.cassandra.model.ExecutingKeyspace;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

/**
 * ThriftEntityLoaderTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class ThriftEntityLoaderTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @InjectMocks
    private ThriftEntityLoader loader;

    @Mock
    private ExecutingKeyspace keyspace;

    @Mock
    private EntityMeta entityMeta;

    @Mock
    private PropertyMeta idMeta;

    @Mock
    private PropertyMeta propertyMeta;

    @Mock
    private EntityMeta joinMeta;

    @Mock
    private PropertyMeta joinIdMeta;

    @Mock
    private EntityMapper mapper;

    @Mock
    private ThriftGenericEntityDao dao;

    @Mock
    private ThriftCounterDao thriftCounterDao;

    @Mock
    private ThriftCompositeFactory thriftCompositeFactory;

    @Mock
    private ReflectionInvoker invoker;

    @Mock
    private ThriftJoinLoaderImpl joinLoaderImpl;

    @Mock
    private ThriftLoaderImpl loaderImpl;

    @Mock
    private ThriftConsistencyLevelPolicy policy;

    @Captor
    ArgumentCaptor<Long> idCaptor;

    private CompleteBean bean = CompleteBeanTestBuilder.builder().randomId().buid();

    private ThriftPersistenceContext context;

    @Before
    public void setUp() throws Exception
    {
        context = ThriftPersistenceContextTestBuilder
                .context(entityMeta, thriftCounterDao, policy, CompleteBean.class, bean.getId())
                .entity(bean)
                .build();
    }

    @Test
    public void should_load_entity() throws Exception
    {
        when(entityMeta.isClusteredEntity()).thenReturn(false);
        when(loaderImpl.load(context, CompleteBean.class)).thenReturn(bean);

        Object actual = loader.load(context, CompleteBean.class);

        assertThat(actual).isSameAs(bean);
    }

    @Test
    public void should_not_load_entity() throws Exception
    {
        Method idSetter = CompleteBean.class.getDeclaredMethod("setId", Long.class);
        when(entityMeta.getIdMeta()).thenReturn(idMeta);
        when(idMeta.getSetter()).thenReturn(idSetter);

        when(entityMeta.isClusteredEntity()).thenReturn(false);
        when(loaderImpl.load(context, CompleteBean.class)).thenReturn(bean);
        context.setLoadEagerFields(false);

        Object actual = loader.load(context, CompleteBean.class);

        assertThat(actual).isNotSameAs(bean);

        verify(invoker).setValueToField(any(CompleteBean.class), eq(idSetter), eq(bean.getId()));
        verifyZeroInteractions(loaderImpl);

    }

    @Test
    public void should_load_simple() throws Exception
    {
        String value = "val";
        Method setter = prepareSetter();
        when(propertyMeta.type()).thenReturn(PropertyType.SIMPLE);
        when(loaderImpl.loadSimpleProperty(context, propertyMeta)).thenReturn(value);

        loader.loadPropertyIntoObject(context, bean, propertyMeta);

        verify(invoker).setValueToField(bean, setter, value);
    }

    @Test
    public void should_load_lazy_simple() throws Exception
    {
        String value = "val";
        Method setter = prepareSetter();
        when(propertyMeta.type()).thenReturn(PropertyType.LAZY_SIMPLE);
        when(loaderImpl.loadSimpleProperty(context, propertyMeta)).thenReturn(value);

        loader.loadPropertyIntoObject(context, bean, propertyMeta);

        verify(invoker).setValueToField(bean, setter, value);
    }

    @Test
    public void should_load_list() throws Exception
    {
        List<Object> value = Arrays.<Object> asList("val");
        Method setter = prepareSetter();
        when(propertyMeta.type()).thenReturn(PropertyType.LIST);
        when(loaderImpl.loadListProperty(context, propertyMeta)).thenReturn(value);

        loader.loadPropertyIntoObject(context, bean, propertyMeta);

        verify(invoker).setValueToField(bean, setter, value);
    }

    @Test
    public void should_load_lazy_list() throws Exception
    {
        List<Object> value = Arrays.<Object> asList("val");
        Method setter = prepareSetter();
        when(propertyMeta.type()).thenReturn(PropertyType.LAZY_LIST);
        when(loaderImpl.loadListProperty(context, propertyMeta)).thenReturn(value);

        loader.loadPropertyIntoObject(context, bean, propertyMeta);

        verify(invoker).setValueToField(bean, setter, value);
    }

    @Test
    public void should_load_set() throws Exception
    {
        Set<Object> value = Sets.<Object> newHashSet("val");
        Method setter = prepareSetter();
        when(propertyMeta.type()).thenReturn(PropertyType.SET);
        when(loaderImpl.loadSetProperty(context, propertyMeta)).thenReturn(value);

        loader.loadPropertyIntoObject(context, bean, propertyMeta);

        verify(invoker).setValueToField(bean, setter, value);
    }

    @Test
    public void should_load_lazy_set() throws Exception
    {
        Set<Object> value = Sets.<Object> newHashSet("val");
        Method setter = prepareSetter();
        when(propertyMeta.type()).thenReturn(PropertyType.LAZY_SET);
        when(loaderImpl.loadSetProperty(context, propertyMeta)).thenReturn(value);

        loader.loadPropertyIntoObject(context, bean, propertyMeta);

        verify(invoker).setValueToField(bean, setter, value);
    }

    @Test
    public void should_load_map() throws Exception
    {
        Map<Object, Object> value = ImmutableMap.<Object, Object> of(11, "val");

        Method setter = prepareSetter();
        when(propertyMeta.type()).thenReturn(PropertyType.MAP);
        when(loaderImpl.loadMapProperty(context, propertyMeta)).thenReturn(value);

        loader.loadPropertyIntoObject(context, bean, propertyMeta);

        verify(invoker).setValueToField(bean, setter, value);
    }

    @Test
    public void should_load_lazy_map() throws Exception
    {
        Map<Object, Object> value = ImmutableMap.<Object, Object> of(11, "val");

        Method setter = prepareSetter();
        when(propertyMeta.type()).thenReturn(PropertyType.LAZY_MAP);
        when(loaderImpl.loadMapProperty(context, propertyMeta)).thenReturn(
                value);

        loader.loadPropertyIntoObject(context, bean, propertyMeta);

        verify(invoker).setValueToField(bean, setter, value);
    }

    @Test
    public void should_load_join_simple() throws Exception
    {
        String value = "val";

        Method setter = prepareSetter();
        when(propertyMeta.type()).thenReturn(PropertyType.JOIN_SIMPLE);
        when(loaderImpl.loadJoinSimple(context, propertyMeta, loader)).thenReturn(value);

        loader.loadPropertyIntoObject(context, bean, propertyMeta);

        verify(invoker).setValueToField(bean, setter, value);
    }

    @Test
    public void should_load_join_list() throws Exception
    {
        List<Object> value = Arrays.<Object> asList("val");

        Method setter = prepareSetter();
        when(propertyMeta.type()).thenReturn(PropertyType.JOIN_LIST);
        when(joinLoaderImpl.loadJoinListProperty(context, propertyMeta)).thenReturn(value);

        loader.loadPropertyIntoObject(context, bean, propertyMeta);

        verify(invoker).setValueToField(bean, setter, value);
    }

    @Test
    public void should_load_join_set() throws Exception
    {
        Set<Object> value = Sets.<Object> newHashSet("val");

        Method setter = prepareSetter();
        when(propertyMeta.type()).thenReturn(PropertyType.JOIN_SET);
        when(joinLoaderImpl.loadJoinSetProperty(context, propertyMeta)).thenReturn(value);

        loader.loadPropertyIntoObject(context, bean, propertyMeta);

        verify(invoker).setValueToField(bean, setter, value);
    }

    @Test
    public void should_load_join_map() throws Exception
    {
        Map<Object, Object> value = ImmutableMap.<Object, Object> of(11, "val");

        Method setter = prepareSetter();
        when(propertyMeta.type()).thenReturn(PropertyType.JOIN_MAP);
        when(joinLoaderImpl.loadJoinMapProperty(context, propertyMeta))
                .thenReturn(value);

        loader.loadPropertyIntoObject(context, bean, propertyMeta);

        verify(invoker).setValueToField(bean, setter, value);
    }

    @Test
    public void should_not_load() throws Exception
    {
        when(propertyMeta.type()).thenReturn(PropertyType.COUNTER);
        loader.loadPropertyIntoObject(context, bean, propertyMeta);

        verifyZeroInteractions(loaderImpl, joinLoaderImpl, invoker);
    }

    @Test
    public void should_load_primary_key() throws Exception
    {
        loader.loadPrimaryKey(context, propertyMeta);

        verify(loaderImpl).loadSimpleProperty(context, propertyMeta);
    }

    private Method prepareSetter() throws Exception
    {
        PropertyMeta tempMeta = PropertyMetaTestBuilder
                .completeBean(Void.class, String.class)
                .field("name")
                .accessors()
                .build();

        when(propertyMeta.getSetter()).thenReturn(tempMeta.getSetter());

        return tempMeta.getSetter();
    }
}
