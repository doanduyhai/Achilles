package info.archinnov.achilles.entity.operations;

import static info.archinnov.achilles.entity.metadata.PropertyType.COUNTER;
import static info.archinnov.achilles.entity.metadata.PropertyType.LAZY_LIST;
import static info.archinnov.achilles.entity.metadata.PropertyType.LAZY_SET;
import static info.archinnov.achilles.entity.metadata.PropertyType.SIMPLE;
import static org.fest.assertions.api.Assertions.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.proxy.CQLEntityInterceptor;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.type.Counter;
import info.archinnov.achilles.type.CounterBuilder;

@RunWith(MockitoJUnitRunner.class)
public class CQLEntityInitializerTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @InjectMocks
    private CQLEntityInitializer initializer = new CQLEntityInitializer();

    @Mock
    private CQLEntityProxifier proxifier;

    @Mock
    private ReflectionInvoker invoker = new ReflectionInvoker();

    @Mock
    private CQLEntityInterceptor<CompleteBean> interceptor;

    private CompleteBean bean = new CompleteBean();


    @Test
    public void should_initialize_entity() throws Exception {

        Class<? extends CompleteBean> beanClass = bean.getClass();

        PropertyMeta nameMeta = new PropertyMeta();
        nameMeta.setEntityClassName("beanClass");
        nameMeta.setType(SIMPLE);
        nameMeta.setGetter(beanClass.getMethod("getName"));

        PropertyMeta friendsMeta = new PropertyMeta();
        friendsMeta.setEntityClassName("beanClass");
        friendsMeta.setType(LAZY_LIST);
        friendsMeta.setGetter(beanClass.getMethod("getFriends"));

        PropertyMeta followersMeta = new PropertyMeta();
        followersMeta.setEntityClassName("beanClass");
        followersMeta.setType(LAZY_SET);
        followersMeta.setGetter(beanClass.getMethod("getFollowers"));
        followersMeta.setInvoker(invoker);

        Set<Method> alreadyLoaded = Sets.newHashSet(friendsMeta.getGetter(), nameMeta.getGetter());

        Map<Method, PropertyMeta> getterMetas = ImmutableMap.<Method, PropertyMeta> of(nameMeta.getGetter(), nameMeta,
                                                                                       friendsMeta
                                                                                               .getGetter(), friendsMeta, followersMeta
                                                                                               .getGetter(), followersMeta);

        Map<String, PropertyMeta> allMetas = ImmutableMap.<String, PropertyMeta> of("name", nameMeta, "friends",
                                                                                    friendsMeta, "followers", followersMeta);

        EntityMeta entityMeta = new EntityMeta();
        entityMeta.setPropertyMetas(allMetas);
        entityMeta.setGetterMetas(getterMetas);

        when(interceptor.getAlreadyLoaded()).thenReturn(alreadyLoaded);

        initializer.initializeEntity(bean, entityMeta, interceptor);

        verify(invoker).getValueFromField(bean, followersMeta.getGetter());
    }

    @Test
    public void should_initialize_and_set_counter_value_for_entity() throws Exception {
        Class<? extends CompleteBean> beanClass = bean.getClass();

        PropertyMeta counterMeta = new PropertyMeta();
        counterMeta.setEntityClassName("beanClass");
        counterMeta.setType(COUNTER);
        counterMeta.setGetter(beanClass.getMethod("getCount"));
        counterMeta.setSetter(beanClass.getMethod("setCount", Counter.class));
        counterMeta.setInvoker(invoker);

        Set<Method> alreadyLoaded = Sets.newHashSet();

        Map<Method, PropertyMeta> getterMetas = ImmutableMap.<Method, PropertyMeta> of(counterMeta.getGetter(),
                                                                                       counterMeta);

        Map<String, PropertyMeta> allMetas = ImmutableMap.<String, PropertyMeta> of("count", counterMeta);

        EntityMeta entityMeta = new EntityMeta();
        entityMeta.setPropertyMetas(allMetas);
        entityMeta.setGetterMetas(getterMetas);

        when(interceptor.getAlreadyLoaded()).thenReturn(alreadyLoaded);
        when(invoker.getValueFromField(bean, counterMeta.getGetter())).thenReturn(CounterBuilder.incr(10L));
        when(proxifier.getRealObject(bean)).thenReturn(bean);

        initializer.initializeEntity(bean, entityMeta, interceptor);

        ArgumentCaptor<Counter> counterCaptor = ArgumentCaptor.forClass(Counter.class);

        verify(invoker).setValueToField(eq(bean), eq(counterMeta.getSetter()), counterCaptor.capture());

        assertThat(counterCaptor.getValue().get()).isEqualTo(10L);
    }

    @Test
    public void should_initialize_and_set_counter_value_for_entity_even_if_already_loaded() throws Exception {
        Class<? extends CompleteBean> beanClass = bean.getClass();

        PropertyMeta counterMeta = new PropertyMeta();
        counterMeta.setEntityClassName("beanClass");
        counterMeta.setType(COUNTER);
        counterMeta.setGetter(beanClass.getMethod("getCount"));
        counterMeta.setSetter(beanClass.getMethod("setCount", Counter.class));
        counterMeta.setInvoker(invoker);

        Set<Method> alreadyLoaded = Sets.newHashSet(counterMeta.getGetter());

        Map<Method, PropertyMeta> getterMetas = ImmutableMap.<Method, PropertyMeta> of(counterMeta.getGetter(),
                                                                                       counterMeta);

        Map<String, PropertyMeta> allMetas = ImmutableMap.<String, PropertyMeta> of("count", counterMeta);

        EntityMeta entityMeta = new EntityMeta();
        entityMeta.setPropertyMetas(allMetas);
        entityMeta.setGetterMetas(getterMetas);

        when(interceptor.getAlreadyLoaded()).thenReturn(alreadyLoaded);
        when(invoker.getValueFromField(bean, counterMeta.getGetter())).thenReturn(CounterBuilder.incr(10L));
        when(proxifier.getRealObject(bean)).thenReturn(bean);

        initializer.initializeEntity(bean, entityMeta, interceptor);

        ArgumentCaptor<Counter> counterCaptor = ArgumentCaptor.forClass(Counter.class);

        verify(invoker).setValueToField(eq(bean), eq(counterMeta.getSetter()), counterCaptor.capture());

        assertThat(counterCaptor.getValue().get()).isEqualTo(10L);
    }
}
