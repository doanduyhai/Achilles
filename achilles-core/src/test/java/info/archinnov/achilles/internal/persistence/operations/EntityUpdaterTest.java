package info.archinnov.achilles.internal.persistence.operations;

import static info.archinnov.achilles.internal.persistence.metadata.PropertyType.SIMPLE;
import static java.util.Arrays.asList;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.internal.context.PersistenceContext;
import info.archinnov.achilles.internal.persistence.metadata.EntityMeta;
import info.archinnov.achilles.internal.persistence.metadata.PropertyMeta;
import info.archinnov.achilles.internal.proxy.EntityInterceptor;
import info.archinnov.achilles.test.builders.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.test.mapping.entity.UserBean;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class EntityUpdaterTest {

	@InjectMocks
	private EntityUpdater entityUpdater;

	@Mock
	private CounterPersister counterPersister;

	@Mock
	private EntityProxifier proxifier;

	@Mock
	private EntityInterceptor<CompleteBean> interceptor;

	@Mock
	private PersistenceContext context;

    @Mock
    private PropertyMeta pm;

    @Mock
    private EntityMeta meta;

    @Captor
    private ArgumentCaptor<List<PropertyMeta>> pmCaptor;

    private CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().buid();

    private List<PropertyMeta> allMetas = new ArrayList();

	private List<PropertyMeta> allCounterMetas = new ArrayList();

	private Map<Method, PropertyMeta> dirtyMap = new HashMap();

	@Before
	public void setUp() {

		when(context.getEntity()).thenReturn(entity);
		when(context.<CompleteBean>getEntityClass()).thenReturn(CompleteBean.class);
		when(context.getEntityMeta()).thenReturn(meta);

		allMetas.clear();
		dirtyMap.clear();
	}

	@Test
	public void should_update_proxified_entity() throws Exception {
		when(proxifier.isProxy(entity)).thenReturn(true);
		when(proxifier.getRealObject(entity)).thenReturn(entity);
		when(proxifier.getInterceptor(entity)).thenReturn(interceptor);
		when(interceptor.getDirtyMap()).thenReturn(dirtyMap);
        when(meta.getAllCounterMetas()).thenReturn(allCounterMetas);

        PropertyMeta pm = PropertyMetaTestBuilder.completeBean(Void.class, UserBean.class).field("user").type(SIMPLE)
				.accessors().build();
        dirtyMap.put(pm.getGetter(),pm);
        when(context.isClusteredCounter()).thenReturn(false);

		entityUpdater.update(context, entity);

		verify(context).setEntity(entity);
        verify(context).pushUpdateStatement(pmCaptor.capture());

        assertThat(pmCaptor.getValue()).containsOnly(pm);

        verify(counterPersister).persistCounters(context,allCounterMetas);
		verify(interceptor).setContext(context);
		verify(interceptor).setTarget(entity);

	}


    @Test
    public void should_update_proxified_clustered_counter_entity() throws Exception {
        when(proxifier.isProxy(entity)).thenReturn(true);
        when(proxifier.getRealObject(entity)).thenReturn(entity);
        when(proxifier.getInterceptor(entity)).thenReturn(interceptor);
        when(interceptor.getDirtyMap()).thenReturn(dirtyMap);

        PropertyMeta pm = PropertyMetaTestBuilder.completeBean(Void.class, UserBean.class).field("user").type(SIMPLE)
                                                 .accessors().build();
        dirtyMap.put(pm.getGetter(),pm);
        when(context.isClusteredCounter()).thenReturn(true);


        entityUpdater.update(context, entity);

        verify(context).setEntity(entity);
        verify(context).pushUpdateStatement(pmCaptor.capture());

        assertThat(pmCaptor.getValue()).containsOnly(pm);

        verify(counterPersister).persistClusteredCounters(context);
        verify(interceptor).setContext(context);
        verify(interceptor).setTarget(entity);

    }
}
