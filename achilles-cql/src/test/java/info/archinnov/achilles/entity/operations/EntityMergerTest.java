package info.archinnov.achilles.entity.operations;

import static info.archinnov.achilles.entity.metadata.PropertyType.SIMPLE;
import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.impl.MergerImpl;
import info.archinnov.achilles.proxy.EntityInterceptor;
import info.archinnov.achilles.test.builders.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.test.mapping.entity.UserBean;

@RunWith(MockitoJUnitRunner.class)
public class EntityMergerTest {

    @InjectMocks
    private EntityMerger entityMerger;

    @Mock
    private MergerImpl merger;

    @Mock
    private EntityPersister persister;

    @Mock
    private EntityProxifier proxifier;

    @Mock
    private EntityInterceptor<CompleteBean> interceptor;

    @Mock
    private PersistenceContext context;

    private CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().buid();

    private EntityMeta meta = new EntityMeta();

    private List<PropertyMeta> allMetas = new ArrayList<PropertyMeta>();

    private Map<Method, PropertyMeta> dirtyMap = new HashMap<Method, PropertyMeta>();

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() {

        when(context.getEntity()).thenReturn(entity);
        when((Class<CompleteBean>) context.getEntityClass()).thenReturn(CompleteBean.class);
        when(context.getEntityMeta()).thenReturn(meta);

        allMetas.clear();
        dirtyMap.clear();
    }

    @Test
    public void should_merge_proxified_entity() throws Exception {
        when(proxifier.isProxy(entity)).thenReturn(true);
        when(proxifier.getRealObject(entity)).thenReturn(entity);
        when(proxifier.getInterceptor(entity)).thenReturn(interceptor);
        when(interceptor.getDirtyMap()).thenReturn(dirtyMap);

        PropertyMeta pm = PropertyMetaTestBuilder.completeBean(Void.class, UserBean.class).field("user").type(SIMPLE)
                                                 .accessors().build();

        meta.setAllMetasExceptIdMeta(Arrays.<PropertyMeta> asList(pm));

        dirtyMap.put(pm.getSetter(), pm);

        CompleteBean actual = entityMerger.merge(context, entity);

        assertThat(actual).isSameAs(entity);
        verify(context).setEntity(entity);
        verify(merger).merge(context, dirtyMap);

        verify(interceptor).setContext(context);
        verify(interceptor).setTarget(entity);

    }

    @Test
    public void should_persist_transient_entity() throws Exception {
        when(proxifier.isProxy(entity)).thenReturn(false);
        when(context.isClusteredEntity()).thenReturn(false);
        when(proxifier.buildProxy(entity, context)).thenReturn(entity);

        CompleteBean actual = entityMerger.merge(context, entity);

        assertThat(actual).isSameAs(entity);
        verify(persister).persist(context);
    }

}
