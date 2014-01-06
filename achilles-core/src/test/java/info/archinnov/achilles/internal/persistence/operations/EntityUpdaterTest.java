package info.archinnov.achilles.internal.persistence.operations;

import static info.archinnov.achilles.internal.persistence.metadata.PropertyType.SIMPLE;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.internal.context.PersistenceContext;
import info.archinnov.achilles.internal.persistence.metadata.EntityMeta;
import info.archinnov.achilles.internal.persistence.metadata.PropertyMeta;
import info.archinnov.achilles.internal.persistence.operations.impl.UpdaterImpl;
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
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class EntityUpdaterTest {

	@InjectMocks
	private EntityUpdater entityUpdater;

	@Mock
	private UpdaterImpl updater;

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

	@Before
	public void setUp() {

		when(context.getEntity()).thenReturn(entity);
		when(context.<CompleteBean> getEntityClass()).thenReturn(CompleteBean.class);
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

		PropertyMeta pm = PropertyMetaTestBuilder.completeBean(Void.class, UserBean.class).field("user").type(SIMPLE)
				.accessors().build();

		meta.setAllMetasExceptId(Arrays.<PropertyMeta>asList(pm));

		dirtyMap.put(pm.getSetter(), pm);

		entityUpdater.update(context, entity);

        verify(updater).update(context, dirtyMap);
		verify(context).setEntity(entity);

		verify(interceptor).setContext(context);
		verify(interceptor).setTarget(entity);

	}
}
