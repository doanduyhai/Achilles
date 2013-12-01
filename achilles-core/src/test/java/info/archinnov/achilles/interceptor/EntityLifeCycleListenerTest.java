package info.archinnov.achilles.interceptor;

import static org.mockito.Mockito.when;
import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.operations.EntityProxifier;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.test.builders.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.fest.assertions.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class EntityLifeCycleListenerTest {

	@InjectMocks
	private EntityLifeCycleListener<PersistenceContext> entityLifeCycleListener;

	@Mock
	private Map<Class<?>, EntityMeta> entityMetaMap;

	@Mock
	EntityProxifier<PersistenceContext> proxifier;

	@Before
	public void initMocks() {
		MockitoAnnotations.initMocks(this);
	}

	@Test
	public void testIntercept_should_apply_right_interceptor_on_right_event() throws Exception {

		CompleteBean bean = CompleteBeanTestBuilder.builder().id(12L).buid();
		Mockito.doCallRealMethod().when(proxifier).deriveBaseClass(bean);
		EntityMeta entityMeta = new EntityMeta();
		PropertyMeta idMeta = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).field("id").type(PropertyType.EMBEDDED_ID).accessors()
				.build();
		idMeta.setInvoker(new ReflectionInvoker());
		entityMeta.setIdMeta(idMeta);
		EventInterceptor<CompleteBean> eventInterceptor = createEventInterceptor(Event.PRE_PERSIST, 30L);
		entityMeta.addInterceptor(eventInterceptor);
		entityMeta.addInterceptor(createEventInterceptor(Event.POST_PERSIST, 35L));

		when(entityMetaMap.get(Matchers.any())).thenReturn(entityMeta);

		entityLifeCycleListener.intercept(bean, Event.PRE_PERSIST);
		Assertions.assertThat(bean.getAge()).isEqualTo(30L);
		entityLifeCycleListener.intercept(bean, Event.POST_PERSIST);
		Assertions.assertThat(bean.getAge()).isEqualTo(35L);
	}

	private EventInterceptor<CompleteBean> createEventInterceptor(final Event event, final long age) {
		EventInterceptor<CompleteBean> eventInterceptor = new EventInterceptor<CompleteBean>() {

			@Override
			public CompleteBean onEvent(CompleteBean entity) {
				entity.setAge(age);
				return entity;
			}

			@Override
			public List<Event> events() {
				return Arrays.asList(event);
			}
		};
		return eventInterceptor;
	}
}
