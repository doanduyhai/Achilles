package fr.doan.achilles.entity.manager;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.Map;

import javax.persistence.FlushModeType;

import mapping.entity.CompleteBean;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import parser.entity.Bean;
import fr.doan.achilles.entity.EntityHelper;
import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.operations.EntityLoader;
import fr.doan.achilles.entity.operations.EntityMerger;
import fr.doan.achilles.entity.operations.EntityPersister;
import fr.doan.achilles.proxy.builder.EntityProxyBuilder;

@SuppressWarnings(
{
		"rawtypes",
		"unchecked"
})
@RunWith(MockitoJUnitRunner.class)
public class ThriftEntityManagerTest
{

	@InjectMocks
	private ThriftEntityManager em;

	@Mock
	private Map<Class<?>, EntityMeta<?>> entityMetaMap;

	@Mock
	private EntityPersister persister;

	@Mock
	private EntityLoader loader;

	@Mock
	private EntityMerger merger;

	@Mock
	private EntityProxyBuilder interceptorBuilder;

	@Mock
	private EntityHelper helper;

	@Mock
	private EntityMeta entityMeta;

	private CompleteBean entity = CompleteBeanTestBuilder.builder().id(1L).name("name").buid();

	private Method idGetter;

	@Before
	public void setUp() throws Exception
	{
		ReflectionTestUtils.setField(em, "persister", persister);
		ReflectionTestUtils.setField(merger, "persister", persister);
		ReflectionTestUtils.setField(em, "loader", loader);
		ReflectionTestUtils.setField(em, "merger", merger);
		ReflectionTestUtils.setField(em, "helper", helper);
		ReflectionTestUtils.setField(em, "interceptorBuilder", interceptorBuilder);

		idGetter = CompleteBean.class.getDeclaredMethod("getId", (Class<?>[]) null);

		when(entityMetaMap.get(CompleteBean.class)).thenReturn(entityMeta);
		PropertyMeta<Void, Long> propertyMeta = mock(PropertyMeta.class);
		when(entityMeta.getIdMeta()).thenReturn(propertyMeta);
		when(propertyMeta.getGetter()).thenReturn(idGetter);

	}

	@Test
	public void should_persist() throws Exception
	{
		when(helper.deriveBaseClass(entity)).thenReturn(CompleteBean.class);
		when(entityMetaMap.get(CompleteBean.class)).thenReturn((entityMeta));
		em.persist(entity);

		verify(persister).persist(entity, entityMeta);
	}

	@Test(expected = IllegalStateException.class)
	public void should_exception_trying_to_persist_a_managed_entity() throws Exception
	{
		when(helper.deriveBaseClass(entity)).thenReturn(CompleteBean.class);
		when(entityMetaMap.get(CompleteBean.class)).thenReturn((entityMeta));
		when(helper.isProxy(entity)).thenReturn(true);
		em.persist(entity);
	}

	@Test
	public void should_merge() throws Exception
	{
		when(helper.deriveBaseClass(entity)).thenReturn(CompleteBean.class);
		when(entityMetaMap.get(CompleteBean.class)).thenReturn((entityMeta));
		when(merger.mergeEntity(entity, entityMeta)).thenReturn(entity);

		CompleteBean mergedEntity = em.merge(entity);

		assertThat(mergedEntity).isSameAs(entity);
	}

	@Test
	public void should_remove() throws Exception
	{
		when(helper.deriveBaseClass(entity)).thenReturn(CompleteBean.class);
		when(entityMetaMap.get(CompleteBean.class)).thenReturn((entityMeta));
		em.remove(entity);
		verify(persister).remove(entity, entityMeta);
	}

	@Test
	public void should_find() throws Exception
	{
		when(entityMetaMap.get(CompleteBean.class)).thenReturn(entityMeta);
		when(loader.load(CompleteBean.class, 1L, entityMeta)).thenReturn(entity);
		when(interceptorBuilder.build(entity, entityMeta)).thenReturn(entity);

		CompleteBean bean = em.find(CompleteBean.class, 1L);

		assertThat(bean).isSameAs(entity);
	}

	@Test
	public void should_get_reference() throws Exception
	{
		when(entityMetaMap.get(CompleteBean.class)).thenReturn(entityMeta);
		when(loader.load(CompleteBean.class, 1L, entityMeta)).thenReturn(entity);
		when(interceptorBuilder.build(entity, entityMeta)).thenReturn(entity);

		CompleteBean bean = em.find(CompleteBean.class, 1L);

		assertThat(bean).isSameAs(entity);
	}

	@Test
	public void should_get_flush_mode() throws Exception
	{
		FlushModeType flushMode = em.getFlushMode();

		assertThat(flushMode).isEqualTo(FlushModeType.AUTO);
	}

	@Test
	public void should_refresh() throws Exception
	{
		Bean entity = new Bean();
		when(entityMetaMap.get(Bean.class)).thenReturn(entityMeta);

		when(loader.load(Bean.class, 1L, entityMeta)).thenReturn(entity);

	}

	@Test(expected = UnsupportedOperationException.class)
	public void should_exception_when_set_flush_mode() throws Exception
	{
		em.setFlushMode(FlushModeType.COMMIT);
	}

}
