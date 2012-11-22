package fr.doan.achilles.manager;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;

import javax.persistence.FlushModeType;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import parser.entity.Bean;
import fr.doan.achilles.metadata.EntityMeta;
import fr.doan.achilles.operations.EntityLoader;
import fr.doan.achilles.operations.EntityPersister;

@SuppressWarnings(
{
		"rawtypes",
		"unchecked"
})
@RunWith(MockitoJUnitRunner.class)
public class AchillesEntityManagerTest
{

	@InjectMocks
	private AchillesEntityManager em;

	@Mock
	private Map<Class<?>, EntityMeta<?>> entityMetaMap;

	@Mock
	private EntityPersister persister;

	@Mock
	private EntityLoader loader;

	@Mock
	private EntityMeta entityMeta;

	@Before
	public void setUp()
	{
		ReflectionTestUtils.setField(em, "persister", persister);
		ReflectionTestUtils.setField(em, "loader", loader);
	}

	@Test
	public void should_persist() throws Exception
	{
		Bean entity = new Bean();

		when(entityMetaMap.get(Bean.class)).thenReturn((entityMeta));
		em.persist(entity);

		verify(persister).persist(entity, entityMeta);
	}

	@Test
	public void should_merge() throws Exception
	{
		Bean entity = new Bean();
		when(entityMetaMap.get(Bean.class)).thenReturn((entityMeta));
		em.merge(entity);
		verify(persister).persist(entity, entityMeta);
	}

	@Test
	public void should_remove() throws Exception
	{
		Bean entity = new Bean();
		when(entityMetaMap.get(Bean.class)).thenReturn((entityMeta));
		em.remove(entity);
		verify(persister).remove(entity, entityMeta);
	}

	@Test
	public void should_find() throws Exception
	{
		Bean entity = new Bean();
		when(entityMetaMap.get(Bean.class)).thenReturn(entityMeta);
		when(loader.load(Bean.class, 1L, entityMeta)).thenReturn(entity);
		Bean bean = em.find(Bean.class, 1L);

		assertThat(bean).isSameAs(entity);
	}

	@Test
	public void should_get_reference() throws Exception
	{
		Bean entity = new Bean();
		when(entityMetaMap.get(Bean.class)).thenReturn(entityMeta);
		when(loader.load(Bean.class, 1L, entityMeta)).thenReturn(entity);
		Bean bean = em.find(Bean.class, 1L);

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
