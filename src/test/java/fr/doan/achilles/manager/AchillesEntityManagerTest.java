package fr.doan.achilles.manager;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import parser.entity.CompleteBean;
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

	@Test
	public void should_persist() throws Exception
	{
		CompleteBean entity = new CompleteBean();

		when(entityMetaMap.get(CompleteBean.class)).thenReturn((entityMeta));
		em.persist(entity);

		verify(persister).persist(entity, entityMeta);
	}

	@Test
	public void should_merge() throws Exception
	{
		CompleteBean entity = new CompleteBean();
		when(entityMetaMap.get(CompleteBean.class)).thenReturn((entityMeta));
		CompleteBean mergedEntity = em.merge(entity);
		verify(persister).persist(entity, entityMeta);
	}

	@Test
	public void should_remove() throws Exception
	{

	}

	@Test
	public void should_find() throws Exception
	{

	}

	@Test
	public void should_get_reference() throws Exception
	{

	}

	@Test
	public void should_get_flush_mode() throws Exception
	{

	}

	@Test
	public void should_refresh() throws Exception
	{

	}

	@Test
	public void should_exception_when_set_flush_mode() throws Exception
	{

	}

}
