package fr.doan.achilles.operations;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import mapping.entity.CompleteBean;
import me.prettyprint.cassandra.model.ExecutingKeyspace;
import me.prettyprint.hector.api.beans.Composite;

import org.apache.cassandra.utils.Pair;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import fr.doan.achilles.bean.BeanMapper;
import fr.doan.achilles.dao.GenericDao;
import fr.doan.achilles.metadata.EntityMeta;

@RunWith(MockitoJUnitRunner.class)
public class EntityLoaderTest
{

	@InjectMocks
	private EntityLoader loader = new EntityLoader();

	@Mock
	private ExecutingKeyspace keyspace;

	@Mock
	private EntityMeta<Long> entityMeta;

	@Mock
	private BeanMapper mapper;

	@Mock
	private GenericDao<Long> dao;

	@Test
	public void should_load_entity() throws Exception
	{
		List<Pair<Composite, Object>> columns = new ArrayList<Pair<Composite, Object>>();
		columns.add(new Pair<Composite, Object>(new Composite(), ""));

		when(entityMeta.getDao()).thenReturn(dao);
		when(dao.eagerFetchEntity(1L)).thenReturn(columns);
		loader.load(CompleteBean.class, 1L, entityMeta);

		verify(mapper).mapColumnsToBean(eq(1L), eq(columns), eq(entityMeta), any(CompleteBean.class));
	}

	@Test
	public void should_not_load_entity_because_not_found() throws Exception
	{
		List<Pair<Composite, Object>> columns = new ArrayList<Pair<Composite, Object>>();

		when(entityMeta.getDao()).thenReturn(dao);
		when(dao.eagerFetchEntity(1L)).thenReturn(columns);
		CompleteBean bean = loader.load(CompleteBean.class, 1L, entityMeta);

		assertThat(bean).isNull();
		verifyZeroInteractions(mapper);
	}

	@Test(expected = RuntimeException.class)
	public void should_exception_when_error() throws Exception
	{

		when(entityMeta.getDao()).thenThrow(new RuntimeException());
		loader.load(CompleteBean.class, 1L, entityMeta);
	}
}
