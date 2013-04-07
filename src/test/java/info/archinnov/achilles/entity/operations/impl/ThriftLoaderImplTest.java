package info.archinnov.achilles.entity.operations.impl;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.consistency.AchillesConfigurableConsistencyLevelPolicy;
import info.archinnov.achilles.dao.CounterDao;
import info.archinnov.achilles.dao.GenericEntityDao;
import info.archinnov.achilles.dao.Pair;
import info.archinnov.achilles.entity.EntityIntrospector;
import info.archinnov.achilles.entity.EntityMapper;
import info.archinnov.achilles.entity.context.FlushContext;
import info.archinnov.achilles.entity.context.PersistenceContext;
import info.archinnov.achilles.entity.context.PersistenceContextTestBuilder;
import info.archinnov.achilles.entity.manager.CompleteBeanTestBuilder;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;

import java.util.ArrayList;
import java.util.List;

import mapping.entity.CompleteBean;
import me.prettyprint.hector.api.beans.DynamicComposite;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import testBuilders.PropertyMetaTestBuilder;

/**
 * ThriftLoaderImplTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class ThriftLoaderImplTest
{

	@InjectMocks
	private ThriftLoaderImpl loader;

	@Mock
	private EntityMapper mapper;

	@Mock
	private EntityIntrospector introspector;

	@Mock
	private EntityMeta<Long> entityMeta;

	@Mock
	private GenericEntityDao<Long> entityDao;

	@Mock
	private FlushContext flushContext;

	@Mock
	private CounterDao counterDao;

	@Mock
	private AchillesConfigurableConsistencyLevelPolicy policy;

	@Captor
	ArgumentCaptor<CompleteBean> beanCaptor;

	private CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().buid();

	@Test
	public void should_load() throws Exception
	{
		PersistenceContext<Long> context = PersistenceContextTestBuilder
				.context(entityMeta, counterDao, policy, CompleteBean.class, entity.getId())
				.entity(entity) //
				.flushContext(flushContext) //
				.entityDao(entityDao) //
				.build();

		PropertyMeta<Void, Long> idMeta = PropertyMetaTestBuilder //
				.completeBean(Void.class, Long.class) //
				.field("id") //
				.accesors() //
				.build();

		DynamicComposite comp = new DynamicComposite();
		List<Pair<DynamicComposite, String>> values = new ArrayList<Pair<DynamicComposite, String>>();
		values.add(new Pair<DynamicComposite, String>(comp, "value"));

		when(entityDao.eagerFetchEntity(entity.getId())).thenReturn(values);
		when(entityMeta.getIdMeta()).thenReturn(idMeta);

		CompleteBean actual = loader.load(context);

		verify(mapper).setEagerPropertiesToEntity(eq(entity.getId()), eq(values), eq(entityMeta),
				beanCaptor.capture());
		verify(introspector).setValueToField(beanCaptor.capture(), eq(idMeta.getSetter()),
				eq(entity.getId()));

		assertThat(beanCaptor.getAllValues()).containsExactly(actual, actual);
	}
}
