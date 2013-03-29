package info.archinnov.achilles.entity.operations;

import static info.archinnov.achilles.entity.metadata.PropertyType.JOIN_SIMPLE;
import static info.archinnov.achilles.entity.metadata.PropertyType.SIMPLE;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.dao.AbstractDao;
import info.archinnov.achilles.dao.GenericDynamicCompositeDao;
import info.archinnov.achilles.dao.Pair;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.builder.EntityMetaTestBuilder;
import info.archinnov.achilles.proxy.interceptor.JpaEntityInterceptor;

import java.util.Map;

import mapping.entity.CompleteBean;
import mapping.entity.UserBean;
import me.prettyprint.hector.api.mutation.Mutator;
import net.sf.cglib.proxy.Factory;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import testBuilders.PropertyMetaTestBuilder;

/**
 * EntityBatcherTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class EntityBatcherTest
{

	@InjectMocks
	private EntityBatcher batcher;

	@Mock
	private EntityMeta<Long> entityMeta;

	private PropertyMeta<Void, Long> idMeta;

	@Mock
	private Map<String, GenericDynamicCompositeDao<?>> entityDaosMap;

	@Mock
	private GenericDynamicCompositeDao<Long> entityDao;

	@Mock
	private GenericDynamicCompositeDao<Long> joinEntityDao;

	@Mock
	private Mutator<Long> mutator;

	@Mock
	private Mutator<Long> joinMutator;

	@Captor
	private ArgumentCaptor<Map<String, Pair<Mutator<?>, AbstractDao<?, ?, ?>>>> mutatorMapCaptor;

	@SuppressWarnings("unchecked")
	@Before
	public void setUp() throws Exception
	{

		idMeta = PropertyMetaTestBuilder //
				.of(CompleteBean.class, Void.class, Long.class) //
				.field("id") //
				.accesors() //
				.type(SIMPLE) //
				.build();
		entityMeta = EntityMetaTestBuilder.builder(idMeta).build();

		// when(entityMeta.getIdMeta()).thenReturn(idMeta);

	}

	@Test
	public void should_start_batch_for_entity_and_join_entity() throws Exception
	{
		Factory bean = mock(Factory.class);

		PropertyMeta<Void, Long> joinIdMeta = PropertyMetaTestBuilder //
				.of(UserBean.class, Void.class, Long.class) //
				.field("userId") //
				.accesors() //
				.type(SIMPLE) //
				.build();

		EntityMeta<Long> joinMeta = EntityMetaTestBuilder.builder(joinIdMeta) //
				.columnFamilyName("join_cf") //
				.build();

		PropertyMeta<Void, UserBean> userMeta = PropertyMetaTestBuilder //
				.of(CompleteBean.class, Void.class, UserBean.class) //
				.field("user") //
				.accesors() //
				.type(JOIN_SIMPLE)//
				.joinMeta(joinMeta) //
				.build();

		entityMeta = EntityMetaTestBuilder.builder(idMeta) //
				.columnFamilyDirectMapping(false) //
				.columnFamilyName("cf") //
				.addPropertyMeta(userMeta) //
				.build();

		when((GenericDynamicCompositeDao<Long>) entityDaosMap.get("join_cf")).thenReturn(
				joinEntityDao);
		when(joinEntityDao.buildMutator()).thenReturn(joinMutator);

		when((GenericDynamicCompositeDao<Long>) entityDaosMap.get("cf")).thenReturn(entityDao);
		when(entityDao.buildMutator()).thenReturn(mutator);

		JpaEntityInterceptor<Long, UserBean> interceptor = mock(JpaEntityInterceptor.class);

		when(bean.getCallback(0)).thenReturn(interceptor);

		batcher.startBatchForEntity(bean, null);

		verify(interceptor).setMutator(mutator);
		verify(interceptor).setMutatorMap(mutatorMapCaptor.capture());

		assertThat((Mutator<Long>) mutatorMapCaptor.getValue().get("user").left).isSameAs(
				joinMutator);
	}
}
