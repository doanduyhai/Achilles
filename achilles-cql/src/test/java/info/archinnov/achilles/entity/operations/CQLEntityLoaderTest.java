package info.archinnov.achilles.entity.operations;

import static info.archinnov.achilles.entity.metadata.PropertyType.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.context.CQLPersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.impl.CQLLoaderImpl;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.type.Counter;

import org.apache.commons.lang.math.RandomUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * CQLEntityLoaderTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class CQLEntityLoaderTest
{

	@InjectMocks
	private CQLEntityLoader loader;

	@Mock
	private CQLLoaderImpl loaderImpl;

	@Mock
	private ReflectionInvoker invoker;

	@Mock
	private CQLEntityProxifier proxifier;

	@Mock
	private CQLPersistenceContext context;

	private CompleteBean entity = new CompleteBean();

	private PropertyMeta<Void, Long> idMeta;

	private Long primaryKey = RandomUtils.nextLong();

	@Before
	public void setUp() throws Exception
	{

		idMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, Long.class)
				.field("id")
				.accessors()
				.build();

		EntityMeta meta = new EntityMeta();
		meta.setClusteredEntity(false);
		meta.setIdMeta(idMeta);

		when(context.getEntity()).thenReturn(entity);
		when(context.getEntityMeta()).thenReturn(meta);
		when(context.getPrimaryKey()).thenReturn(primaryKey);
	}

	@Test
	public void should_load_lazy_entity() throws Exception
	{

		when(context.isLoadEagerFields()).thenReturn(false);
		when(invoker.instanciate(CompleteBean.class)).thenReturn(entity);

		CompleteBean actual = loader.load(context, CompleteBean.class);

		assertThat(actual).isSameAs(entity);

		verify(invoker).setValueToField(actual, idMeta.getSetter(), primaryKey);
	}

	@Test
	public void should_load_entity() throws Exception
	{
		when(context.isLoadEagerFields()).thenReturn(true);
		when(loaderImpl.eagerLoadEntity(context, CompleteBean.class)).thenReturn(entity);

		CompleteBean actual = loader.load(context, CompleteBean.class);

		assertThat(actual).isSameAs(entity);

		verify(invoker).setValueToField(actual, idMeta.getSetter(), primaryKey);
	}

	@Test
	public void should_load_property_into_object() throws Exception
	{
		when(proxifier.getRealObject(entity)).thenReturn(entity);

		PropertyMeta<Void, Long> pm = PropertyMetaTestBuilder
				.valueClass(Long.class)
				.type(SIMPLE)
				.build();

		loader.loadPropertyIntoObject(context, entity, pm);

		verify(loaderImpl).loadPropertyIntoEntity(context, pm, entity);
	}

	@Test
	public void should_load_join_property_into_object() throws Exception
	{
		when(proxifier.getRealObject(entity)).thenReturn(entity);

		PropertyMeta<Void, Long> pm = PropertyMetaTestBuilder
				.valueClass(Long.class)
				.type(JOIN_SIMPLE)
				.build();

		loader.loadPropertyIntoObject(context, entity, pm);

		verify(loaderImpl).loadJoinPropertyIntoEntity(loader, context, pm, entity);
	}

	@Test
	public void should_not_load_property_into_object_for_proxy_type() throws Exception
	{
		when(proxifier.getRealObject(entity)).thenReturn(entity);

		PropertyMeta<Void, Counter> pm = PropertyMetaTestBuilder
				.valueClass(Counter.class)
				.type(COUNTER)
				.build();

		loader.loadPropertyIntoObject(context, entity, pm);

		verifyZeroInteractions(loaderImpl);
	}
}
