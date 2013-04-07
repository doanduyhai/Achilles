package info.archinnov.achilles.entity.operations;

import static org.mockito.Mockito.when;
import info.archinnov.achilles.entity.EntityIntrospector;
import info.archinnov.achilles.entity.context.FlushContext;
import info.archinnov.achilles.entity.context.FlushContext.BatchType;
import info.archinnov.achilles.entity.context.PersistenceContext;
import info.archinnov.achilles.entity.context.PersistenceContextTestBuilder;
import info.archinnov.achilles.entity.manager.CompleteBeanTestBuilder;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;

import java.util.Map;

import mapping.entity.CompleteBean;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.internal.util.reflection.Whitebox;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * EntityValidatorTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class EntityValidatorTest
{
	@Rule
	public ExpectedException exception = ExpectedException.none();

	@InjectMocks
	private EntityValidator entityValidator;

	@Mock
	private EntityIntrospector introspector;

	@Mock
	private EntityProxifier proxifier;

	@Mock
	private Map<Class<?>, EntityMeta<?>> entityMetaMap;

	@Mock
	private EntityMeta<Long> entityMeta;

	@Mock
	private PropertyMeta<Void, Long> idMeta;

	@Before
	public void setUp()
	{
		when(entityMeta.getIdMeta()).thenReturn(idMeta);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_validate() throws Exception
	{
		CompleteBean bean = CompleteBeanTestBuilder.builder().id(12L).buid();

		when((Class<CompleteBean>) proxifier.deriveBaseClass(bean)).thenReturn(CompleteBean.class);
		when((EntityMeta<Long>) entityMetaMap.get(CompleteBean.class)).thenReturn(entityMeta);
		when(introspector.getKey(bean, idMeta)).thenReturn(12L);

		entityValidator.validateEntity(bean, entityMetaMap);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_exception_when_no_id() throws Exception
	{
		CompleteBean bean = CompleteBeanTestBuilder.builder().id(12L).buid();

		when((Class<CompleteBean>) proxifier.deriveBaseClass(bean)).thenReturn(CompleteBean.class);
		when((EntityMeta<Long>) entityMetaMap.get(CompleteBean.class)).thenReturn(entityMeta);
		when(introspector.getKey(bean, idMeta)).thenReturn(null);

		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Cannot get primary key for entity "
				+ CompleteBean.class.getCanonicalName());

		entityValidator.validateEntity(bean, entityMetaMap);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_validate_not_cf_direct_mapping() throws Exception
	{
		CompleteBean bean = CompleteBeanTestBuilder.builder().id(12L).buid();

		when((Class<CompleteBean>) proxifier.deriveBaseClass(bean)).thenReturn(CompleteBean.class);
		when((EntityMeta<Long>) entityMetaMap.get(CompleteBean.class)).thenReturn(entityMeta);
		when(entityMeta.isColumnFamilyDirectMapping()).thenReturn(false);

		entityValidator.validateNotCFDirectMapping(bean, entityMetaMap);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_exception_when_cf_direct_mapping() throws Exception
	{
		CompleteBean bean = CompleteBeanTestBuilder.builder().id(12L).buid();

		when((Class<CompleteBean>) proxifier.deriveBaseClass(bean)).thenReturn(CompleteBean.class);
		when((EntityMeta<Long>) entityMetaMap.get(CompleteBean.class)).thenReturn(entityMeta);
		when(entityMeta.isColumnFamilyDirectMapping()).thenReturn(true);

		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("This operation is not allowed for the entity '"
				+ CompleteBean.class.getCanonicalName()
				+ "' directly mapped to a native column family");

		entityValidator.validateNotCFDirectMapping(bean, entityMetaMap);
	}

	@Test
	public void should_check_no_pending_batch_with_persistence_context() throws Exception
	{
		PersistenceContext<Long> context = PersistenceContextTestBuilder //
				.mockAll(entityMeta, CompleteBean.class, 10L)//
				.build();
		FlushContext flushContext = new FlushContext(null, null, null, null);

		Whitebox.setInternalState(flushContext, "type", BatchType.NONE);
		Whitebox.setInternalState(context, "flushContext", flushContext);

		entityValidator.validateNoPendingBatch(context);
	}

	@Test
	public void should_check_no_when_pending_batch_with_flush_context() throws Exception
	{
		FlushContext flushContext = new FlushContext(null, null, null, null);

		Whitebox.setInternalState(flushContext, "type", BatchType.NONE);

		entityValidator.validateNoPendingBatch(flushContext);
	}
}
