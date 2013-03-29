package info.archinnov.achilles.entity.operations;

import static org.mockito.Mockito.when;

import info.archinnov.achilles.entity.EntityIntrospector;
import info.archinnov.achilles.entity.manager.CompleteBeanTestBuilder;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.operations.EntityValidator;
import info.archinnov.achilles.exception.AchillesException;

import java.util.Map;

import mapping.entity.CompleteBean;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;


/**
 * EntityValidatorTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
@SuppressWarnings(
{
		"rawtypes",
		"unchecked"
})
public class EntityValidatorTest
{

	@InjectMocks
	private EntityValidator entityValidator;

	@Mock
	private EntityIntrospector introspector;

	@Mock
	private Map<Class<?>, EntityMeta<?>> entityMetaMap;

	@Mock
	private EntityMeta entityMeta;

	@Test
	public void should_validate() throws Exception
	{
		CompleteBean bean = CompleteBeanTestBuilder.builder().id(12L).buid();

		when((Class<CompleteBean>) introspector.deriveBaseClass(bean)).thenReturn(CompleteBean.class);
		when(entityMetaMap.get(CompleteBean.class)).thenReturn(entityMeta);
		when(introspector.determinePrimaryKey(bean, entityMeta)).thenReturn(12L);

		entityValidator.validateEntity(bean, entityMetaMap);
	}

	@Test
	public void should_validate_from_entity_meta() throws Exception
	{
		CompleteBean bean = CompleteBeanTestBuilder.builder().id(12L).buid();
		when(introspector.determinePrimaryKey(bean, entityMeta)).thenReturn(12L);
		entityValidator.validateEntity(bean, entityMeta);
	}

	@Test(expected = AchillesException.class)
	public void should_exception_when_no_meta_found() throws Exception
	{
		CompleteBean bean = CompleteBeanTestBuilder.builder().id(12L).buid();

		when((Class<CompleteBean>) introspector.deriveBaseClass(bean)).thenReturn(CompleteBean.class);
		when(entityMetaMap.get(CompleteBean.class)).thenReturn(null);

		entityValidator.validateEntity(bean, entityMetaMap);
	}

	@Test(expected = IllegalArgumentException.class)
	public void should_exception_when_null_id() throws Exception
	{
		CompleteBean bean = CompleteBeanTestBuilder.builder().id(12L).buid();

		when((Class<CompleteBean>) introspector.deriveBaseClass(bean)).thenReturn(CompleteBean.class);
		when(entityMetaMap.get(CompleteBean.class)).thenReturn(entityMeta);
		when(introspector.determinePrimaryKey(bean, entityMeta)).thenReturn(null);

		entityValidator.validateEntity(bean, entityMetaMap);
	}
}
