package fr.doan.achilles.entity.operations;

import static org.mockito.Mockito.when;

import java.util.Map;

import mapping.entity.CompleteBean;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import fr.doan.achilles.entity.EntityHelper;
import fr.doan.achilles.entity.manager.CompleteBeanTestBuilder;
import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.exception.ValidationException;

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
	private EntityHelper helper;

	@Mock
	private Map<Class<?>, EntityMeta<?>> entityMetaMap;

	@Mock
	private EntityMeta entityMeta;

	@Test
	public void should_validate() throws Exception
	{
		CompleteBean bean = CompleteBeanTestBuilder.builder().id(12L).buid();

		when(helper.deriveBaseClass(bean)).thenReturn(CompleteBean.class);
		when(entityMetaMap.get(CompleteBean.class)).thenReturn(entityMeta);
		when(helper.determinePrimaryKey(bean, entityMeta)).thenReturn(12L);

		entityValidator.validateEntity(bean, entityMetaMap);
	}

	@Test
	public void should_validate_from_entity_meta() throws Exception
	{
		CompleteBean bean = CompleteBeanTestBuilder.builder().id(12L).buid();
		when(helper.determinePrimaryKey(bean, entityMeta)).thenReturn(12L);
		entityValidator.validateEntity(bean, entityMeta);
	}

	@Test(expected = ValidationException.class)
	public void should_exception_when_no_meta_found() throws Exception
	{
		CompleteBean bean = CompleteBeanTestBuilder.builder().id(12L).buid();

		when(helper.deriveBaseClass(bean)).thenReturn(CompleteBean.class);
		when(entityMetaMap.get(CompleteBean.class)).thenReturn(null);

		entityValidator.validateEntity(bean, entityMetaMap);
	}

	@Test(expected = IllegalArgumentException.class)
	public void should_exception_when_null_id() throws Exception
	{
		CompleteBean bean = CompleteBeanTestBuilder.builder().id(12L).buid();

		when(helper.deriveBaseClass(bean)).thenReturn(CompleteBean.class);
		when(entityMetaMap.get(CompleteBean.class)).thenReturn(entityMeta);
		when(helper.determinePrimaryKey(bean, entityMeta)).thenReturn(null);

		entityValidator.validateEntity(bean, entityMetaMap);
	}
}
