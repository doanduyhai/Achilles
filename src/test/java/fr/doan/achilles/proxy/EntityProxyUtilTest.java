package fr.doan.achilles.proxy;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;

import mapping.entity.CompleteBean;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.NoOp;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import fr.doan.achilles.entity.manager.CompleteBeanTestBuilder;
import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.proxy.interceptor.JpaInterceptor;

@RunWith(MockitoJUnitRunner.class)
public class EntityProxyUtilTest
{
	private EntityProxyUtil util = new EntityProxyUtil();

	@Mock
	private EntityMeta<Long> entityMeta;

	@Mock
	private PropertyMeta<Long> idMeta;

	@Test
	public void should_proxy_true() throws Exception
	{
		Enhancer enhancer = new Enhancer();

		enhancer.setSuperclass(CompleteBean.class);
		enhancer.setCallback(NoOp.INSTANCE);

		CompleteBean proxy = (CompleteBean) enhancer.create();

		assertThat(util.isProxy(proxy)).isTrue();
	}

	@Test
	public void should_proxy_false() throws Exception
	{
		CompleteBean bean = CompleteBeanTestBuilder.builder().id(1L).buid();
		assertThat(util.isProxy(bean)).isFalse();
	}

	@Test
	public void should_derive_base_class() throws Exception
	{
		CompleteBean entity = CompleteBeanTestBuilder.builder().id(1L).buid();
		Enhancer enhancer = new Enhancer();
		enhancer.setSuperclass(entity.getClass());

		JpaInterceptor<Long> interceptor = new JpaInterceptor<Long>();
		interceptor.setTarget(entity);

		enhancer.setCallback(interceptor);

		CompleteBean proxy = (CompleteBean) enhancer.create();

		assertThat(util.deriveBaseClass(proxy)).isEqualTo(CompleteBean.class);
	}

	@Test
	public void should_determine_primary_key() throws Exception
	{
		Method idGetter = CompleteBean.class.getDeclaredMethod("getId");

		when(entityMeta.getIdMeta()).thenReturn(idMeta);
		when(idMeta.getGetter()).thenReturn(idGetter);

		CompleteBean bean = CompleteBeanTestBuilder.builder().id(12L).buid();

		Object key = util.determinePrimaryKey(bean, entityMeta);

		assertThat(key).isEqualTo(12L);
	}

	@Test
	public void should_determine_null_primary_key() throws Exception
	{
		Method idGetter = CompleteBean.class.getDeclaredMethod("getId");

		when(entityMeta.getIdMeta()).thenReturn(idMeta);
		when(idMeta.getGetter()).thenReturn(idGetter);

		CompleteBean bean = CompleteBeanTestBuilder.builder().buid();

		Object key = util.determinePrimaryKey(bean, entityMeta);

		assertThat(key).isNull();

	}
}
