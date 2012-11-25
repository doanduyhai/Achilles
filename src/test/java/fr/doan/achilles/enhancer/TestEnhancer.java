package fr.doan.achilles.enhancer;

import static org.fest.assertions.api.Assertions.assertThat;
import mapping.entity.CompleteBean;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.Factory;

import org.junit.Test;

public class TestEnhancer
{

	@Test
	public void should_enhance() throws Exception
	{

		Enhancer e = new Enhancer();

		e.setSuperclass(CompleteBean.class);
		e.setCallback(new TestMethodInterceptor(null));

		// CompleteBean bean = CompleteBeanTestBuilder.builder().name("name").age(10L).buid();

		CompleteBean proxy = (CompleteBean) e.create();

		proxy.setName("name");

		assertThat(proxy.getName()).isEqualTo("name");

		assertThat(proxy).isInstanceOf(Factory.class);

		assertThat(Factory.class.isAssignableFrom(proxy.getClass())).isTrue();
	}
}
