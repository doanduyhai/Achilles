package integration.tests;

import static fr.doan.achilles.common.CassandraDaoTest.getCluster;
import static fr.doan.achilles.common.CassandraDaoTest.getKeyspace;
import static org.fest.assertions.api.Assertions.assertThat;
import integration.tests.entity.CompleteBean;
import integration.tests.entity.CompleteBeanTestBuilder;
import net.sf.cglib.proxy.Factory;

import org.junit.Before;
import org.junit.Test;

import fr.doan.achilles.entity.factory.ThriftEntityManagerFactoryImpl;
import fr.doan.achilles.entity.manager.ThriftEntityManager;
import fr.doan.achilles.proxy.interceptor.AchillesInterceptor;

/**
 * LazyLoadingIT
 * 
 * @author DuyHai DOAN
 * 
 */
public class LazyLoadingIT
{
	private final String ENTITY_PACKAGE = "integration.tests.entity";

	private ThriftEntityManagerFactoryImpl factory = new ThriftEntityManagerFactoryImpl(
			getCluster(), getKeyspace(), ENTITY_PACKAGE, true);

	private ThriftEntityManager em = (ThriftEntityManager) factory.createEntityManager();

	private CompleteBean bean;

	@Before
	public void setUp()
	{
		bean = CompleteBeanTestBuilder.builder().randomId().name("DuyHai").age(35L)
				.addFriends("foo", "bar").label("label").buid();

		em.persist(bean);
	}

	@Test
	public void should_not_load_lazy_fields() throws Exception
	{
		bean = em.find(CompleteBean.class, bean.getId());

		Factory proxy = (Factory) bean;
		AchillesInterceptor interceptor = (AchillesInterceptor) proxy.getCallback(0);
		CompleteBean trueBean = (CompleteBean) interceptor.getTarget();

		assertThat(trueBean.getLabel()).isNull();
		assertThat(trueBean.getFriends()).isNull();

		// Trigger loading of lazy fields
		assertThat(bean.getLabel()).isEqualTo("label");
		assertThat(bean.getFriends()).containsExactly("foo", "bar");

		assertThat(trueBean.getLabel()).isEqualTo("label");
		assertThat(trueBean.getFriends()).containsExactly("foo", "bar");
	}
}
