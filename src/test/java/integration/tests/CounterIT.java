package integration.tests;

import static info.archinnov.achilles.entity.metadata.PropertyType.COUNTER;
import static info.archinnov.achilles.serializer.SerializerUtils.STRING_SRZ;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.common.CassandraDaoTest;
import info.archinnov.achilles.dao.CounterDao;
import info.archinnov.achilles.entity.manager.ThriftEntityManager;
import info.archinnov.achilles.entity.metadata.PropertyType;
import integration.tests.entity.CompleteBean;
import integration.tests.entity.CompleteBeanTestBuilder;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.DynamicComposite;

import org.junit.Test;

/**
 * CounterIT
 * 
 * @author DuyHai DOAN
 * 
 */
public class CounterIT
{

	private CounterDao counterDao = CassandraDaoTest.getCounterDao();
	private ThriftEntityManager em = CassandraDaoTest.getEm();
	private CompleteBean bean;

	@Test
	public void should_persist_counter() throws Exception
	{
		bean = CompleteBeanTestBuilder.builder().randomId().name("test").buid();
		bean.setVersion(2L);

		em.persist(bean);
		Composite keyComp = createCounterKey(CompleteBean.class, bean.getId());
		DynamicComposite comp = createCounterName(COUNTER, "version");
		Long actual = counterDao.getCounterValue(keyComp, comp);

		assertThat(actual).isEqualTo(2L);
	}

	@Test
	public void should_merge_counter() throws Exception
	{
		bean = CompleteBeanTestBuilder.builder().randomId().name("test").buid();
		bean = em.merge(bean);

		bean.setVersion(251L);

		em.merge(bean);

		Composite keyComp = createCounterKey(CompleteBean.class, bean.getId());
		DynamicComposite comp = createCounterName(COUNTER, "version");
		Long actual = counterDao.getCounterValue(keyComp, comp);

		assertThat(actual).isEqualTo(251L);
	}

	@Test
	public void should_find_counter() throws Exception
	{
		long version = 10L;
		bean = CompleteBeanTestBuilder.builder().randomId().name("test").version(version).buid();

		em.persist(bean);

		bean = em.find(CompleteBean.class, bean.getId());

		assertThat(bean.getVersion()).isEqualTo(version);
	}

	@Test
	public void should_remove_counter() throws Exception
	{
		long version = 154321L;
		bean = CompleteBeanTestBuilder.builder().randomId().name("test").version(version).buid();
		bean = em.merge(bean);

		Composite keyComp = createCounterKey(CompleteBean.class, bean.getId());
		DynamicComposite comp = createCounterName(COUNTER, "version");
		Long actual = counterDao.getCounterValue(keyComp, comp);

		assertThat(actual).isEqualTo(version);

		em.remove(bean);

		actual = counterDao.getCounterValue(keyComp, comp);

		assertThat(actual).isEqualTo(0);
	}

	@Test
	public void should_refresh_counter() throws Exception
	{
		long version = 454L, newVersion = 1234L;
		bean = CompleteBeanTestBuilder.builder().randomId().name("test").version(version).buid();
		bean = em.merge(bean);

		assertThat(bean.getVersion()).isEqualTo(version);

		Composite keyComp = createCounterKey(CompleteBean.class, bean.getId());
		DynamicComposite comp = createCounterName(COUNTER, "version");

		counterDao.insertCounter(keyComp, comp, newVersion);

		em.refresh(bean);

		assertThat(bean.getVersion()).isEqualTo(newVersion);
	}

	private <T> Composite createCounterKey(Class<T> clazz, Long id)
	{
		Composite comp = new Composite();
		comp.setComponent(0, clazz.getCanonicalName(), STRING_SRZ);
		comp.setComponent(1, id.toString(), STRING_SRZ);
		return comp;
	}

	private DynamicComposite createCounterName(PropertyType type, String propertyName)
	{
		DynamicComposite composite = new DynamicComposite();
		composite.addComponent(0, type.flag(), ComponentEquality.EQUAL);
		composite.addComponent(1, propertyName, ComponentEquality.EQUAL);
		return composite;
	}
}
