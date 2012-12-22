package fr.doan.achilles.entity.manager;

import static fr.doan.achilles.common.CassandraDaoTest.getCluster;
import static fr.doan.achilles.common.CassandraDaoTest.getDao;
import static fr.doan.achilles.common.CassandraDaoTest.getKeyspace;
import static fr.doan.achilles.entity.metadata.PropertyType.END_EAGER;
import static fr.doan.achilles.entity.metadata.PropertyType.LAZY_LIST;
import static fr.doan.achilles.entity.metadata.PropertyType.START_EAGER;
import static fr.doan.achilles.entity.metadata.builder.EntityMetaBuilder.normalizeColumnFamilyName;
import static fr.doan.achilles.serializer.Utils.LONG_SRZ;
import static fr.doan.achilles.serializer.Utils.STRING_SRZ;
import static org.fest.assertions.api.Assertions.assertThat;

import java.lang.reflect.Method;
import java.util.List;

import javax.persistence.FlushModeType;
import javax.persistence.LockModeType;

import mapping.entity.CompleteBean;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.DynamicComposite;
import net.sf.cglib.proxy.Factory;

import org.apache.cassandra.utils.Pair;
import org.junit.After;
import org.junit.Test;

import fr.doan.achilles.composite.factory.DynamicCompositeKeyFactory;
import fr.doan.achilles.dao.GenericEntityDao;
import fr.doan.achilles.entity.factory.ThriftEntityManagerFactoryImpl;
import fr.doan.achilles.entity.metadata.ListLazyMeta;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.metadata.PropertyType;
import fr.doan.achilles.entity.metadata.SimpleMeta;
import fr.doan.achilles.holder.KeyValueHolder;
import fr.doan.achilles.proxy.interceptor.JpaEntityInterceptor;

public class ThriftEntityManagerOperationsIT
{

	private final String ENTITY_PACKAGE = "mapping.entity";
	private GenericEntityDao<Long> dao = getDao(LONG_SRZ,
			normalizeColumnFamilyName(CompleteBean.class.getCanonicalName()));

	private ThriftEntityManagerFactoryImpl factory = new ThriftEntityManagerFactoryImpl(
			getCluster(), getKeyspace(), ENTITY_PACKAGE, true);

	private ThriftEntityManager em = (ThriftEntityManager) factory.createEntityManager();

	private DynamicCompositeKeyFactory keyFactory = new DynamicCompositeKeyFactory();

	@Test
	public void should_persist() throws Exception
	{
		CompleteBean bean = CompleteBeanTestBuilder.builder().randomId().name("DuyHai").age(35L)
				.addFriends("foo", "bar").addFollowers("George", "Paul").addPreference(1, "FR")
				.addPreference(2, "Paris").addPreference(3, "75014").buid();

		em.persist(bean);

		DynamicComposite startCompositeForEagerFetch = new DynamicComposite();
		startCompositeForEagerFetch.addComponent(0, START_EAGER.flag(), ComponentEquality.EQUAL);

		DynamicComposite endCompositeForEagerFetch = new DynamicComposite();
		endCompositeForEagerFetch.addComponent(0, END_EAGER.flag(),
				ComponentEquality.GREATER_THAN_EQUAL);

		List<Pair<DynamicComposite, Object>> columns = dao.findColumnsRange(bean.getId(),
				startCompositeForEagerFetch, endCompositeForEagerFetch, false, 20);

		assertThat(columns).hasSize(7);

		Pair<DynamicComposite, Object> age = columns.get(0);

		Pair<DynamicComposite, Object> name = columns.get(1);

		Pair<DynamicComposite, Object> George = columns.get(2);
		Pair<DynamicComposite, Object> Paul = columns.get(3);

		Pair<DynamicComposite, Object> FR = columns.get(4);
		Pair<DynamicComposite, Object> Paris = columns.get(5);
		Pair<DynamicComposite, Object> _75014 = columns.get(6);

		assertThat(age.left.get(1, STRING_SRZ)).isEqualTo("age_in_years");
		assertThat(age.right).isEqualTo(35L);

		assertThat(name.left.get(1, STRING_SRZ)).isEqualTo("name");
		assertThat(name.right).isEqualTo("DuyHai");

		assertThat(George.left.get(1, STRING_SRZ)).isEqualTo("followers");
		assertThat(George.right).isIn("George", "Paul");
		assertThat(Paul.left.get(1, STRING_SRZ)).isEqualTo("followers");
		assertThat(Paul.right).isIn("George", "Paul");

		assertThat(FR.left.get(1, STRING_SRZ)).isEqualTo("preferences");
		KeyValueHolder country = (KeyValueHolder) FR.right;
		assertThat(country.getKey()).isEqualTo(1);
		assertThat(country.getValue()).isEqualTo("FR");

		assertThat(Paris.left.get(1, STRING_SRZ)).isEqualTo("preferences");
		KeyValueHolder city = (KeyValueHolder) Paris.right;
		assertThat(city.getKey()).isEqualTo(2);
		assertThat(city.getValue()).isEqualTo("Paris");

		assertThat(_75014.left.get(1, STRING_SRZ)).isEqualTo("preferences");
		KeyValueHolder zipCode = (KeyValueHolder) _75014.right;
		assertThat(zipCode.getKey()).isEqualTo(3);
		assertThat(zipCode.getValue()).isEqualTo("75014");

		startCompositeForEagerFetch = new DynamicComposite();
		startCompositeForEagerFetch.addComponent(0, LAZY_LIST.flag(), ComponentEquality.EQUAL);

		endCompositeForEagerFetch = new DynamicComposite();
		endCompositeForEagerFetch.addComponent(0, LAZY_LIST.flag(),
				ComponentEquality.GREATER_THAN_EQUAL);

		columns = dao.findColumnsRange(bean.getId(), startCompositeForEagerFetch,
				endCompositeForEagerFetch, false, 20);
		assertThat(columns).hasSize(2);

		Pair<DynamicComposite, Object> foo = columns.get(0);
		Pair<DynamicComposite, Object> bar = columns.get(1);

		assertThat(foo.left.get(1, STRING_SRZ)).isEqualTo("friends");
		assertThat(foo.right).isEqualTo("foo");
		assertThat(bar.left.get(1, STRING_SRZ)).isEqualTo("friends");
		assertThat(bar.right).isEqualTo("bar");

	}

	@Test
	public void should_persist_with_null_fields() throws Exception
	{
		CompleteBean bean = CompleteBeanTestBuilder.builder().randomId().name("DuyHai").buid();

		em.persist(bean);

		DynamicComposite startCompositeForEagerFetch = new DynamicComposite();
		startCompositeForEagerFetch.addComponent(0, START_EAGER.flag(), ComponentEquality.EQUAL);

		DynamicComposite endCompositeForEagerFetch = new DynamicComposite();
		endCompositeForEagerFetch.addComponent(0, END_EAGER.flag(),
				ComponentEquality.GREATER_THAN_EQUAL);

		List<Pair<DynamicComposite, Object>> columns = dao.findColumnsRange(bean.getId(),
				startCompositeForEagerFetch, endCompositeForEagerFetch, false, 20);

		assertThat(columns).hasSize(1);

		Pair<DynamicComposite, Object> name = columns.get(0);

		assertThat(name.left.get(1, STRING_SRZ)).isEqualTo("name");
		assertThat(name.right).isEqualTo("DuyHai");
	}

	@Test
	public void should_find() throws Exception
	{
		CompleteBean bean = CompleteBeanTestBuilder.builder().randomId().name("Jonathan").age(40L)
				.addFriends("bob").addFollowers("Billy", "Stephen", "Jacky").addPreference(1, "US")
				.addPreference(2, "New York").buid();

		em.persist(bean);

		CompleteBean found = em.find(CompleteBean.class, bean.getId());

		assertThat(found).isNotNull();
		assertThat(found).isInstanceOf(Factory.class);
	}

	@SuppressWarnings(
	{
			"rawtypes",
			"unchecked"
	})
	@Test
	public void should_find_lazy() throws Exception
	{
		CompleteBean bean = CompleteBeanTestBuilder.builder().randomId().name("Jonathan").age(40L)
				.addFriends("bob", "alice").addFollowers("Billy", "Stephen", "Jacky")
				.addPreference(1, "US").addPreference(2, "New York").buid();

		em.persist(bean);

		CompleteBean found = em.find(CompleteBean.class, bean.getId());

		Factory factory = (Factory) found;
		JpaEntityInterceptor interceptor = (JpaEntityInterceptor) factory.getCallback(0);

		Method getFriends = CompleteBean.class.getDeclaredMethod("getFriends", (Class<?>[]) null);
		List<String> lazyFriends = (List<String>) getFriends.invoke(interceptor.getTarget());

		assertThat(lazyFriends).isNull();

		List<String> friends = found.getFriends();

		assertThat(friends).isNotNull();
		assertThat(friends).hasSize(2);
		assertThat(friends).containsExactly("bob", "alice");
	}

	@Test
	public void should_merge_modifications() throws Exception
	{
		CompleteBean bean = CompleteBeanTestBuilder.builder().randomId().name("Jonathan").age(40L)
				.addFriends("bob", "alice").addFollowers("Billy", "Stephen", "Jacky")
				.addPreference(1, "US").addPreference(2, "New York").buid();
		em.persist(bean);

		CompleteBean found = em.find(CompleteBean.class, bean.getId());

		found.setAge(100L);
		found.getFriends().add("eve");
		found.getPreferences().put(1, "FR");

		CompleteBean merged = em.merge(found);

		assertThat(merged.getFriends()).hasSize(3);
		assertThat(merged.getFriends()).containsExactly("bob", "alice", "eve");
		assertThat(merged.getPreferences()).hasSize(2);
		assertThat(merged.getPreferences().get(1)).isEqualTo("FR");

		DynamicComposite startCompositeForEagerFetch = new DynamicComposite();
		startCompositeForEagerFetch.addComponent(0, PropertyType.SIMPLE.flag(),
				ComponentEquality.EQUAL);
		startCompositeForEagerFetch.addComponent(1, "age_in_years", ComponentEquality.EQUAL);
		startCompositeForEagerFetch.addComponent(2, 0, ComponentEquality.EQUAL);

		DynamicComposite endCompositeForEagerFetch = new DynamicComposite();
		endCompositeForEagerFetch.addComponent(0, PropertyType.SIMPLE.flag(),
				ComponentEquality.EQUAL);
		endCompositeForEagerFetch.addComponent(1, "age_in_years", ComponentEquality.EQUAL);
		endCompositeForEagerFetch.addComponent(2, 0, ComponentEquality.GREATER_THAN_EQUAL);

		List<Pair<DynamicComposite, Object>> columns = dao.findColumnsRange(bean.getId(),
				startCompositeForEagerFetch, endCompositeForEagerFetch, false, 20);

		assertThat(columns).hasSize(1);

		Pair<DynamicComposite, Object> age = columns.get(0);

		assertThat(age.left.get(1, STRING_SRZ)).isEqualTo("age_in_years");
		assertThat(age.right).isEqualTo(100L);

		startCompositeForEagerFetch = new DynamicComposite();
		startCompositeForEagerFetch.addComponent(0, PropertyType.LAZY_LIST.flag(),
				ComponentEquality.EQUAL);
		startCompositeForEagerFetch.addComponent(1, "friends", ComponentEquality.EQUAL);
		startCompositeForEagerFetch.addComponent(2, 0, ComponentEquality.EQUAL);

		endCompositeForEagerFetch = new DynamicComposite();
		endCompositeForEagerFetch.addComponent(0, PropertyType.LAZY_LIST.flag(),
				ComponentEquality.EQUAL);
		endCompositeForEagerFetch.addComponent(1, "friends", ComponentEquality.EQUAL);
		endCompositeForEagerFetch.addComponent(2, 2, ComponentEquality.GREATER_THAN_EQUAL);

		columns = dao.findColumnsRange(bean.getId(), startCompositeForEagerFetch,
				endCompositeForEagerFetch, false, 20);

		assertThat(columns).hasSize(3);

		Pair<DynamicComposite, Object> eve = columns.get(2);

		assertThat(eve.left.get(1, STRING_SRZ)).isEqualTo("friends");
		assertThat(eve.right).isEqualTo("eve");

		startCompositeForEagerFetch = new DynamicComposite();
		startCompositeForEagerFetch.addComponent(0, PropertyType.MAP.flag(),
				ComponentEquality.EQUAL);
		startCompositeForEagerFetch.addComponent(1, "preferences", ComponentEquality.EQUAL);
		startCompositeForEagerFetch.addComponent(2, 0, ComponentEquality.EQUAL);

		endCompositeForEagerFetch = new DynamicComposite();
		endCompositeForEagerFetch.addComponent(0, PropertyType.MAP.flag(), ComponentEquality.EQUAL);
		endCompositeForEagerFetch.addComponent(1, "preferences", ComponentEquality.EQUAL);
		endCompositeForEagerFetch.addComponent(2, 2, ComponentEquality.GREATER_THAN_EQUAL);

		columns = dao.findColumnsRange(bean.getId(), startCompositeForEagerFetch,
				endCompositeForEagerFetch, false, 20);

		assertThat(columns).hasSize(2);

		Pair<DynamicComposite, Object> FR = columns.get(0);

		assertThat(FR.left.get(1, STRING_SRZ)).isEqualTo("preferences");
		KeyValueHolder mapValue = (KeyValueHolder) FR.right;
		assertThat(mapValue.getValue()).isEqualTo("FR");
	}

	@Test
	public void should_remove() throws Exception
	{
		CompleteBean bean = CompleteBeanTestBuilder.builder().randomId().name("DuyHai").age(35L)
				.addFriends("foo", "bar").addFollowers("George", "Paul").addPreference(1, "FR")
				.addPreference(2, "Paris").addPreference(3, "75014").buid();

		em.persist(bean);

		em.remove(bean);

		CompleteBean foundBean = em.find(CompleteBean.class, bean.getId());

		assertThat(foundBean).isNull();

		List<Pair<DynamicComposite, Object>> columns = dao.findColumnsRange(bean.getId(), null,
				null, false, 20);

		assertThat(columns).hasSize(0);

	}

	@Test
	public void should_get_reference() throws Exception
	{
		CompleteBean bean = CompleteBeanTestBuilder.builder().randomId().name("DuyHai").age(35L)
				.addFriends("foo", "bar").addFollowers("George", "Paul").addPreference(1, "FR")
				.addPreference(2, "Paris").addPreference(3, "75014").buid();

		em.persist(bean);

		CompleteBean foundBean = em.getReference(CompleteBean.class, bean.getId());

		assertThat(foundBean).isNotNull();
		assertThat(foundBean.getId()).isEqualTo(bean.getId());

	}

	@Test(expected = UnsupportedOperationException.class)
	public void should_exception_on_set_flush_mode() throws Exception
	{
		em.setFlushMode(FlushModeType.COMMIT);
	}

	@Test
	public void should_get_flush_moe() throws Exception
	{
		FlushModeType type = em.getFlushMode();

		assertThat(type).isEqualTo(FlushModeType.AUTO);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void should_exception_on_lock() throws Exception
	{
		em.lock("sdf", LockModeType.READ);
	}

	@Test(expected = IllegalStateException.class)
	public void should_exception_refreshing_non_managed_entity() throws Exception
	{
		em.refresh("test");
	}

	@Test
	public void should_refresh() throws Exception
	{

		CompleteBean bean = CompleteBeanTestBuilder.builder().randomId().name("DuyHai").age(35L)
				.addFriends("foo", "bar").addFollowers("George", "Paul").addPreference(1, "FR")
				.addPreference(2, "Paris").addPreference(3, "75014").buid();

		bean = em.merge(bean);

		bean.getFriends();

		PropertyMeta<Void, String> nameMeta = new SimpleMeta<String>();
		nameMeta.setPropertyName("name");

		DynamicComposite nameComposite = keyFactory.createBaseForInsert(nameMeta, 0);
		dao.setValue(bean.getId(), nameComposite, "DuyHai_modified");

		PropertyMeta<Void, String> listLazyMeta = new ListLazyMeta<String>();
		listLazyMeta.setPropertyName("friends");

		DynamicComposite friend3Composite = keyFactory.createBaseForInsert(listLazyMeta, 2);
		dao.setValue(bean.getId(), friend3Composite, "qux");

		em.refresh(bean);

		assertThat(bean.getName()).isEqualTo("DuyHai_modified");
		assertThat(bean.getFriends()).hasSize(3);
		assertThat(bean.getFriends().get(2)).isEqualTo("qux");

	}

	@Test(expected = UnsupportedOperationException.class)
	public void should_exception_when_contains() throws Exception
	{
		em.contains("sdf");
	}

	@Test(expected = UnsupportedOperationException.class)
	public void should_exception_when_create_query() throws Exception
	{
		em.createQuery("query");
	}

	@Test(expected = UnsupportedOperationException.class)
	public void should_exception_when_create_named_query() throws Exception
	{
		em.createNamedQuery("query");
	}

	@Test(expected = UnsupportedOperationException.class)
	public void should_exception_when_create_native_query() throws Exception
	{
		em.createNativeQuery("query");
	}

	@Test(expected = UnsupportedOperationException.class)
	public void should_exception_when_create_native_query_with_result_class() throws Exception
	{
		em.createNativeQuery("query", String.class);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void should_exception_when_create_native_query_with_result_set_mapping()
			throws Exception
	{
		em.createNativeQuery("query", "mapping");
	}

	@Test
	public void should_get_delegate() throws Exception
	{
		Object delegate = em.getDelegate();

		assertThat(delegate).isSameAs(em);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void should_exception_when_get_transaction() throws Exception
	{
		em.getTransaction();
	}

	@After
	public void tearDown()
	{
		dao.truncate();
	}
}
