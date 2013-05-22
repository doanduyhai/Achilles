package integration.tests;

import static info.archinnov.achilles.common.ThriftCassandraDaoTest.getEntityDao;
import static info.archinnov.achilles.entity.metadata.PropertyType.*;
import static info.archinnov.achilles.serializer.ThriftSerializerUtils.STRING_SRZ;
import static info.archinnov.achilles.table.AchillesTableHelper.normalizerAndValidateColumnFamilyName;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.common.ThriftCassandraDaoTest;
import info.archinnov.achilles.composite.ThriftCompositeFactory;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.entity.manager.ThriftEntityManager;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.proxy.ThriftEntityInterceptor;
import info.archinnov.achilles.type.KeyValue;
import info.archinnov.achilles.type.Pair;
import integration.tests.entity.CompleteBean;
import integration.tests.entity.CompleteBeanTestBuilder;
import integration.tests.entity.Tweet;

import java.lang.reflect.Method;
import java.util.List;

import javax.persistence.FlushModeType;
import javax.persistence.LockModeType;

import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.Composite;
import net.sf.cglib.proxy.Factory;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import testBuilders.TweetTestBuilder;

/**
 * JPAOperationsIT
 * 
 * @author DuyHai DOAN
 * 
 */
public class JPAOperationsIT
{
	@Rule
	public ExpectedException exception = ExpectedException.none();

	private ThriftGenericEntityDao dao = getEntityDao(
			normalizerAndValidateColumnFamilyName(CompleteBean.class.getName()), Long.class);

	private ThriftEntityManager em = ThriftCassandraDaoTest.getEm();

	private ThriftCompositeFactory thriftCompositeFactory = new ThriftCompositeFactory();

	private ObjectMapper objectMapper = new ObjectMapper();

	@Test
	public void should_persist() throws Exception
	{
		CompleteBean bean = CompleteBeanTestBuilder
				.builder()
				.randomId()
				.name("DuyHai")
				.age(35L)
				.addFriends("foo", "bar")
				.addFollowers("George", "Paul")
				.addPreference(1, "FR")
				.addPreference(2, "Paris")
				.addPreference(3, "75014")
				.buid();

		em.persist(bean);

		Composite startCompositeForEagerFetch = new Composite();
		startCompositeForEagerFetch.addComponent(0, START_EAGER.flag(), ComponentEquality.EQUAL);

		Composite endCompositeForEagerFetch = new Composite();
		endCompositeForEagerFetch.addComponent(0, END_EAGER.flag(),
				ComponentEquality.GREATER_THAN_EQUAL);

		List<Pair<Composite, String>> columns = dao.findColumnsRange(bean.getId(),
				startCompositeForEagerFetch, endCompositeForEagerFetch, false, 20);

		assertThat(columns).hasSize(8);

		Pair<Composite, String> serialVersionUID = columns.get(0);

		Pair<Composite, String> age = columns.get(1);

		Pair<Composite, String> name = columns.get(2);

		Pair<Composite, String> George = columns.get(3);
		Pair<Composite, String> Paul = columns.get(4);

		Pair<Composite, String> FR = columns.get(5);
		Pair<Composite, String> Paris = columns.get(6);
		Pair<Composite, String> _75014 = columns.get(7);

		assertThat(serialVersionUID.left.get(1, STRING_SRZ)).isEqualTo(SERIAL_VERSION_UID.name());
		assertThat(Long.parseLong(serialVersionUID.right)).isEqualTo(151L);

		assertThat(age.left.get(1, STRING_SRZ)).isEqualTo("age_in_years");
		assertThat(readLong(age.right)).isEqualTo(35L);

		assertThat(name.left.get(1, STRING_SRZ)).isEqualTo("name");
		assertThat(name.right).isEqualTo("DuyHai");

		assertThat(George.left.get(1, STRING_SRZ)).isEqualTo("followers");
		assertThat(George.right).isIn("George", "Paul");
		assertThat(Paul.left.get(1, STRING_SRZ)).isEqualTo("followers");
		assertThat(Paul.right).isIn("George", "Paul");

		assertThat(FR.left.get(1, STRING_SRZ)).isEqualTo("preferences");
		KeyValue<Integer, String> country = readKeyValue(FR.right);
		assertThat(country.getKey()).isEqualTo(1);
		assertThat(country.getValue()).isEqualTo("FR");

		assertThat(Paris.left.get(1, STRING_SRZ)).isEqualTo("preferences");
		KeyValue<Integer, String> city = readKeyValue(Paris.right);
		assertThat(city.getKey()).isEqualTo(2);
		assertThat(city.getValue()).isEqualTo("Paris");

		assertThat(_75014.left.get(1, STRING_SRZ)).isEqualTo("preferences");
		KeyValue<Integer, String> zipCode = readKeyValue(_75014.right);
		assertThat(zipCode.getKey()).isEqualTo(3);
		assertThat(zipCode.getValue()).isEqualTo("75014");

		startCompositeForEagerFetch = new Composite();
		startCompositeForEagerFetch.addComponent(0, LAZY_LIST.flag(), ComponentEquality.EQUAL);
		startCompositeForEagerFetch.addComponent(1, "friends", ComponentEquality.EQUAL);

		endCompositeForEagerFetch = new Composite();
		endCompositeForEagerFetch.addComponent(0, LAZY_LIST.flag(), ComponentEquality.EQUAL);
		endCompositeForEagerFetch.addComponent(1, "friends", ComponentEquality.GREATER_THAN_EQUAL);

		columns = dao.findColumnsRange(bean.getId(), startCompositeForEagerFetch,
				endCompositeForEagerFetch, false, 20);
		assertThat(columns).hasSize(2);

		Pair<Composite, String> foo = columns.get(0);
		Pair<Composite, String> bar = columns.get(1);

		assertThat(foo.left.get(1, STRING_SRZ)).isEqualTo("friends");
		assertThat(foo.right).isEqualTo("foo");
		assertThat(bar.left.get(1, STRING_SRZ)).isEqualTo("friends");
		assertThat(bar.right).isEqualTo("bar");

	}

	@Test
	public void should_persist_empty_bean() throws Exception
	{
		CompleteBean bean = CompleteBeanTestBuilder.builder().randomId().buid();

		em.persist(bean);

		CompleteBean found = em.find(CompleteBean.class, bean.getId());

		assertThat(found).isNotNull();
	}

	@Test
	public void should_cascade_merge_join_simple_property() throws Exception
	{
		CompleteBean bean = CompleteBeanTestBuilder.builder().randomId().name("DuyHai").buid();
		Tweet welcomeTweet = TweetTestBuilder.tweet().randomId().content("Welcome").buid();

		bean.setWelcomeTweet(welcomeTweet);

		em.merge(bean);

		Tweet persistedWelcomeTweet = em.find(Tweet.class, welcomeTweet.getId());

		assertThat(persistedWelcomeTweet).isNotNull();
		assertThat(persistedWelcomeTweet.getContent()).isEqualTo("Welcome");

	}

	@Test
	public void should_find() throws Exception
	{
		CompleteBean bean = CompleteBeanTestBuilder.builder().randomId().name("Jonathan").buid();

		em.persist(bean);

		CompleteBean found = em.find(CompleteBean.class, bean.getId());

		assertThat(found).isNotNull();
		assertThat(found).isInstanceOf(Factory.class);
	}

	@Test(expected = RuntimeException.class)
	public void should_exception_when_serialVersionUID_changes() throws Exception
	{
		CompleteBean bean = CompleteBeanTestBuilder.builder().randomId().name("Jonathan").buid();

		em.persist(bean);

		Composite composite = new Composite();
		composite.addComponent(0, SERIAL_VERSION_UID.flag(), ComponentEquality.EQUAL);
		composite.addComponent(1, SERIAL_VERSION_UID.name(), ComponentEquality.EQUAL);

		dao.setValue(bean.getId(), composite, "123");

		em.find(CompleteBean.class, bean.getId());

	}

	@SuppressWarnings("rawtypes")
	@Test
	public void should_find_lazy_simple() throws Exception
	{
		CompleteBean bean = CompleteBeanTestBuilder
				.builder()
				.randomId()
				.name("Jonathan")
				.label("label")
				.buid();

		em.persist(bean);

		CompleteBean found = em.find(CompleteBean.class, bean.getId());

		Factory factory = (Factory) found;
		ThriftEntityInterceptor interceptor = (ThriftEntityInterceptor) factory.getCallback(0);

		Method getLabel = CompleteBean.class.getDeclaredMethod("getLabel");
		String label = (String) getLabel.invoke(interceptor.getTarget());

		assertThat(label).isNull();

		String lazyLabel = found.getLabel();

		assertThat(lazyLabel).isNotNull();
		assertThat(lazyLabel).isEqualTo("label");
	}

	@SuppressWarnings(
	{
			"rawtypes",
			"unchecked"
	})
	@Test
	public void should_find_lazy_list() throws Exception
	{
		CompleteBean bean = CompleteBeanTestBuilder
				.builder()
				.randomId()
				.name("Jonathan")
				.age(40L)
				.addFriends("bob", "alice")
				.addFollowers("Billy", "Stephen", "Jacky")
				.addPreference(1, "US")
				.addPreference(2, "New York")
				.buid();

		em.persist(bean);

		CompleteBean found = em.find(CompleteBean.class, bean.getId());

		Factory factory = (Factory) found;
		ThriftEntityInterceptor interceptor = (ThriftEntityInterceptor) factory.getCallback(0);

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
		CompleteBean bean = CompleteBeanTestBuilder
				.builder()
				.randomId()
				.name("Jonathan")
				.age(40L)
				.addFriends("bob", "alice")
				.addFollowers("Billy", "Stephen", "Jacky")
				.addPreference(1, "US")
				.addPreference(2, "New York")
				.buid();
		em.persist(bean);

		CompleteBean found = em.find(CompleteBean.class, bean.getId());

		found.setAge(100L);
		found.getFriends().add("eve");
		found.getPreferences().put(1, "FR");

		CompleteBean merged = em.merge(found);

		assertThat(merged).isSameAs(found);

		assertThat(merged.getFriends()).hasSize(3);
		assertThat(merged.getFriends()).containsExactly("bob", "alice", "eve");
		assertThat(merged.getPreferences()).hasSize(2);
		assertThat(merged.getPreferences().get(1)).isEqualTo("FR");

		Composite composite = new Composite();
		composite.addComponent(0, PropertyType.SIMPLE.flag(), ComponentEquality.EQUAL);
		composite.addComponent(1, "age_in_years", ComponentEquality.EQUAL);
		composite.addComponent(2, 0, ComponentEquality.EQUAL);

		assertThat(readLong(dao.<Long, String> getValue(bean.getId(), composite))).isEqualTo(100L);

		Composite startCompositeForEagerFetch = new Composite();
		startCompositeForEagerFetch.addComponent(0, PropertyType.LAZY_LIST.flag(),
				ComponentEquality.EQUAL);
		startCompositeForEagerFetch.addComponent(1, "friends", ComponentEquality.EQUAL);
		startCompositeForEagerFetch.addComponent(2, 0, ComponentEquality.EQUAL);

		Composite endCompositeForEagerFetch = new Composite();
		endCompositeForEagerFetch.addComponent(0, PropertyType.LAZY_LIST.flag(),
				ComponentEquality.EQUAL);
		endCompositeForEagerFetch.addComponent(1, "friends", ComponentEquality.EQUAL);
		endCompositeForEagerFetch.addComponent(2, 2, ComponentEquality.GREATER_THAN_EQUAL);

		List<Pair<Composite, String>> columns = dao.findColumnsRange(bean.getId(),
				startCompositeForEagerFetch, endCompositeForEagerFetch, false, 20);

		assertThat(columns).hasSize(3);

		Pair<Composite, String> eve = columns.get(2);

		assertThat(eve.left.get(1, STRING_SRZ)).isEqualTo("friends");
		assertThat(eve.right).isEqualTo("eve");

		startCompositeForEagerFetch = new Composite();
		startCompositeForEagerFetch.addComponent(0, PropertyType.MAP.flag(),
				ComponentEquality.EQUAL);
		startCompositeForEagerFetch.addComponent(1, "preferences", ComponentEquality.EQUAL);
		startCompositeForEagerFetch.addComponent(2, 0, ComponentEquality.EQUAL);

		endCompositeForEagerFetch = new Composite();
		endCompositeForEagerFetch.addComponent(0, PropertyType.MAP.flag(), ComponentEquality.EQUAL);
		endCompositeForEagerFetch.addComponent(1, "preferences", ComponentEquality.EQUAL);
		endCompositeForEagerFetch.addComponent(2, 2, ComponentEquality.GREATER_THAN_EQUAL);

		columns = dao.findColumnsRange(bean.getId(), startCompositeForEagerFetch,
				endCompositeForEagerFetch, false, 20);

		assertThat(columns).hasSize(2);

		Pair<Composite, String> FR = columns.get(0);

		assertThat(FR.left.get(1, STRING_SRZ)).isEqualTo("preferences");
		KeyValue<Integer, String> mapValue = readKeyValue(FR.right);
		assertThat(mapValue.getValue()).isEqualTo("FR");
	}

	@Test
	public void should_return_managed_entity_after_merge() throws Exception
	{
		CompleteBean bean = CompleteBeanTestBuilder.builder().randomId().buid();
		bean = em.merge(bean);

		assertThat(bean).isInstanceOf(Factory.class);
	}

	@Test
	public void should_return_same_entity_as_merged_bean_when_managed() throws Exception
	{

		CompleteBean bean = CompleteBeanTestBuilder.builder().randomId().name("Jonathan").buid();
		Tweet tweet = TweetTestBuilder.tweet().randomId().content("tweet").buid();
		bean.setWelcomeTweet(tweet);

		bean = em.merge(bean);

		CompleteBean bean2 = em.merge(bean);

		assertThat(bean2).isSameAs(bean);
		assertThat(bean.getWelcomeTweet()).isInstanceOf(Factory.class);
		assertThat(bean2.getWelcomeTweet()).isInstanceOf(Factory.class);
	}

	@Test
	public void should_remove() throws Exception
	{
		CompleteBean bean = CompleteBeanTestBuilder
				.builder()
				.randomId()
				.name("DuyHai")
				.age(35L)
				.addFriends("foo", "bar")
				.addFollowers("George", "Paul")
				.addPreference(1, "FR")
				.addPreference(2, "Paris")
				.addPreference(3, "75014")
				.buid();

		bean = em.merge(bean);

		em.remove(bean);

		CompleteBean foundBean = em.find(CompleteBean.class, bean.getId());

		assertThat(foundBean).isNull();

		List<Pair<Composite, String>> columns = dao.findColumnsRange(bean.getId(), null, null,
				false, 20);

		assertThat(columns).hasSize(0);

	}

	@Test(expected = IllegalStateException.class)
	public void should_exception_when_removing_transient_entity() throws Exception
	{
		CompleteBean bean = CompleteBeanTestBuilder
				.builder()
				.randomId()
				.name("DuyHai")
				.age(35L)
				.addFriends("foo", "bar")
				.addFollowers("George", "Paul")
				.addPreference(1, "FR")
				.addPreference(2, "Paris")
				.addPreference(3, "75014")
				.buid();

		em.remove(bean);
	}

	@Test
	public void should_get_reference() throws Exception
	{
		CompleteBean bean = CompleteBeanTestBuilder
				.builder()
				.randomId()
				.name("DuyHai")
				.age(35L)
				.addFriends("foo", "bar")
				.addFollowers("George", "Paul")
				.addPreference(1, "FR")
				.addPreference(2, "Paris")
				.addPreference(3, "75014")
				.buid();

		em.persist(bean);

		CompleteBean foundBean = em.getReference(CompleteBean.class, bean.getId());

		assertThat(foundBean).isNotNull();

		// Real object should be empty
		CompleteBean realObject = em.unproxy(foundBean);

		assertThat(realObject.getId()).isEqualTo(bean.getId());
		assertThat(realObject.getName()).isNull();
		assertThat(realObject.getAge()).isNull();
		assertThat(realObject.getFriends()).isNull();
		assertThat(realObject.getFollowers()).isNull();
		assertThat(realObject.getPreferences()).isNull();

		assertThat(foundBean.getId()).isEqualTo(bean.getId());
		assertThat(foundBean.getName()).isEqualTo("DuyHai");
		assertThat(foundBean.getAge()).isEqualTo(35L);
		assertThat(foundBean.getFriends()).containsExactly("foo", "bar");
		assertThat(foundBean.getFollowers()).contains("George", "Paul");

		assertThat(foundBean.getPreferences()).containsKey(1);
		assertThat(foundBean.getPreferences()).containsKey(2);
		assertThat(foundBean.getPreferences()).containsKey(3);

		assertThat(foundBean.getPreferences()).containsValue("FR");
		assertThat(foundBean.getPreferences()).containsValue("Paris");
		assertThat(foundBean.getPreferences()).containsValue("75014");
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

	@Test
	public void should_exception_refreshing_non_managed_entity() throws Exception
	{
		CompleteBean completeBean = CompleteBeanTestBuilder
				.builder()
				.randomId()
				.name("name")
				.buid();
		exception.expect(IllegalStateException.class);
		exception.expectMessage("The entity '" + completeBean + "' is not in 'managed' state");
		em.refresh(completeBean);
	}

	@Test
	public void should_refresh() throws Exception
	{

		CompleteBean bean = CompleteBeanTestBuilder
				.builder()
				.randomId()
				.name("DuyHai")
				.age(35L)
				.addFriends("foo", "bar")
				.addFollowers("George", "Paul")
				.addPreference(1, "FR")
				.addPreference(2, "Paris")
				.addPreference(3, "75014")
				.buid();

		bean = em.merge(bean);

		bean.getFriends();

		PropertyMeta<Void, String> nameMeta = new PropertyMeta<Void, String>();
		nameMeta.setType(PropertyType.SIMPLE);

		nameMeta.setPropertyName("name");

		Composite nameComposite = thriftCompositeFactory.createForBatchInsertSingleValue(nameMeta);
		dao.setValue(bean.getId(), nameComposite, "DuyHai_modified");

		PropertyMeta<Void, String> listLazyMeta = new PropertyMeta<Void, String>();
		listLazyMeta.setType(LAZY_LIST);
		listLazyMeta.setPropertyName("friends");

		Composite friend3Composite = thriftCompositeFactory.createForBatchInsertMultiValue(listLazyMeta,
				2);
		dao.setValue(bean.getId(), friend3Composite, "qux");

		em.refresh(bean);

		assertThat(bean.getName()).isEqualTo("DuyHai_modified");
		assertThat(bean.getFriends()).hasSize(3);
		assertThat(bean.getFriends().get(2)).isEqualTo("qux");

	}

	@Test
	public void should_find_unmapped_field() throws Exception
	{
		CompleteBean bean = CompleteBeanTestBuilder.builder().randomId().name("DuyHai") //
				.label("label")
				.age(35L)
				.addFriends("foo", "bar")
				//
				.addFollowers("George", "Paul")
				.addPreference(1, "FR")
				//
				.addPreference(2, "Paris")
				.addPreference(3, "75014")
				.buid();

		bean = em.merge(bean);

		assertThat(bean.getLabel()).isEqualTo("label");

	}

	@Test
	public void should_return_null_and_not_wrapper_for_null_values() throws Exception
	{
		CompleteBean bean = CompleteBeanTestBuilder.builder().randomId().name("DuyHai") //
				.buid();

		bean.setFriends(null);
		bean.setFollowers(null);
		bean.setPreferences(null);

		em.persist(bean);

		bean = em.find(CompleteBean.class, bean.getId());

		assertThat(bean.getFriends()).isNull();
		assertThat(bean.getFollowers()).isNull();
		assertThat(bean.getPreferences()).isNull();
		assertThat(bean.getLabel()).isNull();
		assertThat(bean.getAge()).isNull();
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

	private Long readLong(String value) throws Exception
	{
		return this.objectMapper.readValue(value, Long.class);
	}

	@SuppressWarnings("unchecked")
	private KeyValue<Integer, String> readKeyValue(String value) throws Exception
	{
		return this.objectMapper.readValue(value, KeyValue.class);
	}

	@After
	public void tearDown()
	{
		dao.truncate();
	}
}
