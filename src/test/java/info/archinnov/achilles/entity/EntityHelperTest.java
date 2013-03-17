package info.archinnov.achilles.entity;

import static info.archinnov.achilles.entity.metadata.PropertyType.SIMPLE;
import static info.archinnov.achilles.entity.metadata.factory.PropertyMetaFactory.factory;
import static info.archinnov.achilles.entity.type.ConsistencyLevel.ALL;
import static info.archinnov.achilles.entity.type.ConsistencyLevel.ANY;
import static info.archinnov.achilles.entity.type.ConsistencyLevel.EACH_QUORUM;
import static info.archinnov.achilles.entity.type.ConsistencyLevel.LOCAL_QUORUM;
import static info.archinnov.achilles.entity.type.ConsistencyLevel.ONE;
import static info.archinnov.achilles.entity.type.ConsistencyLevel.QUORUM;
import static info.archinnov.achilles.entity.type.ConsistencyLevel.THREE;
import static info.archinnov.achilles.entity.type.ConsistencyLevel.TWO;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.annotations.Consistency;
import info.archinnov.achilles.dao.GenericDynamicCompositeDao;
import info.archinnov.achilles.dao.Pair;
import info.archinnov.achilles.entity.manager.CompleteBeanTestBuilder;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.type.ConsistencyLevel;
import info.archinnov.achilles.exception.BeanMappingException;
import info.archinnov.achilles.proxy.interceptor.JpaEntityInterceptor;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.Table;

import mapping.entity.CompleteBean;
import mapping.entity.TweetMultiKey;
import mapping.entity.UserBean;
import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import me.prettyprint.hector.api.HConsistencyLevel;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.Factory;
import net.sf.cglib.proxy.NoOp;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import parser.entity.BeanWithColumnFamilyName;
import parser.entity.ChildBean;

/**
 * EntityHelperTest
 * 
 * @author DuyHai DOAN
 * 
 */
@SuppressWarnings("unused")
@RunWith(MockitoJUnitRunner.class)
public class EntityHelperTest
{

	@Rule
	public ExpectedException expectedEx = ExpectedException.none();

	@Mock
	private EntityMeta<Long> entityMeta;

	@Mock
	private PropertyMeta<Void, Long> idMeta;

	@Mock
	private PropertyMeta<TweetMultiKey, String> wideMapMeta;

	@Mock
	private Map<Method, PropertyMeta<?, ?>> getterMetas;

	@Mock
	private Map<Method, PropertyMeta<?, ?>> setterMetas;

	@Mock
	private GenericDynamicCompositeDao<Long> dao;

	private final EntityHelper helper = new EntityHelper();

	@Test
	public void should_derive_getter() throws Exception
	{

		class Test
		{

			Boolean old;
		}

		String[] getterNames = helper.deriveGetterName(Test.class.getDeclaredField("old"));
		assertThat(getterNames).hasSize(1);
		assertThat(getterNames[0]).isEqualTo("getOld");
	}

	@Test
	public void should_derive_getter_for_boolean_primitive() throws Exception
	{

		class Test
		{

			boolean old;
		}

		String[] getterNames = helper.deriveGetterName(Test.class.getDeclaredField("old"));
		assertThat(getterNames).hasSize(2);
		assertThat(getterNames[0]).isEqualTo("isOld");
		assertThat(getterNames[1]).isEqualTo("getOld");
	}

	@Test
	public void should_derive_setter() throws Exception
	{
		class Test
		{

			boolean a;
		}

		assertThat(helper.deriveSetterName("a")).isEqualTo("setA");
	}

	@Test
	public void should_exception_when_no_getter() throws Exception
	{

		class Test
		{
			String name;
		}

		expectedEx.expect(BeanMappingException.class);
		expectedEx.expectMessage("The getter for field 'name' does not exist");

		helper.findGetter(Test.class, Test.class.getDeclaredField("name"));
	}

	@Test
	public void should_exception_when_no_setter() throws Exception
	{

		class Test
		{
			String name;

			public String getA()
			{
				return name;
			}
		}

		expectedEx.expect(BeanMappingException.class);
		expectedEx.expectMessage("The setter for field 'name' does not exist");

		helper.findSetter(Test.class, Test.class.getDeclaredField("name"));
	}

	@Test
	public void should_exception_when_incorrect_getter() throws Exception
	{

		class Test
		{
			String name;

			public Long getName()
			{
				return 1L;
			}

		}
		expectedEx.expect(BeanMappingException.class);
		expectedEx.expectMessage("The getter for field 'name' does not return correct type");

		helper.findGetter(Test.class, Test.class.getDeclaredField("name"));
	}

	@Test
	public void should_exception_when_setter_returning_wrong_type() throws Exception
	{

		class Test
		{
			String name;

			public String getName()
			{
				return name;
			}

			public Long setName(String name)
			{
				return 1L;
			}

		}
		expectedEx.expect(BeanMappingException.class);
		expectedEx
				.expectMessage("The setter for field 'name' does not return correct type or does not have the correct parameter");

		helper.findSetter(Test.class, Test.class.getDeclaredField("name"));
	}

	@Test
	public void should_exception_when_setter_taking_wrong_type() throws Exception
	{

		class Test
		{
			String name;

			public String getName()
			{
				return name;
			}

			public void setName(Long name)
			{}

		}

		expectedEx.expect(BeanMappingException.class);
		expectedEx.expectMessage("The setter for field 'name' does not exist or is incorrect");

		helper.findSetter(Test.class, Test.class.getDeclaredField("name"));
	}

	@Test
	public void should_find_getter_from_boolean_as_isOld() throws Exception
	{
		class Test
		{
			boolean old;

			public boolean isOld()
			{
				return old;
			}

			public void setOld(boolean old)
			{
				this.old = old;
			}
		}

		Method[] accessors = helper.findAccessors(Test.class, Test.class.getDeclaredField("old"));

		assertThat(accessors[0].getName()).isEqualTo("isOld");
	}

	@Test
	public void should_find_getter_from_boolean_as_getOld() throws Exception
	{
		class Test
		{
			boolean old;

			public boolean getOld()
			{
				return old;
			}

			public void setOld(boolean old)
			{
				this.old = old;
			}
		}

		Method[] accessors = helper.findAccessors(Test.class, Test.class.getDeclaredField("old"));

		assertThat(accessors[0].getName()).isEqualTo("getOld");
	}

	@Test
	public void should_find_accessors() throws Exception
	{

		Method[] accessors = helper.findAccessors(Bean.class,
				Bean.class.getDeclaredField("complicatedAttributeName"));

		assertThat(accessors).hasSize(2);
		assertThat(accessors[0].getName()).isEqualTo("getComplicatedAttributeName");
		assertThat(accessors[1].getName()).isEqualTo("setComplicatedAttributeName");
	}

	@Test
	public void should_find_accessors_from_collection_types() throws Exception
	{

		Method[] accessors = helper.findAccessors(ComplexBean.class,
				ComplexBean.class.getDeclaredField("friends"));

		assertThat(accessors).hasSize(2);
		assertThat(accessors[0].getName()).isEqualTo("getFriends");
		assertThat(accessors[1].getName()).isEqualTo("setFriends");
	}

	@Test
	public void should_find_accessors_from_widemap_type() throws Exception
	{
		Method[] accessors = helper.findAccessors(CompleteBean.class,
				CompleteBean.class.getDeclaredField("tweets"));

		assertThat(accessors).hasSize(2);
		assertThat(accessors[0].getName()).isEqualTo("getTweets");
		assertThat(accessors[1]).isNull();
	}

	@Test
	public void should_get_value_from_field() throws Exception
	{
		Bean bean = new Bean();
		bean.setComplicatedAttributeName("test");
		Method getter = Bean.class.getDeclaredMethod("getComplicatedAttributeName");

		String value = (String) helper.getValueFromField(bean, getter);
		assertThat(value).isEqualTo("test");
	}

	@Test
	public void should_get_value_from_null_field() throws Exception
	{
		Method getter = Bean.class.getDeclaredMethod("getComplicatedAttributeName");
		assertThat(helper.getValueFromField(null, getter)).isNull();
	}

	@Test
	public void should_set_value_to_field() throws Exception
	{
		Bean bean = new Bean();
		Method setter = Bean.class.getDeclaredMethod("setComplicatedAttributeName", String.class);

		helper.setValueToField(bean, setter, "fecezzef");

		assertThat(bean.getComplicatedAttributeName()).isEqualTo("fecezzef");
	}

	@Test
	public void should_not_set_value_when_null_field() throws Exception
	{
		Method setter = Bean.class.getDeclaredMethod("setComplicatedAttributeName", String.class);
		helper.setValueToField(null, setter, "fecezzef");
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_get_value_from_collection_field() throws Exception
	{
		ComplexBean bean = new ComplexBean();
		bean.setFriends(Arrays.asList("foo", "bar"));
		Method getter = ComplexBean.class.getDeclaredMethod("getFriends");

		List<String> value = (List<String>) helper.getValueFromField(bean, getter);
		assertThat(value).containsExactly("foo", "bar");
	}

	@Test
	public void should_get_key() throws Exception
	{
		Bean bean = new Bean();
		bean.setComplicatedAttributeName("test");

		Method[] accessors = helper.findAccessors(Bean.class,
				Bean.class.getDeclaredField("complicatedAttributeName"));
		PropertyMeta<Void, String> idMeta = factory(Void.class, String.class).type(SIMPLE)
				.propertyName("complicatedAttributeName").accessors(accessors).build();

		String key = helper.getKey(bean, idMeta);
		assertThat(key).isEqualTo("test");
	}

	@Test
	public void should_return_null_key_when_null_entity() throws Exception
	{
		PropertyMeta<Void, String> idMeta = new PropertyMeta<Void, String>();
		assertThat(helper.getKey(null, idMeta)).isNull();
	}

	@Test
	public void should_get_inherited_fields() throws Exception
	{
		List<Field> fields = helper.getInheritedPrivateFields(ChildBean.class);

		assertThat(fields).hasSize(4);
		assertThat(fields.get(0).getName()).isEqualTo("nickname");
		assertThat(fields.get(1).getName()).isEqualTo("name");
		assertThat(fields.get(2).getName()).isEqualTo("address");
		assertThat(fields.get(3).getName()).isEqualTo("id");
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Test
	public void should_get_inherited_field_by_annotation() throws Exception
	{
		Field id = helper.getInheritedPrivateFields(ChildBean.class, Id.class);

		assertThat(id.getName()).isEqualTo("id");
		assertThat(id.getType()).isEqualTo((Class) Long.class);
	}

	@Test
	public void should_not_get_inherited_field_by_annotation_when_no_match() throws Exception
	{
		assertThat(helper.getInheritedPrivateFields(ChildBean.class, javax.persistence.Basic.class))
				.isNull();
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Test
	public void should_get_inherited_field_by_annotation_and_name() throws Exception
	{
		Field address = helper.getInheritedPrivateFields(ChildBean.class, Column.class, "address");

		assertThat(address.getName()).isEqualTo("address");
		assertThat(address.getType()).isEqualTo((Class) String.class);
	}

	@Test
	public void should_not_get_inherited_field_by_annotation_and_name_when_no_match()
			throws Exception
	{
		assertThat(
				helper.getInheritedPrivateFields(ChildBean.class, javax.persistence.Basic.class,
						"address")).isNull();
	}

	@Test
	public void should_find_serial_version_UID() throws Exception
	{
		class Test
		{
			private static final long serialVersionUID = 1542L;
		}

		Long serialUID = helper.findSerialVersionUID(Test.class);
		assertThat(serialUID).isEqualTo(1542L);
	}

	@Test
	public void should_exception_when_no_serial_version_UID() throws Exception
	{
		class Test
		{
			private static final long fieldName = 1542L;
		}

		expectedEx.expect(BeanMappingException.class);
		expectedEx
				.expectMessage("The 'serialVersionUID' property should be declared for entity 'null'");

		helper.findSerialVersionUID(Test.class);
	}

	@Test
	public void should_infer_column_family_from_annotation() throws Exception
	{
		String cfName = helper.inferColumnFamilyName(BeanWithColumnFamilyName.class,
				"canonicalName");
		assertThat(cfName).isEqualTo("myOwnCF");
	}

	@Test
	public void should_infer_column_family_from_default_name() throws Exception
	{
		String cfName = helper.inferColumnFamilyName(CompleteBean.class, "canonicalName");
		assertThat(cfName).isEqualTo("canonicalName");
	}

	@Test
	public void should_infer_column_family_from_default_name_when_empty_annotation_name()
			throws Exception
	{
		@Table(name = "")
		class Test
		{

		}
		String cfName = helper.inferColumnFamilyName(Test.class, "canonicalName");
		assertThat(cfName).isEqualTo("canonicalName");
	}

	@Test
	public void should_proxy_true() throws Exception
	{
		Enhancer enhancer = new Enhancer();

		enhancer.setSuperclass(CompleteBean.class);
		enhancer.setCallback(NoOp.INSTANCE);

		CompleteBean proxy = (CompleteBean) enhancer.create();

		assertThat(helper.isProxy(proxy)).isTrue();
	}

	@Test
	public void should_proxy_false() throws Exception
	{
		CompleteBean bean = CompleteBeanTestBuilder.builder().id(1L).buid();
		assertThat(helper.isProxy(bean)).isFalse();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_derive_base_class() throws Exception
	{
		CompleteBean entity = CompleteBeanTestBuilder.builder().id(1L).buid();
		Enhancer enhancer = new Enhancer();
		enhancer.setSuperclass(entity.getClass());

		JpaEntityInterceptor<Long, CompleteBean> interceptor = new JpaEntityInterceptor<Long, CompleteBean>();
		interceptor.setTarget(entity);

		enhancer.setCallback(interceptor);

		CompleteBean proxy = (CompleteBean) enhancer.create();

		assertThat((Class<CompleteBean>) helper.deriveBaseClass(proxy)).isEqualTo(
				CompleteBean.class);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_derive_base_class_from_transient() throws Exception
	{
		assertThat((Class<CompleteBean>) helper.deriveBaseClass(new CompleteBean())).isEqualTo(
				CompleteBean.class);
	}

	@Test
	public void should_determine_primary_key() throws Exception
	{
		Method idGetter = CompleteBean.class.getDeclaredMethod("getId");

		when(entityMeta.getIdMeta()).thenReturn(idMeta);
		when(idMeta.getGetter()).thenReturn(idGetter);

		CompleteBean bean = CompleteBeanTestBuilder.builder().id(12L).buid();

		Object key = helper.determinePrimaryKey(bean, entityMeta);

		assertThat(key).isEqualTo(12L);
	}

	@Test
	public void should_return_null_for_primary_key_when_exception() throws Exception
	{
		when(entityMeta.getIdMeta()).thenThrow(new RuntimeException());

		assertThat(helper.determinePrimaryKey(new CompleteBean(), entityMeta)).isNull();
	}

	@Test
	public void should_determine_null_primary_key() throws Exception
	{
		Method idGetter = CompleteBean.class.getDeclaredMethod("getId");

		when(entityMeta.getIdMeta()).thenReturn(idMeta);
		when(idMeta.getGetter()).thenReturn(idGetter);

		CompleteBean bean = CompleteBeanTestBuilder.builder().buid();

		Object key = helper.determinePrimaryKey(bean, entityMeta);

		assertThat(key).isNull();

	}

	@Test
	public void should_determine_multikey() throws Exception
	{
		Method idGetter = TweetMultiKey.class.getDeclaredMethod("getId");
		Method authorGetter = TweetMultiKey.class.getDeclaredMethod("getAuthor");
		Method retweetCountGetter = TweetMultiKey.class.getDeclaredMethod("getRetweetCount");

		TweetMultiKey multiKey = new TweetMultiKey();
		UUID uuid = TimeUUIDUtils.getUniqueTimeUUIDinMillis();

		multiKey.setId(uuid);
		multiKey.setAuthor("author");
		multiKey.setRetweetCount(12);

		List<Object> multiKeyList = helper.determineMultiKey(multiKey,
				Arrays.asList(idGetter, authorGetter, retweetCountGetter));

		assertThat(multiKeyList).hasSize(3);
		assertThat(multiKeyList.get(0)).isEqualTo(uuid);
		assertThat(multiKeyList.get(1)).isEqualTo("author");
		assertThat(multiKeyList.get(2)).isEqualTo(12);
	}

	@Test
	public void should_determine_multikey_with_null() throws Exception
	{
		Method idGetter = TweetMultiKey.class.getDeclaredMethod("getId");
		Method authorGetter = TweetMultiKey.class.getDeclaredMethod("getAuthor");
		Method retweetCountGetter = TweetMultiKey.class.getDeclaredMethod("getRetweetCount");

		TweetMultiKey multiKey = new TweetMultiKey();
		UUID uuid = TimeUUIDUtils.getUniqueTimeUUIDinMillis();

		multiKey.setId(uuid);
		multiKey.setRetweetCount(12);

		List<Object> multiKeyList = helper.determineMultiKey(multiKey,
				Arrays.asList(idGetter, authorGetter, retweetCountGetter));

		assertThat(multiKeyList).hasSize(3);
		assertThat(multiKeyList.get(0)).isEqualTo(uuid);
		assertThat(multiKeyList.get(1)).isNull();
		assertThat(multiKeyList.get(2)).isEqualTo(12);
	}

	@Test
	public void should_return_empty_multikey_when_null_entity() throws Exception
	{
		assertThat(helper.determineMultiKey(null, new ArrayList<Method>())).isEmpty();
	}

	@Test
	public void should_build_proxy() throws Exception
	{
		Method idGetter = CompleteBean.class.getDeclaredMethod("getId", (Class<?>[]) null);
		Method idSetter = CompleteBean.class.getDeclaredMethod("setId", Long.class);

		CompleteBean entity = CompleteBeanTestBuilder.builder().id(1L).name("name").buid();

		when(entityMeta.getIdMeta()).thenReturn(idMeta);
		when(idMeta.getGetter()).thenReturn(idGetter);
		when(idMeta.getSetter()).thenReturn(idSetter);

		when(entityMeta.getGetterMetas()).thenReturn(getterMetas);
		when(entityMeta.getSetterMetas()).thenReturn(setterMetas);
		when(entityMeta.getEntityDao()).thenReturn(dao);
		when(entityMeta.getIdMeta()).thenReturn(idMeta);

		when(idMeta.getGetter()).thenReturn(idGetter);
		when(idMeta.getSetter()).thenReturn(idSetter);

		CompleteBean proxy = helper.buildProxy(entity, entityMeta);

		assertThat(proxy).isNotNull();
		assertThat(proxy).isInstanceOf(Factory.class);
		Factory factory = (Factory) proxy;

		assertThat(factory.getCallbacks()).hasSize(1);
		assertThat(factory.getCallback(0)).isInstanceOf(JpaEntityInterceptor.class);

	}

	@Test
	public void should_build_null_proxy() throws Exception
	{
		assertThat(helper.buildProxy(null, entityMeta)).isNull();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_get_real_object_from_proxy() throws Exception
	{
		UserBean realObject = new UserBean();
		JpaEntityInterceptor<Long, UserBean> interceptor = mock(JpaEntityInterceptor.class);
		when(interceptor.getTarget()).thenReturn(realObject);

		Enhancer enhancer = new Enhancer();
		enhancer.setSuperclass(UserBean.class);
		enhancer.setCallback(interceptor);
		UserBean proxy = (UserBean) enhancer.create();

		UserBean actual = helper.getRealObject(proxy);

		assertThat(actual).isSameAs(realObject);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_get_interceptor_from_proxy() throws Exception
	{
		JpaEntityInterceptor<Long, UserBean> interceptor = mock(JpaEntityInterceptor.class);

		Enhancer enhancer = new Enhancer();
		enhancer.setSuperclass(UserBean.class);
		enhancer.setCallback(interceptor);
		UserBean proxy = (UserBean) enhancer.create();

		JpaEntityInterceptor<Long, UserBean> actual = helper.getInterceptor(proxy);

		assertThat(actual).isSameAs(interceptor);

	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_ensure_proxy() throws Exception
	{
		JpaEntityInterceptor<Long, UserBean> interceptor = mock(JpaEntityInterceptor.class);

		Enhancer enhancer = new Enhancer();
		enhancer.setSuperclass(UserBean.class);
		enhancer.setCallback(interceptor);
		UserBean proxy = (UserBean) enhancer.create();

		helper.ensureProxy(proxy);
	}

	@Test(expected = IllegalStateException.class)
	public void should_exception_when_not_proxy() throws Exception
	{
		helper.ensureProxy(new CompleteBean());
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_unproxy_entity() throws Exception
	{
		CompleteBean realObject = new CompleteBean();

		JpaEntityInterceptor<Long, CompleteBean> interceptor = mock(JpaEntityInterceptor.class);

		Enhancer enhancer = new Enhancer();
		enhancer.setSuperclass(CompleteBean.class);
		enhancer.setCallback(interceptor);
		CompleteBean proxy = (CompleteBean) enhancer.create();

		when(interceptor.getTarget()).thenReturn(realObject);

		CompleteBean actual = helper.unproxy(proxy);

		assertThat(actual).isSameAs(realObject);
	}

	@Test
	public void should_return_same_entity_when_calling_unproxy_on_non_proxified_entity()
			throws Exception
	{
		CompleteBean realObject = new CompleteBean();
		CompleteBean actual = helper.unproxy(realObject);

		assertThat(actual).isSameAs(realObject);
	}

	@Test
	public void should_unproxy_real_entryset() throws Exception
	{
		Map<Integer, CompleteBean> map = new HashMap<Integer, CompleteBean>();
		map.put(1, new CompleteBean());
		Entry<Integer, CompleteBean> entry = map.entrySet().iterator().next();

		assertThat(helper.unproxy(entry)).isSameAs(entry);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_unproxy_entryset_containing_proxy() throws Exception
	{
		CompleteBean realObject = new CompleteBean();
		JpaEntityInterceptor<Long, CompleteBean> interceptor = mock(JpaEntityInterceptor.class);
		Enhancer enhancer = new Enhancer();
		enhancer.setSuperclass(CompleteBean.class);
		enhancer.setCallback(interceptor);
		CompleteBean proxy = (CompleteBean) enhancer.create();

		Map<Integer, CompleteBean> map = new HashMap<Integer, CompleteBean>();
		map.put(1, proxy);
		Entry<Integer, CompleteBean> entry = map.entrySet().iterator().next();

		when(interceptor.getTarget()).thenReturn(realObject);

		assertThat(helper.unproxy(entry).getValue()).isSameAs(realObject);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_unproxy_collection_of_entities() throws Exception
	{
		CompleteBean realObject = new CompleteBean();
		Collection<CompleteBean> proxies = new ArrayList<CompleteBean>();

		JpaEntityInterceptor<Long, CompleteBean> interceptor = mock(JpaEntityInterceptor.class);

		Enhancer enhancer = new Enhancer();
		enhancer.setSuperclass(CompleteBean.class);
		enhancer.setCallback(interceptor);
		CompleteBean proxy = (CompleteBean) enhancer.create();
		proxies.add(proxy);

		when(interceptor.getTarget()).thenReturn(realObject);

		Collection<CompleteBean> actual = helper.unproxy(proxies);

		assertThat(actual).containsExactly(realObject);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_unproxy_list_of_entities() throws Exception
	{
		CompleteBean realObject = new CompleteBean();
		List<CompleteBean> proxies = new ArrayList<CompleteBean>();

		JpaEntityInterceptor<Long, CompleteBean> interceptor = mock(JpaEntityInterceptor.class);

		Enhancer enhancer = new Enhancer();
		enhancer.setSuperclass(CompleteBean.class);
		enhancer.setCallback(interceptor);
		CompleteBean proxy = (CompleteBean) enhancer.create();
		proxies.add(proxy);

		when(interceptor.getTarget()).thenReturn(realObject);

		List<CompleteBean> actual = helper.unproxy(proxies);

		assertThat(actual).containsExactly(realObject);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_unproxy_set_of_entities() throws Exception
	{
		CompleteBean realObject = new CompleteBean();
		Set<CompleteBean> proxies = new HashSet<CompleteBean>();

		JpaEntityInterceptor<Long, CompleteBean> interceptor = mock(JpaEntityInterceptor.class);

		Enhancer enhancer = new Enhancer();
		enhancer.setSuperclass(CompleteBean.class);
		enhancer.setCallback(interceptor);
		CompleteBean proxy = (CompleteBean) enhancer.create();
		proxies.add(proxy);

		when(interceptor.getTarget()).thenReturn(realObject);

		Set<CompleteBean> actual = helper.unproxy(proxies);

		assertThat(actual).containsExactly(realObject);
	}

	@Test
	public void should_find_any_any_consistency_level() throws Exception
	{
		@Consistency(read = ANY, write = ANY)
		class Test
		{}

		Pair<Pair<ConsistencyLevel, ConsistencyLevel>, Pair<HConsistencyLevel, HConsistencyLevel>> levels = helper
				.findConsistencyLevels(Test.class);

		assertThat(levels.left.left).isEqualTo(ANY);
		assertThat(levels.left.right).isEqualTo(ANY);
		assertThat(levels.right.left).isEqualTo(HConsistencyLevel.ANY);
		assertThat(levels.right.right).isEqualTo(HConsistencyLevel.ANY);
	}

	@Test
	public void should_find_one_one_consistency_level() throws Exception
	{
		@Consistency(read = ONE, write = ONE)
		class Test
		{}

		Pair<Pair<ConsistencyLevel, ConsistencyLevel>, Pair<HConsistencyLevel, HConsistencyLevel>> levels = helper
				.findConsistencyLevels(Test.class);

		assertThat(levels.left.left).isEqualTo(ONE);
		assertThat(levels.left.right).isEqualTo(ONE);
		assertThat(levels.right.left).isEqualTo(HConsistencyLevel.ONE);
		assertThat(levels.right.right).isEqualTo(HConsistencyLevel.ONE);
	}

	@Test
	public void should_find_two_two_consistency_level() throws Exception
	{
		@Consistency(read = TWO, write = TWO)
		class Test
		{}

		Pair<Pair<ConsistencyLevel, ConsistencyLevel>, Pair<HConsistencyLevel, HConsistencyLevel>> levels = helper
				.findConsistencyLevels(Test.class);

		assertThat(levels.left.left).isEqualTo(TWO);
		assertThat(levels.left.right).isEqualTo(TWO);
		assertThat(levels.right.left).isEqualTo(HConsistencyLevel.TWO);
		assertThat(levels.right.right).isEqualTo(HConsistencyLevel.TWO);
	}

	@Test
	public void should_find_three_three_consistency_level() throws Exception
	{
		@Consistency(read = THREE, write = THREE)
		class Test
		{}

		Pair<Pair<ConsistencyLevel, ConsistencyLevel>, Pair<HConsistencyLevel, HConsistencyLevel>> levels = helper
				.findConsistencyLevels(Test.class);

		assertThat(levels.left.left).isEqualTo(THREE);
		assertThat(levels.left.right).isEqualTo(THREE);
		assertThat(levels.right.left).isEqualTo(HConsistencyLevel.THREE);
		assertThat(levels.right.right).isEqualTo(HConsistencyLevel.THREE);
	}

	@Test
	public void should_find_local_quorum_local_quorum_consistency_level() throws Exception
	{
		@Consistency(read = LOCAL_QUORUM, write = LOCAL_QUORUM)
		class Test
		{}

		Pair<Pair<ConsistencyLevel, ConsistencyLevel>, Pair<HConsistencyLevel, HConsistencyLevel>> levels = helper
				.findConsistencyLevels(Test.class);

		assertThat(levels.left.left).isEqualTo(LOCAL_QUORUM);
		assertThat(levels.left.right).isEqualTo(LOCAL_QUORUM);
		assertThat(levels.right.left).isEqualTo(HConsistencyLevel.LOCAL_QUORUM);
		assertThat(levels.right.right).isEqualTo(HConsistencyLevel.LOCAL_QUORUM);
	}

	@Test
	public void should_find_each_quorum_each_quorum_consistency_level() throws Exception
	{
		@Consistency(read = EACH_QUORUM, write = EACH_QUORUM)
		class Test
		{}

		Pair<Pair<ConsistencyLevel, ConsistencyLevel>, Pair<HConsistencyLevel, HConsistencyLevel>> levels = helper
				.findConsistencyLevels(Test.class);

		assertThat(levels.left.left).isEqualTo(EACH_QUORUM);
		assertThat(levels.left.right).isEqualTo(EACH_QUORUM);
		assertThat(levels.right.left).isEqualTo(HConsistencyLevel.EACH_QUORUM);
		assertThat(levels.right.right).isEqualTo(HConsistencyLevel.EACH_QUORUM);
	}

	@Test
	public void should_find_quorum_quorum_consistency_level() throws Exception
	{
		@Consistency(read = QUORUM, write = QUORUM)
		class Test
		{}

		Pair<Pair<ConsistencyLevel, ConsistencyLevel>, Pair<HConsistencyLevel, HConsistencyLevel>> levels = helper
				.findConsistencyLevels(Test.class);

		assertThat(levels.left.left).isEqualTo(QUORUM);
		assertThat(levels.left.right).isEqualTo(QUORUM);
		assertThat(levels.right.left).isEqualTo(HConsistencyLevel.QUORUM);
		assertThat(levels.right.right).isEqualTo(HConsistencyLevel.QUORUM);
	}

	@Test
	public void should_find_all_all_consistency_level() throws Exception
	{
		@Consistency(read = ALL, write = ALL)
		class Test
		{}

		Pair<Pair<ConsistencyLevel, ConsistencyLevel>, Pair<HConsistencyLevel, HConsistencyLevel>> levels = helper
				.findConsistencyLevels(Test.class);

		assertThat(levels.left.left).isEqualTo(ALL);
		assertThat(levels.left.right).isEqualTo(ALL);
		assertThat(levels.right.left).isEqualTo(HConsistencyLevel.ALL);
		assertThat(levels.right.right).isEqualTo(HConsistencyLevel.ALL);
	}

	@Test
	public void should_find_one_one_consistency_level_by_default() throws Exception
	{
		class Test
		{}

		Pair<Pair<ConsistencyLevel, ConsistencyLevel>, Pair<HConsistencyLevel, HConsistencyLevel>> levels = helper
				.findConsistencyLevels(Test.class);

		assertThat(levels.right.left).isEqualTo(HConsistencyLevel.ONE);
		assertThat(levels.right.right).isEqualTo(HConsistencyLevel.ONE);
	}

	class Bean
	{

		private String complicatedAttributeName;

		public String getComplicatedAttributeName()
		{
			return complicatedAttributeName;
		}

		public void setComplicatedAttributeName(String complicatedAttributeName)
		{
			this.complicatedAttributeName = complicatedAttributeName;
		}
	}

	class ComplexBean
	{
		private List<String> friends;

		public List<String> getFriends()
		{
			return friends;
		}

		public void setFriends(List<String> friends)
		{
			this.friends = friends;
		}
	}
}
