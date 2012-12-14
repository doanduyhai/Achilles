package fr.doan.achilles.proxy;

import static fr.doan.achilles.serializer.Utils.BYTE_SRZ;
import static fr.doan.achilles.serializer.Utils.INT_SRZ;
import static fr.doan.achilles.serializer.Utils.STRING_SRZ;
import static fr.doan.achilles.serializer.Utils.UUID_SRZ;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import mapping.entity.CompleteBean;
import mapping.entity.TweetMultiKey;
import me.prettyprint.cassandra.model.HColumnImpl;
import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.NoOp;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import fr.doan.achilles.entity.manager.CompleteBeanTestBuilder;
import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.entity.metadata.MultiKeyWideMapMeta;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.metadata.PropertyType;
import fr.doan.achilles.entity.type.KeyValue;
import fr.doan.achilles.proxy.interceptor.JpaInterceptor;
import fr.doan.achilles.serializer.Utils;

@RunWith(MockitoJUnitRunner.class)
public class EntityWrapperUtilTest
{
	private EntityWrapperUtil util = new EntityWrapperUtil();

	@Mock
	private EntityMeta<Long> entityMeta;

	@Mock
	private PropertyMeta<Long> idMeta;

	@Mock
	private MultiKeyWideMapMeta<TweetMultiKey, String> wideMapMeta;

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

		List<Object> multiKeyList = util.determineMultiKey(multiKey,
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

		List<Object> multiKeyList = util.determineMultiKey(multiKey,
				Arrays.asList(idGetter, authorGetter, retweetCountGetter));

		assertThat(multiKeyList).hasSize(3);
		assertThat(multiKeyList.get(0)).isEqualTo(uuid);
		assertThat(multiKeyList.get(1)).isNull();
		assertThat(multiKeyList.get(2)).isEqualTo(12);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_build_multi_key_instance() throws Exception
	{
		Method authorSetter = TweetMultiKey.class.getDeclaredMethod("setAuthor", String.class);
		Method idSetter = TweetMultiKey.class.getDeclaredMethod("setId", UUID.class);
		Method retweetCountSetter = TweetMultiKey.class.getDeclaredMethod("setRetweetCount",
				int.class);

		UUID uuid1 = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
		UUID uuid2 = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
		UUID uuid3 = TimeUUIDUtils.getUniqueTimeUUIDinMillis();

		HColumn<DynamicComposite, Object> hCol1 = buildHColumn(
				buildComposite("author1", uuid1, 11), "val1");
		HColumn<DynamicComposite, Object> hCol2 = buildHColumn(
				buildComposite("author2", uuid2, 12), "val2");
		HColumn<DynamicComposite, Object> hCol3 = buildHColumn(
				buildComposite("author3", uuid3, 13), "val3");

		when(wideMapMeta.get("val1")).thenReturn("val1");
		when(wideMapMeta.get("val2")).thenReturn("val2");
		when(wideMapMeta.get("val3")).thenReturn("val3");

		when(wideMapMeta.getComponentSerializers()).thenReturn(
				Arrays.asList((Serializer<?>) STRING_SRZ, UUID_SRZ, INT_SRZ));

		List<KeyValue<TweetMultiKey, String>> multiKeys = util.buildMultiKeyList(TweetMultiKey.class,
				wideMapMeta, //
				Arrays.asList(hCol1, hCol2, hCol3), //
				Arrays.asList(authorSetter, idSetter, retweetCountSetter));

		assertThat(multiKeys).hasSize(3);

		assertThat(multiKeys.get(0).getKey().getAuthor()).isEqualTo("author1");
		assertThat(multiKeys.get(0).getKey().getId()).isEqualTo(uuid1);
		assertThat(multiKeys.get(0).getKey().getRetweetCount()).isEqualTo(11);
		assertThat(multiKeys.get(0).getValue()).isEqualTo("val1");

		assertThat(multiKeys.get(1).getKey().getAuthor()).isEqualTo("author2");
		assertThat(multiKeys.get(1).getKey().getId()).isEqualTo(uuid2);
		assertThat(multiKeys.get(1).getKey().getRetweetCount()).isEqualTo(12);
		assertThat(multiKeys.get(1).getValue()).isEqualTo("val2");

		assertThat(multiKeys.get(2).getKey().getAuthor()).isEqualTo("author3");
		assertThat(multiKeys.get(2).getKey().getId()).isEqualTo(uuid3);
		assertThat(multiKeys.get(2).getKey().getRetweetCount()).isEqualTo(13);
		assertThat(multiKeys.get(2).getValue()).isEqualTo("val3");
	}

	private DynamicComposite buildComposite(String author, UUID uuid, int retweetCount)
	{
		DynamicComposite composite = new DynamicComposite();
		composite.setComponent(0, PropertyType.WIDE_MAP.flag(), BYTE_SRZ, BYTE_SRZ
				.getComparatorType().getTypeName());
		composite.setComponent(1, "multiKey1", STRING_SRZ, STRING_SRZ.getComparatorType()
				.getTypeName());
		composite.setComponent(2, author, STRING_SRZ, STRING_SRZ.getComparatorType().getTypeName());
		composite.setComponent(3, uuid, UUID_SRZ, UUID_SRZ.getComparatorType().getTypeName());
		composite.setComponent(4, retweetCount, INT_SRZ, INT_SRZ.getComparatorType().getTypeName());

		return composite;
	}

	private HColumn<DynamicComposite, Object> buildHColumn(DynamicComposite comp, String value)
	{
		HColumn<DynamicComposite, Object> hColumn = new HColumnImpl<DynamicComposite, Object>(
				Utils.DYNA_COMP_SRZ, Utils.OBJECT_SRZ);

		hColumn.setName(comp);
		hColumn.setValue(value);
		return hColumn;
	}
}
