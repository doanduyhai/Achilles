package fr.doan.achilles.holder.factory;

import static fr.doan.achilles.serializer.SerializerUtils.INT_SRZ;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;
import static testBuilders.PropertyMetaTestBuilder.noClass;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mapping.entity.TweetMultiKey;
import mapping.entity.UserBean;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import testBuilders.CompositeTestBuilder;
import testBuilders.HColumTestBuilder;

import com.google.common.base.Function;

import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.entity.metadata.MultiKeyProperties;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.operations.EntityLoader;
import fr.doan.achilles.holder.KeyValue;
import fr.doan.achilles.serializer.SerializerUtils;

/**
 * KeyValueFactoryTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class KeyValueFactoryTest
{

	@InjectMocks
	private KeyValueFactory factory;

	@Mock
	private PropertyMeta<Integer, String> wideMapMeta;

	@Mock
	private PropertyMeta<TweetMultiKey, String> multiKeyWideMeta;

	@Mock
	private MultiKeyProperties multiKeyProperties;

	@Mock
	private PropertyMeta<Integer, UserBean> joinPropertyMeta;

	@Mock
	private EntityLoader loader;

	@Mock
	private CompositeTransformer compositeTransformer;

	@Mock
	private DynamicCompositeTransformer dynamicCompositeTransformer;

	@Captor
	private ArgumentCaptor<List<Long>> joinIdsCaptor;

	@Before
	public void setUp()
	{
		ReflectionTestUtils.setField(factory, "loader", loader);
		ReflectionTestUtils.setField(factory, "compositeTransformer", compositeTransformer);
		ReflectionTestUtils.setField(factory, "dynamicCompositeTransformer",
				dynamicCompositeTransformer);
		when(multiKeyWideMeta.getMultiKeyProperties()).thenReturn(multiKeyProperties);
	}

	@Test
	public void should_create_keyvalue_from_dynamic_composite_hcolumn() throws Exception
	{
		DynamicComposite dynComp = CompositeTestBuilder.builder().buildDynamic();
		HColumn<DynamicComposite, Object> hColumn = HColumTestBuilder.dynamic(dynComp, "test");

		KeyValue<Integer, String> keyValue = new KeyValue<Integer, String>(12, "test");
		when(dynamicCompositeTransformer.buildKeyValueFromDynamicComposite(wideMapMeta, hColumn))
				.thenReturn(keyValue);
		KeyValue<Integer, String> built = factory.createKeyValueForDynamicComposite(wideMapMeta,
				hColumn);

		assertThat(built).isSameAs(keyValue);
	}

	@Test
	public void should_create_key_from_dynamic_composite_hcolumn() throws Exception
	{
		DynamicComposite dynComp = CompositeTestBuilder.builder().buildDynamic();
		HColumn<DynamicComposite, Object> hColumn = HColumTestBuilder.dynamic(dynComp, "test");

		Integer key = 123;
		when(dynamicCompositeTransformer.buildKeyFromDynamicComposite(wideMapMeta, hColumn))
				.thenReturn(key);
		Integer built = factory.createKeyForDynamicComposite(wideMapMeta, hColumn);

		assertThat(built).isSameAs(key);
	}

	@Test
	public void should_create_value_from_dynamic_composite_hcolumn() throws Exception
	{
		String value = "test";
		DynamicComposite dynComp = CompositeTestBuilder.builder().buildDynamic();
		HColumn<DynamicComposite, Object> hColumn = HColumTestBuilder.dynamic(dynComp, value);

		when(wideMapMeta.getValue(value)).thenReturn(value);
		String built = factory.createValueForDynamicComposite(wideMapMeta, hColumn);

		assertThat(built).isSameAs(value);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_create_value_list_for_dynamic_composite() throws Exception
	{
		DynamicComposite dynComp1 = CompositeTestBuilder.builder().buildDynamic();
		DynamicComposite dynComp2 = CompositeTestBuilder.builder().buildDynamic();
		HColumn<DynamicComposite, Object> hCol1 = HColumTestBuilder.dynamic(dynComp1, "test1");
		HColumn<DynamicComposite, Object> hCol2 = HColumTestBuilder.dynamic(dynComp2, "test2");

		Function<HColumn<DynamicComposite, Object>, String> function = new Function<HColumn<DynamicComposite, Object>, String>()
		{
			public String apply(HColumn<DynamicComposite, Object> hCol)
			{
				return (String) hCol.getValue();
			}
		};

		when(dynamicCompositeTransformer.buildValueTransformer(wideMapMeta)).thenReturn(function);

		List<String> builtList = factory.createValueListForDynamicComposite(wideMapMeta,
				Arrays.asList(hCol1, hCol2));

		assertThat(builtList).containsExactly("test1", "test2");
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_create_join_value_list_for_dynamic_composite() throws Exception
	{
		Long joinId1 = 11L, joinId2 = 12L;
		UserBean bean1 = new UserBean(), bean2 = new UserBean();

		EntityMeta<Long> joinMeta = new EntityMeta<Long>();
		PropertyMeta<Integer, UserBean> propertyMeta = noClass(Integer.class, UserBean.class)
				.joinMeta(joinMeta).build();

		DynamicComposite dynComp1 = CompositeTestBuilder.builder().buildDynamic();
		DynamicComposite dynComp2 = CompositeTestBuilder.builder().buildDynamic();
		HColumn<DynamicComposite, Object> hCol1 = HColumTestBuilder.dynamic(dynComp1, joinId1);
		HColumn<DynamicComposite, Object> hCol2 = HColumTestBuilder.dynamic(dynComp2, joinId2);

		Function<HColumn<DynamicComposite, Object>, Object> rawValueFn = new Function<HColumn<DynamicComposite, Object>, Object>()
		{
			public Object apply(HColumn<DynamicComposite, Object> hCol)
			{
				return hCol.getValue();
			}
		};

		when(dynamicCompositeTransformer.buildRawValueTransformer()).thenReturn(rawValueFn);
		Map<Long, UserBean> map = new HashMap<Long, UserBean>();
		map.put(joinId1, bean1);
		map.put(joinId2, bean2);

		when(loader.loadJoinEntities(eq(UserBean.class), joinIdsCaptor.capture(), eq(joinMeta)))
				.thenReturn(map);
		List<UserBean> builtList = factory.createJoinValueListForDynamicComposite(propertyMeta,
				Arrays.asList(hCol1, hCol2));

		assertThat(builtList).containsExactly(bean1, bean2);
		assertThat(joinIdsCaptor.getValue()).containsExactly(joinId1, joinId2);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_create_key_list_for_dynamic_composite() throws Exception
	{
		DynamicComposite dynComp1 = CompositeTestBuilder.builder().values(0, 1, 11).buildDynamic();
		DynamicComposite dynComp2 = CompositeTestBuilder.builder().values(0, 1, 12).buildDynamic();
		HColumn<DynamicComposite, Object> hCol1 = HColumTestBuilder.dynamic(dynComp1, "test1");
		HColumn<DynamicComposite, Object> hCol2 = HColumTestBuilder.dynamic(dynComp2, "test2");

		Function<HColumn<DynamicComposite, Object>, Integer> function = new Function<HColumn<DynamicComposite, Object>, Integer>()
		{
			public Integer apply(HColumn<DynamicComposite, Object> hCol)
			{
				return (Integer) hCol.getName().getComponent(2).getValue(SerializerUtils.INT_SRZ);
			}
		};

		when(dynamicCompositeTransformer.buildKeyTransformer(wideMapMeta)).thenReturn(function);

		List<Integer> builtList = factory.createKeyListForDynamicComposite(wideMapMeta,
				Arrays.asList(hCol1, hCol2));

		assertThat(builtList).containsExactly(11, 12);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_create_keyvalue_list_for_dynamic_composite() throws Exception
	{
		DynamicComposite dynComp1 = CompositeTestBuilder.builder().values(0, 1, 11).buildDynamic();
		DynamicComposite dynComp2 = CompositeTestBuilder.builder().values(0, 1, 12).buildDynamic();
		HColumn<DynamicComposite, Object> hCol1 = HColumTestBuilder.dynamic(dynComp1, "test1", 456);
		HColumn<DynamicComposite, Object> hCol2 = HColumTestBuilder.dynamic(dynComp2, "test2", 789);

		Function<HColumn<DynamicComposite, Object>, KeyValue<Integer, String>> function = new Function<HColumn<DynamicComposite, Object>, KeyValue<Integer, String>>()
		{
			public KeyValue<Integer, String> apply(HColumn<DynamicComposite, Object> hCol)
			{
				Integer key = (Integer) hCol.getName().getComponent(2)
						.getValue(SerializerUtils.INT_SRZ);
				String value = (String) hCol.getValue();

				return new KeyValue<Integer, String>(key, value, hCol.getTtl());
			}
		};

		when(dynamicCompositeTransformer.buildKeyValueTransformer(wideMapMeta))
				.thenReturn(function);

		List<KeyValue<Integer, String>> builtList = factory.createKeyValueListForDynamicComposite(
				wideMapMeta, Arrays.asList(hCol1, hCol2));

		assertThat(builtList).hasSize(2);

		assertThat(builtList.get(0).getKey()).isEqualTo(11);
		assertThat(builtList.get(0).getValue()).isEqualTo("test1");
		assertThat(builtList.get(0).getTtl()).isEqualTo(456);

		assertThat(builtList.get(1).getKey()).isEqualTo(12);
		assertThat(builtList.get(1).getValue()).isEqualTo("test2");
		assertThat(builtList.get(1).getTtl()).isEqualTo(789);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_create_join_keyvalue_list_for_dynamic_composite() throws Exception
	{
		Integer key1 = 11, key2 = 12, ttl1 = 456, ttl2 = 789;
		Long joinId1 = 11L, joinId2 = 12L;
		UserBean bean1 = new UserBean(), bean2 = new UserBean();

		EntityMeta<Long> joinMeta = new EntityMeta<Long>();
		PropertyMeta<Integer, UserBean> propertyMeta = noClass(Integer.class, UserBean.class)
				.joinMeta(joinMeta).build();

		DynamicComposite dynComp1 = CompositeTestBuilder.builder().values(0, 1, key1)
				.buildDynamic();
		DynamicComposite dynComp2 = CompositeTestBuilder.builder().values(0, 1, key2)
				.buildDynamic();
		HColumn<DynamicComposite, Object> hCol1 = HColumTestBuilder
				.dynamic(dynComp1, joinId1, ttl1);
		HColumn<DynamicComposite, Object> hCol2 = HColumTestBuilder
				.dynamic(dynComp2, joinId2, ttl2);

		Function<HColumn<DynamicComposite, Object>, Integer> keyFunction = new Function<HColumn<DynamicComposite, Object>, Integer>()
		{
			public Integer apply(HColumn<DynamicComposite, Object> hCol)
			{
				return (Integer) hCol.getName().getComponent(2).getValue(INT_SRZ);
			}
		};

		Function<HColumn<DynamicComposite, Object>, Object> rawValueFn = new Function<HColumn<DynamicComposite, Object>, Object>()
		{
			public Object apply(HColumn<DynamicComposite, Object> hCol)
			{
				return hCol.getValue();
			}
		};
		Function<HColumn<DynamicComposite, Object>, Integer> ttlFn = new Function<HColumn<DynamicComposite, Object>, Integer>()
		{
			public Integer apply(HColumn<DynamicComposite, Object> hCol)
			{
				return hCol.getTtl();
			}
		};

		when(dynamicCompositeTransformer.buildKeyTransformer(propertyMeta)).thenReturn(keyFunction);
		when(dynamicCompositeTransformer.buildRawValueTransformer()).thenReturn(rawValueFn);
		when(dynamicCompositeTransformer.buildTtlTransformer()).thenReturn(ttlFn);

		Map<Long, UserBean> map = new HashMap<Long, UserBean>();
		map.put(joinId1, bean1);
		map.put(joinId2, bean2);

		when(loader.loadJoinEntities(eq(UserBean.class), joinIdsCaptor.capture(), eq(joinMeta)))
				.thenReturn(map);
		List<KeyValue<Integer, UserBean>> builtList = factory
				.createJoinKeyValueListForDynamicComposite(propertyMeta,
						Arrays.asList(hCol1, hCol2));

		assertThat(joinIdsCaptor.getValue()).containsExactly(joinId1, joinId2);

		assertThat(builtList).hasSize(2);

		assertThat(builtList.get(0).getKey()).isEqualTo(key1);
		assertThat(builtList.get(0).getValue()).isEqualTo(bean1);
		assertThat(builtList.get(0).getTtl()).isEqualTo(ttl1);

		assertThat(builtList.get(1).getKey()).isEqualTo(key2);
		assertThat(builtList.get(1).getValue()).isEqualTo(bean2);
		assertThat(builtList.get(1).getTtl()).isEqualTo(ttl2);
	}

	// Composite
	@Test
	public void should_create_keyvalue_from_composite_hcolumn() throws Exception
	{
		Composite comp = CompositeTestBuilder.builder().buildSimple();
		HColumn<Composite, String> hColumn = HColumTestBuilder.simple(comp, "test");

		KeyValue<Integer, String> keyValue = new KeyValue<Integer, String>(12, "test");
		when(compositeTransformer.buildKeyValueFromComposite(wideMapMeta, hColumn)).thenReturn(
				keyValue);
		KeyValue<Integer, String> built = factory.createKeyValueForComposite(wideMapMeta, hColumn);

		assertThat(built).isSameAs(keyValue);
	}

	@Test
	public void should_create_key_from_composite_hcolumn() throws Exception
	{
		Composite comp = CompositeTestBuilder.builder().buildSimple();
		HColumn<Composite, String> hColumn = HColumTestBuilder.simple(comp, "test");

		Integer key = 123;
		when(compositeTransformer.buildKeyFromComposite(wideMapMeta, hColumn)).thenReturn(key);
		Integer built = factory.createKeyForComposite(wideMapMeta, hColumn);

		assertThat(built).isSameAs(key);
	}

	@Test
	public void should_create_value_from_composite_hcolumn() throws Exception
	{
		String value = "test";
		Composite comp = CompositeTestBuilder.builder().buildSimple();
		HColumn<Composite, String> hColumn = HColumTestBuilder.simple(comp, value);

		when(wideMapMeta.getValue(value)).thenReturn(value);
		String built = factory.createValueForComposite(wideMapMeta, hColumn);

		assertThat(built).isSameAs(value);
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Test
	public void should_create_value_list_for_composite() throws Exception
	{
		Composite comp1 = CompositeTestBuilder.builder().buildSimple();
		Composite comp2 = CompositeTestBuilder.builder().buildSimple();
		HColumn<Composite, String> hCol1 = HColumTestBuilder.simple(comp1, "test1");
		HColumn<Composite, String> hCol2 = HColumTestBuilder.simple(comp2, "test2");

		Function<HColumn<Composite, String>, String> function = new Function<HColumn<Composite, String>, String>()
		{
			public String apply(HColumn<Composite, String> hCol)
			{
				return (String) hCol.getValue();
			}
		};

		when(compositeTransformer.buildValueTransformer(wideMapMeta)).thenReturn(
				(Function) function);

		List<String> builtList = factory.createValueListForComposite(wideMapMeta,
				Arrays.asList((HColumn<Composite, ?>) hCol1, hCol2));

		assertThat(builtList).containsExactly("test1", "test2");
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Test
	public void should_create_join_value_list_for_composite() throws Exception
	{
		Long joinId1 = 11L, joinId2 = 12L;
		UserBean bean1 = new UserBean(), bean2 = new UserBean();

		EntityMeta<Long> joinMeta = new EntityMeta<Long>();
		PropertyMeta<Integer, UserBean> propertyMeta = noClass(Integer.class, UserBean.class)
				.joinMeta(joinMeta).build();

		Composite comp1 = CompositeTestBuilder.builder().buildSimple();
		Composite comp2 = CompositeTestBuilder.builder().buildSimple();
		HColumn<Composite, Long> hCol1 = HColumTestBuilder.simple(comp1, joinId1);
		HColumn<Composite, Long> hCol2 = HColumTestBuilder.simple(comp2, joinId2);

		Function<HColumn<Composite, Long>, Long> rawValueFn = new Function<HColumn<Composite, Long>, Long>()
		{
			public Long apply(HColumn<Composite, Long> hCol)
			{
				return hCol.getValue();
			}
		};

		when(compositeTransformer.buildRawValueTransformer()).thenReturn((Function) rawValueFn);
		Map<Long, UserBean> map = new HashMap<Long, UserBean>();
		map.put(joinId1, bean1);
		map.put(joinId2, bean2);

		when(loader.loadJoinEntities(eq(UserBean.class), joinIdsCaptor.capture(), eq(joinMeta)))
				.thenReturn(map);
		List<UserBean> builtList = factory.createJoinValueListForComposite(propertyMeta,
				Arrays.asList((HColumn<Composite, ?>) hCol1, hCol2));

		assertThat(builtList).containsExactly(bean1, bean2);
		assertThat(joinIdsCaptor.getValue()).containsExactly(joinId1, joinId2);
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Test
	public void should_create_key_list_for_composite() throws Exception
	{
		Composite comp1 = CompositeTestBuilder.builder().values(11).buildSimple();
		Composite comp2 = CompositeTestBuilder.builder().values(12).buildSimple();
		HColumn<Composite, String> hCol1 = HColumTestBuilder.simple(comp1, "test1");
		HColumn<Composite, String> hCol2 = HColumTestBuilder.simple(comp2, "test2");

		Function<HColumn<Composite, Integer>, Integer> function = new Function<HColumn<Composite, Integer>, Integer>()
		{
			public Integer apply(HColumn<Composite, Integer> hCol)
			{
				return (Integer) hCol.getName().getComponent(0).getValue(SerializerUtils.INT_SRZ);
			}
		};

		when(compositeTransformer.buildKeyTransformer(wideMapMeta)).thenReturn((Function) function);

		List<Integer> builtList = factory.createKeyListForComposite(wideMapMeta,
				Arrays.asList((HColumn<Composite, ?>) hCol1, hCol2));

		assertThat(builtList).containsExactly(11, 12);
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Test
	public void should_create_keyvalue_list_for_composite() throws Exception
	{
		Composite comp1 = CompositeTestBuilder.builder().values(11).buildSimple();
		Composite comp2 = CompositeTestBuilder.builder().values(12).buildSimple();
		HColumn<Composite, String> hCol1 = HColumTestBuilder.simple(comp1, "test1", 456);
		HColumn<Composite, String> hCol2 = HColumTestBuilder.simple(comp2, "test2", 789);

		Function<HColumn<Composite, String>, KeyValue<Integer, String>> function = new Function<HColumn<Composite, String>, KeyValue<Integer, String>>()
		{
			public KeyValue<Integer, String> apply(HColumn<Composite, String> hCol)
			{
				Integer key = (Integer) hCol.getName().getComponent(0)
						.getValue(SerializerUtils.INT_SRZ);
				String value = (String) hCol.getValue();

				return new KeyValue<Integer, String>(key, value, hCol.getTtl());
			}
		};

		when(compositeTransformer.buildKeyValueTransformer(wideMapMeta)).thenReturn(
				(Function) function);

		List<KeyValue<Integer, String>> builtList = factory.createKeyValueListForComposite(
				wideMapMeta, Arrays.asList((HColumn<Composite, ?>) hCol1, hCol2));

		assertThat(builtList).hasSize(2);

		assertThat(builtList.get(0).getKey()).isEqualTo(11);
		assertThat(builtList.get(0).getValue()).isEqualTo("test1");
		assertThat(builtList.get(0).getTtl()).isEqualTo(456);

		assertThat(builtList.get(1).getKey()).isEqualTo(12);
		assertThat(builtList.get(1).getValue()).isEqualTo("test2");
		assertThat(builtList.get(1).getTtl()).isEqualTo(789);
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Test
	public void should_create_join_keyvalue_list_for_composite() throws Exception
	{
		Integer key1 = 11, key2 = 12, ttl1 = 456, ttl2 = 789;
		Long joinId1 = 11L, joinId2 = 12L;
		UserBean bean1 = new UserBean(), bean2 = new UserBean();

		EntityMeta<Long> joinMeta = new EntityMeta<Long>();
		PropertyMeta<Integer, UserBean> propertyMeta = noClass(Integer.class, UserBean.class)
				.joinMeta(joinMeta).build();

		Composite comp1 = CompositeTestBuilder.builder().values(key1).buildSimple();
		Composite comp2 = CompositeTestBuilder.builder().values(key2).buildSimple();
		HColumn<Composite, Long> hCol1 = HColumTestBuilder.simple(comp1, joinId1, ttl1);
		HColumn<Composite, Long> hCol2 = HColumTestBuilder.simple(comp2, joinId2, ttl2);

		Function<HColumn<Composite, Integer>, Integer> keyFunction = new Function<HColumn<Composite, Integer>, Integer>()
		{
			public Integer apply(HColumn<Composite, Integer> hCol)
			{
				return (Integer) hCol.getName().getComponent(0).getValue(INT_SRZ);
			}
		};

		Function<HColumn<Composite, Long>, Long> rawValueFn = new Function<HColumn<Composite, Long>, Long>()
		{
			public Long apply(HColumn<Composite, Long> hCol)
			{
				return hCol.getValue();
			}
		};

		Function<HColumn<Composite, Long>, Integer> ttlFn = new Function<HColumn<Composite, Long>, Integer>()
		{
			public Integer apply(HColumn<Composite, Long> hCol)
			{
				return hCol.getTtl();
			}
		};

		when(compositeTransformer.buildKeyTransformer(propertyMeta)).thenReturn(
				(Function) keyFunction);
		when(compositeTransformer.buildRawValueTransformer()).thenReturn((Function) rawValueFn);
		when(compositeTransformer.buildTtlTransformer()).thenReturn((Function) ttlFn);

		Map<Long, UserBean> map = new HashMap<Long, UserBean>();
		map.put(joinId1, bean1);
		map.put(joinId2, bean2);

		when(loader.loadJoinEntities(eq(UserBean.class), joinIdsCaptor.capture(), eq(joinMeta)))
				.thenReturn(map);
		List<KeyValue<Integer, UserBean>> builtList = factory.createJoinKeyValueListForComposite(
				propertyMeta, Arrays.asList((HColumn<Composite, ?>) hCol1, hCol2));

		assertThat(joinIdsCaptor.getValue()).containsExactly(joinId1, joinId2);

		assertThat(builtList).hasSize(2);

		assertThat(builtList.get(0).getKey()).isEqualTo(key1);
		assertThat(builtList.get(0).getValue()).isEqualTo(bean1);
		assertThat(builtList.get(0).getTtl()).isEqualTo(ttl1);

		assertThat(builtList.get(1).getKey()).isEqualTo(key2);
		assertThat(builtList.get(1).getValue()).isEqualTo(bean2);
		assertThat(builtList.get(1).getTtl()).isEqualTo(ttl2);
	}
}
