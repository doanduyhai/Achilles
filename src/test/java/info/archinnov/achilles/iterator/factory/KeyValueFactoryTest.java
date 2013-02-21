package info.archinnov.achilles.iterator.factory;

import static info.archinnov.achilles.serializer.SerializerUtils.INT_SRZ;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;
import static testBuilders.PropertyMetaTestBuilder.noClass;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.MultiKeyProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.EntityLoader;
import info.archinnov.achilles.entity.type.KeyValue;
import info.archinnov.achilles.serializer.SerializerUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mapping.entity.TweetMultiKey;
import mapping.entity.UserBean;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

import testBuilders.CompositeTestBuilder;
import testBuilders.HColumnTestBuilder;

import com.google.common.base.Function;

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

	private ObjectMapper objectMapper = new ObjectMapper();

	@Before
	public void setUp()
	{
		Whitebox.setInternalState(factory, "loader", loader);
		Whitebox.setInternalState(factory, "compositeTransformer", compositeTransformer);
		Whitebox.setInternalState(factory, "dynamicCompositeTransformer",
				dynamicCompositeTransformer);

		when(multiKeyWideMeta.getMultiKeyProperties()).thenReturn(multiKeyProperties);
	}

	@Test
	public void should_create_keyvalue_from_dynamic_composite_hcolumn() throws Exception
	{
		DynamicComposite dynComp = CompositeTestBuilder.builder().buildDynamic();
		HColumn<DynamicComposite, String> hColumn = HColumnTestBuilder.dynamic(dynComp, "test");

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
		HColumn<DynamicComposite, String> hColumn = HColumnTestBuilder.dynamic(dynComp, "test");

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
		HColumn<DynamicComposite, String> hColumn = HColumnTestBuilder.dynamic(dynComp, value);

		when(dynamicCompositeTransformer.buildValueFromDynamicComposite(wideMapMeta, hColumn))
				.thenReturn(value);
		String built = factory.createValueForDynamicComposite(wideMapMeta, hColumn);

		assertThat(built).isEqualTo(value);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_create_value_list_for_dynamic_composite() throws Exception
	{
		DynamicComposite dynComp1 = CompositeTestBuilder.builder().buildDynamic();
		DynamicComposite dynComp2 = CompositeTestBuilder.builder().buildDynamic();
		HColumn<DynamicComposite, String> hCol1 = HColumnTestBuilder.dynamic(dynComp1, "test1");
		HColumn<DynamicComposite, String> hCol2 = HColumnTestBuilder.dynamic(dynComp2, "test2");

		Function<HColumn<DynamicComposite, String>, String> function = new Function<HColumn<DynamicComposite, String>, String>()
		{
			public String apply(HColumn<DynamicComposite, String> hCol)
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
		HColumn<DynamicComposite, String> hCol1 = HColumnTestBuilder.dynamic(dynComp1,
				joinId1.toString());
		HColumn<DynamicComposite, String> hCol2 = HColumnTestBuilder.dynamic(dynComp2,
				joinId2.toString());

		Function<HColumn<DynamicComposite, String>, Object> rawValueFn = new Function<HColumn<DynamicComposite, String>, Object>()
		{

			public Object apply(HColumn<DynamicComposite, String> hCol)
			{
				try
				{
					return readLong(hCol.getValue());
				}
				catch (Exception e)
				{
					e.printStackTrace();
				}
				return hCol.getValue();
			}
		};

		when(dynamicCompositeTransformer.buildRawValueTransformer(propertyMeta)).thenReturn(
				rawValueFn);
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
		HColumn<DynamicComposite, String> hCol1 = HColumnTestBuilder.dynamic(dynComp1, "test1");
		HColumn<DynamicComposite, String> hCol2 = HColumnTestBuilder.dynamic(dynComp2, "test2");

		Function<HColumn<DynamicComposite, String>, Integer> function = new Function<HColumn<DynamicComposite, String>, Integer>()
		{
			public Integer apply(HColumn<DynamicComposite, String> hCol)
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
		HColumn<DynamicComposite, String> hCol1 = HColumnTestBuilder
				.dynamic(dynComp1, "test1", 456);
		HColumn<DynamicComposite, String> hCol2 = HColumnTestBuilder
				.dynamic(dynComp2, "test2", 789);

		Function<HColumn<DynamicComposite, String>, KeyValue<Integer, String>> function = new Function<HColumn<DynamicComposite, String>, KeyValue<Integer, String>>()
		{
			public KeyValue<Integer, String> apply(HColumn<DynamicComposite, String> hCol)
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
	public void should_create_join_key_value_list_for_dynamic_composite() throws Exception
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
		HColumn<DynamicComposite, String> hCol1 = HColumnTestBuilder.dynamic(dynComp1,
				joinId1.toString(), ttl1);
		HColumn<DynamicComposite, String> hCol2 = HColumnTestBuilder.dynamic(dynComp2,
				joinId2.toString(), ttl2);

		Function<HColumn<DynamicComposite, String>, Integer> keyFunction = new Function<HColumn<DynamicComposite, String>, Integer>()
		{
			public Integer apply(HColumn<DynamicComposite, String> hCol)
			{
				return (Integer) hCol.getName().getComponent(2).getValue(INT_SRZ);
			}
		};

		Function<HColumn<DynamicComposite, String>, Object> rawValueFn = new Function<HColumn<DynamicComposite, String>, Object>()
		{
			public Object apply(HColumn<DynamicComposite, String> hCol)
			{
				try
				{
					return readLong(hCol.getValue());
				}
				catch (Exception e)
				{
					e.printStackTrace();
				}
				return hCol.getValue();
			}
		};
		Function<HColumn<DynamicComposite, String>, Integer> ttlFn = new Function<HColumn<DynamicComposite, String>, Integer>()
		{
			public Integer apply(HColumn<DynamicComposite, String> hCol)
			{
				return hCol.getTtl();
			}
		};

		when(dynamicCompositeTransformer.buildKeyTransformer(propertyMeta)).thenReturn(keyFunction);
		when(dynamicCompositeTransformer.buildRawValueTransformer(propertyMeta)).thenReturn(
				rawValueFn);
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
		HColumn<Composite, String> hColumn = HColumnTestBuilder.simple(comp, "test");

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
		HColumn<Composite, String> hColumn = HColumnTestBuilder.simple(comp, "test");

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
		HColumn<Composite, String> hColumn = HColumnTestBuilder.simple(comp, value);

		when(compositeTransformer.buildValueFromComposite(wideMapMeta, hColumn)).thenReturn(value);
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
		HColumn<Composite, String> hCol1 = HColumnTestBuilder.simple(comp1, "test1");
		HColumn<Composite, String> hCol2 = HColumnTestBuilder.simple(comp2, "test2");

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
		HColumn<Composite, Long> hCol1 = HColumnTestBuilder.simple(comp1, joinId1);
		HColumn<Composite, Long> hCol2 = HColumnTestBuilder.simple(comp2, joinId2);

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
		HColumn<Composite, String> hCol1 = HColumnTestBuilder.simple(comp1, "test1");
		HColumn<Composite, String> hCol2 = HColumnTestBuilder.simple(comp2, "test2");

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
		HColumn<Composite, String> hCol1 = HColumnTestBuilder.simple(comp1, "test1", 456);
		HColumn<Composite, String> hCol2 = HColumnTestBuilder.simple(comp2, "test2", 789);

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
		HColumn<Composite, Long> hCol1 = HColumnTestBuilder.simple(comp1, joinId1, ttl1);
		HColumn<Composite, Long> hCol2 = HColumnTestBuilder.simple(comp2, joinId2, ttl2);

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

	private Long readLong(String value) throws Exception
	{
		return objectMapper.readValue(value, Long.class);
	}
}
