package info.archinnov.achilles.iterator.factory;

import static info.archinnov.achilles.serializer.ThriftSerializerUtils.INT_SRZ;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import static testBuilders.PropertyMetaTestBuilder.noClass;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.MultiKeyProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.ThriftEntityProxifier;
import info.archinnov.achilles.helper.ThriftJoinEntityHelper;
import info.archinnov.achilles.proxy.wrapper.CounterBuilder;
import info.archinnov.achilles.serializer.ThriftSerializerUtils;
import info.archinnov.achilles.type.Counter;
import info.archinnov.achilles.type.KeyValue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mapping.entity.TweetMultiKey;
import mapping.entity.UserBean;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HCounterColumn;

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
 * ThriftKeyValueFactoryTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class ThriftKeyValueFactoryTest
{

	@InjectMocks
	private ThriftKeyValueFactory factory;

	@Mock
	private PropertyMeta<Integer, String> wideMapMeta;

	@Mock
	private PropertyMeta<TweetMultiKey, String> multiKeyWideMeta;

	@Mock
	private PropertyMeta<Integer, Counter> counterMeta;

	@Mock
	private MultiKeyProperties multiKeyProperties;

	@Mock
	private PropertyMeta<Integer, UserBean> joinPropertyMeta;

	@Mock
	private ThriftJoinEntityHelper joinHelper;

	@Mock
	private ThriftEntityProxifier proxifier;

	@Mock
	private ThriftCompositeTransformer thriftCompositeTransformer;

	@Mock
	private ThriftPersistenceContext context, joinContext1, joinContext2;

	@Mock
	private ThriftGenericEntityDao joinEntityDao;

	@Captor
	private ArgumentCaptor<List<Long>> joinIdsCaptor;

	private Long joinId1 = 11L, joinId2 = 12L;
	private Integer key1 = 11, key2 = 12, ttl1 = 456, ttl2 = 789;
	private UserBean bean1 = new UserBean(), bean2 = new UserBean();
	private EntityMeta joinMeta = new EntityMeta();
	private PropertyMeta<Integer, UserBean> propertyMeta;
	private Map<Long, UserBean> map = new HashMap<Long, UserBean>();

	@Before
	public void setUp() throws Exception
	{
		Whitebox.setInternalState(factory, "proxifier", proxifier);
		Whitebox.setInternalState(factory, "joinHelper", joinHelper);
		Whitebox
				.setInternalState(factory, "thriftCompositeTransformer", thriftCompositeTransformer);

		when(multiKeyWideMeta.getMultiKeyProperties()).thenReturn(multiKeyProperties);
		when((ThriftGenericEntityDao) context.findEntityDao("join_cf")).thenReturn(joinEntityDao);

		joinMeta.setTableName("join_cf");
		propertyMeta = noClass(Integer.class, UserBean.class) //
				.joinMeta(joinMeta)
				//
				.build();

		map.clear();
		map.put(joinId1, bean1);
		map.put(joinId2, bean2);

		when(joinHelper.loadJoinEntities(eq(UserBean.class), //
				joinIdsCaptor.capture(), eq(joinMeta), eq(joinEntityDao))).thenReturn(map);
		when(context.newPersistenceContext(joinMeta, bean1)).thenReturn(joinContext1);
		when(context.newPersistenceContext(joinMeta, bean2)).thenReturn(joinContext2);
		when(proxifier.buildProxy(bean1, joinContext1)).thenReturn(bean1);
		when(proxifier.buildProxy(bean2, joinContext2)).thenReturn(bean2);
	}

	// Composite
	@Test
	public void should_create_keyvalue() throws Exception
	{
		Composite comp = CompositeTestBuilder.builder().buildSimple();
		HColumn<Composite, String> hColumn = HColumnTestBuilder.simple(comp, "test");

		KeyValue<Integer, String> keyValue = new KeyValue<Integer, String>(12, "test");
		when(thriftCompositeTransformer.buildKeyValue(context, wideMapMeta, hColumn)).thenReturn(
				keyValue);
		KeyValue<Integer, String> built = factory.createKeyValue(context, wideMapMeta, hColumn);

		assertThat(built).isSameAs(keyValue);
	}

	@Test
	public void should_create_key() throws Exception
	{
		Composite comp = CompositeTestBuilder.builder().buildSimple();
		HColumn<Composite, String> hColumn = HColumnTestBuilder.simple(comp, "test");

		Integer key = 123;
		when(thriftCompositeTransformer.buildKey(wideMapMeta, hColumn)).thenReturn(key);
		Integer built = factory.createKey(wideMapMeta, hColumn);

		assertThat(built).isSameAs(key);
	}

	@Test
	public void should_create_value() throws Exception
	{
		String value = "test";
		Composite comp = CompositeTestBuilder.builder().buildSimple();
		HColumn<Composite, String> hColumn = HColumnTestBuilder.simple(comp, value);

		when(thriftCompositeTransformer.buildValue(context, wideMapMeta, hColumn))
				.thenReturn(value);
		String built = factory.createValue(context, wideMapMeta, hColumn);

		assertThat(built).isSameAs(value);
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Test
	public void should_create_value_list() throws Exception
	{
		Composite comp1 = CompositeTestBuilder.builder().buildSimple();
		Composite comp2 = CompositeTestBuilder.builder().buildSimple();
		HColumn<Composite, String> hCol1 = HColumnTestBuilder.simple(comp1, "test1");
		HColumn<Composite, String> hCol2 = HColumnTestBuilder.simple(comp2, "test2");

		Function<HColumn<Composite, String>, String> function = new Function<HColumn<Composite, String>, String>()
		{
			@Override
			public String apply(HColumn<Composite, String> hCol)
			{
				return hCol.getValue();
			}
		};

		when(thriftCompositeTransformer.buildValueTransformer(wideMapMeta)).thenReturn(
				(Function) function);

		List<String> builtList = factory.createValueList(wideMapMeta, Arrays.asList(hCol1, hCol2));

		assertThat(builtList).containsExactly("test1", "test2");
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Test
	public void should_create_join_value_list() throws Exception
	{
		Composite comp1 = CompositeTestBuilder.builder().buildSimple();
		Composite comp2 = CompositeTestBuilder.builder().buildSimple();
		HColumn<Composite, Long> hCol1 = HColumnTestBuilder.simple(comp1, joinId1);
		HColumn<Composite, Long> hCol2 = HColumnTestBuilder.simple(comp2, joinId2);

		Function<HColumn<Composite, Long>, Long> rawValueFn = new Function<HColumn<Composite, Long>, Long>()
		{
			@Override
			public Long apply(HColumn<Composite, Long> hCol)
			{
				return hCol.getValue();
			}
		};

		when(thriftCompositeTransformer.buildRawValueTransformer()).thenReturn(
				(Function) rawValueFn);
		List<UserBean> builtList = factory.createJoinValueList(context, propertyMeta,
				Arrays.asList(hCol1, hCol2));

		assertThat(builtList).containsExactly(bean1, bean2);
		assertThat(joinIdsCaptor.getValue()).containsExactly(joinId1, joinId2);
	}

	@Test
	public void should_create_empty_join_value_list() throws Exception
	{
		List<HColumn<Composite, Long>> hCols = new ArrayList<HColumn<Composite, Long>>();

		List<UserBean> builtList = factory.createJoinValueList(context, propertyMeta, hCols);

		assertThat(builtList).isEmpty();
		verifyZeroInteractions(thriftCompositeTransformer, joinHelper);
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Test
	public void should_create_key_list() throws Exception
	{
		Composite comp1 = CompositeTestBuilder.builder().values(11).buildSimple();
		Composite comp2 = CompositeTestBuilder.builder().values(12).buildSimple();
		HColumn<Composite, String> hCol1 = HColumnTestBuilder.simple(comp1, "test1");
		HColumn<Composite, String> hCol2 = HColumnTestBuilder.simple(comp2, "test2");

		Function<HColumn<Composite, Integer>, Integer> function = new Function<HColumn<Composite, Integer>, Integer>()
		{
			@Override
			public Integer apply(HColumn<Composite, Integer> hCol)
			{
				return (Integer) hCol
						.getName()
						.getComponent(0)
						.getValue(ThriftSerializerUtils.INT_SRZ);
			}
		};

		when(thriftCompositeTransformer.buildKeyTransformer(wideMapMeta)).thenReturn(
				(Function) function);

		List<Integer> builtList = factory.createKeyList(wideMapMeta, Arrays.asList(hCol1, hCol2));

		assertThat(builtList).containsExactly(11, 12);
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Test
	public void should_create_keyvalue_list() throws Exception
	{
		Composite comp1 = CompositeTestBuilder.builder().values(11).buildSimple();
		Composite comp2 = CompositeTestBuilder.builder().values(12).buildSimple();
		HColumn<Composite, String> hCol1 = HColumnTestBuilder.simple(comp1, "test1", 456);
		HColumn<Composite, String> hCol2 = HColumnTestBuilder.simple(comp2, "test2", 789);

		Function<HColumn<Composite, String>, KeyValue<Integer, String>> function = new Function<HColumn<Composite, String>, KeyValue<Integer, String>>()
		{
			@Override
			public KeyValue<Integer, String> apply(HColumn<Composite, String> hCol)
			{
				Integer key = (Integer) hCol
						.getName()
						.getComponent(0)
						.getValue(ThriftSerializerUtils.INT_SRZ);
				String value = hCol.getValue();

				return new KeyValue<Integer, String>(key, value, hCol.getTtl(), hCol.getClock());
			}
		};

		when(thriftCompositeTransformer.buildKeyValueTransformer(context, wideMapMeta)).thenReturn(
				(Function) function);

		List<KeyValue<Integer, String>> builtList = factory.createKeyValueList(context,
				wideMapMeta, Arrays.asList(hCol1, hCol2));

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
	public void should_create_join_keyvalue_list() throws Exception
	{
		long timestamp1 = 11L;
		long timestamp2 = 12L;

		Composite comp1 = CompositeTestBuilder.builder().values(key1).buildSimple();
		Composite comp2 = CompositeTestBuilder.builder().values(key2).buildSimple();
		HColumn<Composite, Long> hCol1 = HColumnTestBuilder.simple(comp1, joinId1, ttl1);
		HColumn<Composite, Long> hCol2 = HColumnTestBuilder.simple(comp2, joinId2, ttl2);

		hCol1.setClock(timestamp1);
		hCol2.setClock(timestamp2);

		Function<HColumn<Composite, Integer>, Integer> keyFunction = new Function<HColumn<Composite, Integer>, Integer>()
		{
			@Override
			public Integer apply(HColumn<Composite, Integer> hCol)
			{
				return (Integer) hCol.getName().getComponent(0).getValue(INT_SRZ);
			}
		};

		Function<HColumn<Composite, Long>, Long> rawValueFn = new Function<HColumn<Composite, Long>, Long>()
		{
			@Override
			public Long apply(HColumn<Composite, Long> hCol)
			{
				return hCol.getValue();
			}
		};

		Function<HColumn<Composite, Long>, Integer> ttlFn = new Function<HColumn<Composite, Long>, Integer>()
		{
			@Override
			public Integer apply(HColumn<Composite, Long> hCol)
			{
				return hCol.getTtl();
			}
		};

		Function<HColumn<Composite, Long>, Long> timestampFn = new Function<HColumn<Composite, Long>, Long>()
		{
			@Override
			public Long apply(HColumn<Composite, Long> hCol)
			{
				return hCol.getClock();
			}
		};

		when(thriftCompositeTransformer.buildKeyTransformer(propertyMeta)).thenReturn(
				(Function) keyFunction);
		when(thriftCompositeTransformer.buildRawValueTransformer()).thenReturn(
				(Function) rawValueFn);
		when(thriftCompositeTransformer.buildTtlTransformer()).thenReturn((Function) ttlFn);
		when(thriftCompositeTransformer.buildTimestampTransformer()).thenReturn(
				(Function) timestampFn);

		List<KeyValue<Integer, UserBean>> builtList = factory.createJoinKeyValueList(context,
				propertyMeta, Arrays.asList(hCol1, hCol2));

		assertThat(joinIdsCaptor.getValue()).containsExactly(joinId1, joinId2);

		assertThat(builtList).hasSize(2);

		assertThat(builtList.get(0).getKey()).isEqualTo(key1);
		assertThat(builtList.get(0).getValue()).isEqualTo(bean1);
		assertThat(builtList.get(0).getTtl()).isEqualTo(ttl1);
		assertThat(builtList.get(0).getTimestamp()).isEqualTo(timestamp1);

		assertThat(builtList.get(1).getKey()).isEqualTo(key2);
		assertThat(builtList.get(1).getValue()).isEqualTo(bean2);
		assertThat(builtList.get(1).getTtl()).isEqualTo(ttl2);
		assertThat(builtList.get(1).getTimestamp()).isEqualTo(timestamp2);
	}

	@Test
	public void should_create_empty_join_keyvalue_list() throws Exception
	{
		List<HColumn<Composite, Long>> hCols = new ArrayList<HColumn<Composite, Long>>();

		List<KeyValue<Integer, UserBean>> builtList = factory.createJoinKeyValueList(context,
				propertyMeta, hCols);

		assertThat(builtList).isEmpty();
		verifyZeroInteractions(thriftCompositeTransformer, joinHelper);
	}

	@Test
	public void should_create_counter_keyvalue() throws Exception
	{
		Composite comp = CompositeTestBuilder.builder().buildSimple();
		HCounterColumn<Composite> hColumn = HColumnTestBuilder.counter(comp, 150L);

		KeyValue<Integer, Counter> keyValue = new KeyValue<Integer, Counter>(12,
				CounterBuilder.incr(150L));
		when(thriftCompositeTransformer.buildCounterKeyValue(context, counterMeta, hColumn))
				.thenReturn(keyValue);
		KeyValue<Integer, Counter> built = factory.createCounterKeyValue(context, counterMeta,
				hColumn);

		assertThat(built).isSameAs(keyValue);
	}

	@Test
	public void should_create_counter_key() throws Exception
	{
		Composite dynComp = CompositeTestBuilder.builder().buildSimple();
		HCounterColumn<Composite> hColumn = HColumnTestBuilder.counter(dynComp, 150L);

		Integer key = 123;
		when(thriftCompositeTransformer.buildCounterKey(counterMeta, hColumn)).thenReturn(key);
		Integer built = factory.createCounterKey(counterMeta, hColumn);

		assertThat(built).isSameAs(key);
	}

	@Test
	public void should_create_counter_value() throws Exception
	{
		Long value = 150L;
		Composite dynComp = CompositeTestBuilder.builder().buildSimple();
		HCounterColumn<Composite> hColumn = HColumnTestBuilder.counter(dynComp, value);

		Counter counter = CounterBuilder.incr(value);
		when(thriftCompositeTransformer.buildCounterValue(context, counterMeta, hColumn))
				.thenReturn(counter);
		Counter built = factory.createCounterValue(context, counterMeta, hColumn);

		assertThat(built).isEqualTo(counter);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_create_counter_keyvalue_list() throws Exception
	{
		Composite dynComp1 = CompositeTestBuilder.builder().values(0, 1, 1).buildSimple();
		Composite dynComp2 = CompositeTestBuilder.builder().values(0, 1, 2).buildSimple();
		HCounterColumn<Composite> hCol1 = HColumnTestBuilder.counter(dynComp1, 11L);
		HCounterColumn<Composite> hCol2 = HColumnTestBuilder.counter(dynComp2, 12L);
		final long timestamp = System.currentTimeMillis();
		Function<HCounterColumn<Composite>, KeyValue<Integer, Counter>> function = new Function<HCounterColumn<Composite>, KeyValue<Integer, Counter>>()
		{
			@Override
			public KeyValue<Integer, Counter> apply(HCounterColumn<Composite> hCol)
			{
				Integer key = (Integer) hCol
						.getName()
						.getComponent(2)
						.getValue(ThriftSerializerUtils.INT_SRZ);
				Counter value = CounterBuilder.incr(hCol.getValue());

				return new KeyValue<Integer, Counter>(key, value, 0, timestamp);
			}
		};

		when(thriftCompositeTransformer.buildCounterKeyValueTransformer(context, counterMeta))
				.thenReturn(function);

		List<KeyValue<Integer, Counter>> builtList = factory.createCounterKeyValueList(context,
				counterMeta, Arrays.asList(hCol1, hCol2));

		assertThat(builtList).hasSize(2);

		assertThat(builtList.get(0).getKey()).isEqualTo(1);
		assertThat(builtList.get(0).getValue().get()).isEqualTo(11L);
		assertThat(builtList.get(0).getTtl()).isEqualTo(0);
		assertThat(builtList.get(0).getTimestamp()).isEqualTo(timestamp);

		assertThat(builtList.get(1).getKey()).isEqualTo(2);
		assertThat(builtList.get(1).getValue().get()).isEqualTo(12L);
		assertThat(builtList.get(1).getTtl()).isEqualTo(0);
		assertThat(builtList.get(1).getTimestamp()).isEqualTo(timestamp);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_create_counter_value_list() throws Exception
	{
		Composite dynComp1 = CompositeTestBuilder.builder().buildSimple();
		Composite dynComp2 = CompositeTestBuilder.builder().buildSimple();
		HCounterColumn<Composite> hCol1 = HColumnTestBuilder.counter(dynComp1, 11L);
		HCounterColumn<Composite> hCol2 = HColumnTestBuilder.counter(dynComp2, 12L);

		Function<HCounterColumn<Composite>, Counter> function = new Function<HCounterColumn<Composite>, Counter>()
		{
			@Override
			public Counter apply(HCounterColumn<Composite> hCol)
			{
				return CounterBuilder.incr(hCol.getValue());
			}
		};

		when(thriftCompositeTransformer.buildCounterValueTransformer(context, counterMeta))
				.thenReturn(function);

		List<Counter> builtList = factory.createCounterValueList(context, counterMeta,
				Arrays.asList(hCol1, hCol2));

		assertThat(builtList).hasSize(2);
		assertThat(builtList.get(0).get()).isEqualTo(hCol1.getValue());
		assertThat(builtList.get(1).get()).isEqualTo(hCol2.getValue());

	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Test
	public void should_create_counter_key_list() throws Exception
	{
		Composite dynComp1 = CompositeTestBuilder.builder().values(0, 1, 1).buildSimple();
		Composite dynComp2 = CompositeTestBuilder.builder().values(0, 1, 2).buildSimple();
		HCounterColumn<Composite> hCol1 = HColumnTestBuilder.counter(dynComp1, 11L);
		HCounterColumn<Composite> hCol2 = HColumnTestBuilder.counter(dynComp2, 12L);

		Function<HCounterColumn<Composite>, Integer> function = new Function<HCounterColumn<Composite>, Integer>()
		{
			@Override
			public Integer apply(HCounterColumn<Composite> hCol)
			{
				return (Integer) hCol.getName().getComponent(2).getValue(INT_SRZ);
			}
		};

		when((Function) thriftCompositeTransformer.buildCounterKeyTransformer(counterMeta))
				.thenReturn(function);

		List<Integer> builtList = factory.createCounterKeyList(counterMeta,
				Arrays.asList(hCol1, hCol2));

		assertThat(builtList).containsExactly(1, 2);
	}

	@Test
	public void should_create_ttl_for_composite() throws Exception
	{
		Composite name = new Composite();
		HColumn<Composite, String> hCol = HColumnTestBuilder.simple(name, "test", 1212);

		assertThat(factory.createTtl(hCol)).isEqualTo(1212);
	}

	@Test
	public void should_create_counter_ttl() throws Exception
	{
		Composite name = new Composite();
		HColumn<Composite, String> hCol = HColumnTestBuilder.simple(name, "test", 12);

		assertThat(factory.createTtl(hCol)).isEqualTo(12);
	}

}
