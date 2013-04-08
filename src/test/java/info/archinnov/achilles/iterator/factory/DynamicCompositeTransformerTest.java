package info.archinnov.achilles.iterator.factory;

import static info.archinnov.achilles.entity.metadata.PropertyType.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.entity.PropertyHelper;
import info.archinnov.achilles.entity.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.JoinProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.EntityProxifier;
import info.archinnov.achilles.entity.type.KeyValue;

import java.util.Arrays;
import java.util.List;

import mapping.entity.TweetMultiKey;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HCounterColumn;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import testBuilders.CompositeTestBuilder;
import testBuilders.HColumnTestBuilder;
import testBuilders.PropertyMetaTestBuilder;

import com.google.common.collect.Lists;

/**
 * DynamicCompositeTransformerTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class DynamicCompositeTransformerTest
{

	@InjectMocks
	private DynamicCompositeTransformer transformer;

	@Mock
	private PropertyHelper helper;

	@Mock
	private EntityProxifier proxifier;

	@Mock
	private PersistenceContext<Long> context;

	@Mock
	private PersistenceContext<Long> joinContext;

	@SuppressWarnings("unchecked")
	@Test
	public void should_build_single_key_transformer() throws Exception
	{
		DynamicComposite comp1 = CompositeTestBuilder.builder().values(1, 2, 45).buildDynamic();
		DynamicComposite comp2 = CompositeTestBuilder.builder().values(1, 2, 51).buildDynamic();
		HColumn<DynamicComposite, String> hCol1 = HColumnTestBuilder.dynamic(comp1, "test1");
		HColumn<DynamicComposite, String> hCol2 = HColumnTestBuilder.dynamic(comp2, "test2");

		PropertyMeta<Integer, String> propertyMeta = PropertyMetaTestBuilder.noClass(Integer.class,
				String.class).build();

		List<Integer> keys = Lists.transform(Arrays.asList(hCol1, hCol2),
				transformer.buildKeyTransformer(propertyMeta));

		assertThat(keys).containsExactly(45, 51);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_build_multi_key_transformer() throws Exception
	{
		DynamicComposite comp1 = CompositeTestBuilder.builder().values("a", "b").buildDynamic();
		DynamicComposite comp2 = CompositeTestBuilder.builder().values("c", "d").buildDynamic();
		HColumn<DynamicComposite, String> hCol1 = HColumnTestBuilder.dynamic(comp1, "test1");
		HColumn<DynamicComposite, String> hCol2 = HColumnTestBuilder.dynamic(comp2, "test2");

		PropertyMeta<TweetMultiKey, String> propertyMeta = PropertyMetaTestBuilder.noClass(
				TweetMultiKey.class, String.class).build();

		TweetMultiKey multiKey1 = new TweetMultiKey();
		TweetMultiKey multiKey2 = new TweetMultiKey();

		when(helper.buildMultiKeyForDynamicComposite(propertyMeta, hCol1.getName().getComponents()))
				.thenReturn(multiKey1);
		when(helper.buildMultiKeyForDynamicComposite(propertyMeta, hCol2.getName().getComponents()))
				.thenReturn(multiKey2);
		List<TweetMultiKey> keys = Lists.transform(Arrays.asList(hCol1, hCol2),
				transformer.buildKeyTransformer(propertyMeta));

		assertThat(keys).containsExactly(multiKey1, multiKey2);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_build_value_transformer() throws Exception
	{
		DynamicComposite comp1 = CompositeTestBuilder.builder().buildDynamic();
		DynamicComposite comp2 = CompositeTestBuilder.builder().buildDynamic();
		HColumn<DynamicComposite, String> hCol1 = HColumnTestBuilder.dynamic(comp1, "test1");
		HColumn<DynamicComposite, String> hCol2 = HColumnTestBuilder.dynamic(comp2, "test2");

		PropertyMeta<Integer, String> propertyMeta = PropertyMetaTestBuilder //
				.noClass(Integer.class, String.class) //
				.build();

		List<String> values = Lists.transform(Arrays.asList(hCol1, hCol2),
				transformer.buildValueTransformer(propertyMeta));

		assertThat(values).containsExactly("test1", "test2");
	}

	@Test
	public void should_build_join_value_transformer() throws Exception
	{
		DynamicComposite comp1 = CompositeTestBuilder.builder().buildDynamic();
		HColumn<DynamicComposite, String> hCol1 = HColumnTestBuilder.dynamic(comp1, "user");

		PropertyMeta<Void, Long> joinIdMeta = PropertyMetaTestBuilder//
				.valueClass(Long.class) //
				.type(SIMPLE)//
				.build();

		EntityMeta<Long> joinMeta = new EntityMeta<Long>();
		joinMeta.setIdMeta(joinIdMeta);

		PropertyMeta<Void, String> propertyMeta = PropertyMetaTestBuilder //
				.valueClass(String.class)//
				.type(JOIN_SIMPLE)//
				.joinMeta(joinMeta) //
				.build();
		when(context.newPersistenceContext(joinMeta, hCol1.getValue())).thenReturn(joinContext);
		when(proxifier.buildProxy("user", joinContext)).thenReturn("user");
		assertThat(transformer.buildValueFromDynamicComposite(context, propertyMeta, hCol1))
				.isEqualTo("user");
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_build_raw_value_transformer() throws Exception
	{
		DynamicComposite comp1 = CompositeTestBuilder.builder().buildDynamic();
		DynamicComposite comp2 = CompositeTestBuilder.builder().buildDynamic();
		HColumn<DynamicComposite, String> hCol1 = HColumnTestBuilder.dynamic(comp1, "test1");
		HColumn<DynamicComposite, String> hCol2 = HColumnTestBuilder.dynamic(comp2, "test2");

		PropertyMeta<Void, String> propertyMeta = PropertyMetaTestBuilder
				.noClass(Void.class, String.class).type(SIMPLE).build();

		List<Object> rawValues = Lists.transform(Arrays.asList(hCol1, hCol2),
				transformer.buildRawValueTransformer(propertyMeta));

		assertThat(rawValues).containsExactly("test1", "test2");
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_build_raw_join_value_transformer() throws Exception
	{
		DynamicComposite comp1 = CompositeTestBuilder.builder().buildDynamic();
		DynamicComposite comp2 = CompositeTestBuilder.builder().buildDynamic();
		HColumn<DynamicComposite, String> hCol1 = HColumnTestBuilder.dynamic(comp1, "test1");
		HColumn<DynamicComposite, String> hCol2 = HColumnTestBuilder.dynamic(comp2, "test2");

		PropertyMeta<Void, String> propertyMeta = PropertyMetaTestBuilder.valueClass(String.class)
				.type(JOIN_SIMPLE).build();

		PropertyMeta<Void, String> joinIdMeta = PropertyMetaTestBuilder.valueClass(String.class)
				.type(SIMPLE).build();

		EntityMeta<String> entityMeta = new EntityMeta<String>();
		entityMeta.setIdMeta(joinIdMeta);
		JoinProperties joinProperties = new JoinProperties();
		joinProperties.setEntityMeta(entityMeta);

		propertyMeta.setJoinProperties(joinProperties);

		List<Object> rawValues = Lists.transform(Arrays.asList(hCol1, hCol2),
				transformer.buildRawValueTransformer(propertyMeta));

		assertThat(rawValues).containsExactly("test1", "test2");
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_build_ttl_transformer() throws Exception
	{
		DynamicComposite comp1 = CompositeTestBuilder.builder().buildDynamic();
		DynamicComposite comp2 = CompositeTestBuilder.builder().buildDynamic();
		HColumn<DynamicComposite, String> hCol1 = HColumnTestBuilder.dynamic(comp1, "test1", 12);
		HColumn<DynamicComposite, String> hCol2 = HColumnTestBuilder.dynamic(comp2, "test2", 13);

		List<Integer> rawValues = Lists.transform(Arrays.asList(hCol1, hCol2),
				transformer.buildTtlTransformer());

		assertThat(rawValues).containsExactly(12, 13);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_build_keyvalue_transformer() throws Exception
	{
		DynamicComposite comp1 = CompositeTestBuilder.builder().values(1, 2, 11).buildDynamic();
		DynamicComposite comp2 = CompositeTestBuilder.builder().values(1, 3, 12).buildDynamic();
		HColumn<DynamicComposite, String> hCol1 = HColumnTestBuilder.dynamic(comp1, "test1", 456);
		HColumn<DynamicComposite, String> hCol2 = HColumnTestBuilder.dynamic(comp2, "test2", 789);

		PropertyMeta<Integer, String> propertyMeta = PropertyMetaTestBuilder
				.noClass(Integer.class, String.class).type(WIDE_MAP).build();

		List<KeyValue<Integer, String>> keyValues = Lists.transform(Arrays.asList(hCol1, hCol2),
				transformer.buildKeyValueTransformer(context, propertyMeta));

		assertThat(keyValues).hasSize(2);

		assertThat(keyValues.get(0).getKey()).isEqualTo(11);
		assertThat(keyValues.get(0).getValue()).isEqualTo("test1");
		assertThat(keyValues.get(0).getTtl()).isEqualTo(456);

		assertThat(keyValues.get(1).getKey()).isEqualTo(12);
		assertThat(keyValues.get(1).getValue()).isEqualTo("test2");
		assertThat(keyValues.get(1).getTtl()).isEqualTo(789);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_build_counter_keyvalue_transformer() throws Exception
	{
		DynamicComposite comp1 = CompositeTestBuilder.builder().values(1, 2, 1).buildDynamic();
		DynamicComposite comp2 = CompositeTestBuilder.builder().values(1, 3, 2).buildDynamic();
		HCounterColumn<DynamicComposite> hCol1 = HColumnTestBuilder.counter(comp1, 11L);
		HCounterColumn<DynamicComposite> hCol2 = HColumnTestBuilder.counter(comp2, 12L);

		PropertyMeta<Integer, Long> propertyMeta = PropertyMetaTestBuilder
				.noClass(Integer.class, Long.class).type(COUNTER).build();

		List<KeyValue<Integer, Long>> keyValues = Lists.transform(Arrays.asList(hCol1, hCol2),
				transformer.buildCounterKeyValueTransformer(propertyMeta));

		assertThat(keyValues).hasSize(2);

		assertThat(keyValues.get(0).getKey()).isEqualTo(1);
		assertThat(keyValues.get(0).getValue()).isEqualTo(11L);
		assertThat(keyValues.get(0).getTtl()).isEqualTo(0);

		assertThat(keyValues.get(1).getKey()).isEqualTo(2);
		assertThat(keyValues.get(1).getValue()).isEqualTo(12L);
		assertThat(keyValues.get(1).getTtl()).isEqualTo(0);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_build_counter_key_transformer() throws Exception
	{
		DynamicComposite comp1 = CompositeTestBuilder.builder().values(1, 2, 1).buildDynamic();
		DynamicComposite comp2 = CompositeTestBuilder.builder().values(1, 3, 2).buildDynamic();
		HCounterColumn<DynamicComposite> hCol1 = HColumnTestBuilder.counter(comp1, 11L);
		HCounterColumn<DynamicComposite> hCol2 = HColumnTestBuilder.counter(comp2, 12L);

		PropertyMeta<Integer, Long> propertyMeta = PropertyMetaTestBuilder
				.noClass(Integer.class, Long.class).type(COUNTER).build();

		List<Integer> keys = Lists.transform(Arrays.asList(hCol1, hCol2),
				transformer.buildCounterKeyTransformer(propertyMeta));

		assertThat(keys).hasSize(2);
		assertThat(keys.get(0)).isEqualTo(1);
		assertThat(keys.get(1)).isEqualTo(2);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_build_counter_multi_key_transformer() throws Exception
	{
		DynamicComposite comp1 = CompositeTestBuilder.builder().values(1, 2, 1).buildDynamic();
		DynamicComposite comp2 = CompositeTestBuilder.builder().values(1, 3, 2).buildDynamic();
		HCounterColumn<DynamicComposite> hCol1 = HColumnTestBuilder.counter(comp1, 11L);
		HCounterColumn<DynamicComposite> hCol2 = HColumnTestBuilder.counter(comp2, 12L);

		PropertyMeta<TweetMultiKey, Long> propertyMeta = PropertyMetaTestBuilder
				.noClass(TweetMultiKey.class, Long.class).type(COUNTER).build();

		TweetMultiKey multiKey1 = new TweetMultiKey();
		TweetMultiKey multiKey2 = new TweetMultiKey();

		when(helper.buildMultiKeyForDynamicComposite(eq(propertyMeta), any(List.class)))
				.thenReturn(multiKey1, multiKey2);

		List<TweetMultiKey> keys = Lists.transform(Arrays.asList(hCol1, hCol2),
				transformer.buildCounterKeyTransformer(propertyMeta));

		assertThat(keys).hasSize(2);
		assertThat(keys).contains(multiKey1, multiKey2);

	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_build_counter_value_transformer() throws Exception
	{
		DynamicComposite comp1 = CompositeTestBuilder.builder().values(1, 2, 1).buildDynamic();
		DynamicComposite comp2 = CompositeTestBuilder.builder().values(1, 3, 2).buildDynamic();
		HCounterColumn<DynamicComposite> hCol1 = HColumnTestBuilder.counter(comp1, 11L);
		HCounterColumn<DynamicComposite> hCol2 = HColumnTestBuilder.counter(comp2, 12L);

		PropertyMeta<Integer, Long> propertyMeta = PropertyMetaTestBuilder
				.noClass(Integer.class, Long.class).type(COUNTER).build();

		List<Long> values = Lists.transform(Arrays.asList(hCol1, hCol2),
				transformer.buildCounterValueTransformer(propertyMeta));

		assertThat(values).hasSize(2);
		assertThat(values.get(0)).isEqualTo(11L);
		assertThat(values.get(1)).isEqualTo(12L);
	}
}
