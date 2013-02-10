package info.archinnov.achilles.holder.factory;

import static info.archinnov.achilles.entity.metadata.PropertyType.WIDE_MAP;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.entity.PropertyHelper;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.holder.KeyValue;

import java.util.Arrays;
import java.util.List;

import mapping.entity.TweetMultiKey;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import testBuilders.CompositeTestBuilder;
import testBuilders.HColumnTestBuilder;
import testBuilders.PropertyMetaTestBuilder;

import com.google.common.collect.Lists;

/**
 * CompositeTransformerTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class CompositeTransformerTest
{
	@InjectMocks
	private CompositeTransformer transformer;

	@Mock
	private PropertyHelper helper;

	@Before
	public void setUp()
	{
		ReflectionTestUtils.setField(transformer, "helper", helper);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_build_single_key_transformer() throws Exception
	{
		Composite comp1 = CompositeTestBuilder.builder().values(45).buildSimple();
		Composite comp2 = CompositeTestBuilder.builder().values(51).buildSimple();
		HColumn<Composite, String> hCol1 = HColumnTestBuilder.simple(comp1, "test1");
		HColumn<Composite, String> hCol2 = HColumnTestBuilder.simple(comp2, "test2");

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
		Composite comp1 = CompositeTestBuilder.builder().values("a", "b").buildSimple();
		Composite comp2 = CompositeTestBuilder.builder().values("c", "d").buildSimple();
		HColumn<Composite, String> hCol1 = HColumnTestBuilder.simple(comp1, "test1");
		HColumn<Composite, String> hCol2 = HColumnTestBuilder.simple(comp2, "test2");

		PropertyMeta<TweetMultiKey, String> propertyMeta = PropertyMetaTestBuilder.noClass(
				TweetMultiKey.class, String.class).build();

		TweetMultiKey multiKey1 = new TweetMultiKey();
		TweetMultiKey multiKey2 = new TweetMultiKey();

		when(helper.buildMultiKeyForComposite(propertyMeta, hCol1.getName().getComponents()))
				.thenReturn(multiKey1);
		when(helper.buildMultiKeyForComposite(propertyMeta, hCol2.getName().getComponents()))
				.thenReturn(multiKey2);
		List<TweetMultiKey> keys = Lists.transform(Arrays.asList(hCol1, hCol2),
				transformer.buildKeyTransformer(propertyMeta));

		assertThat(keys).containsExactly(multiKey1, multiKey2);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_build_value_transformer() throws Exception
	{
		Composite comp1 = CompositeTestBuilder.builder().buildSimple();
		Composite comp2 = CompositeTestBuilder.builder().buildSimple();
		HColumn<Composite, String> hCol1 = HColumnTestBuilder.simple(comp1, "test1");
		HColumn<Composite, String> hCol2 = HColumnTestBuilder.simple(comp2, "test2");

		PropertyMeta<Integer, String> propertyMeta = PropertyMetaTestBuilder
				.noClass(Integer.class, String.class).type(WIDE_MAP).build();

		List<String> values = Lists.transform(Arrays.asList(hCol1, hCol2),
				transformer.buildValueTransformer(propertyMeta));

		assertThat(values).containsExactly("test1", "test2");
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_build_raw_value_transformer() throws Exception
	{
		Composite comp1 = CompositeTestBuilder.builder().buildSimple();
		Composite comp2 = CompositeTestBuilder.builder().buildSimple();
		HColumn<Composite, String> hCol1 = HColumnTestBuilder.simple(comp1, "test1");
		HColumn<Composite, String> hCol2 = HColumnTestBuilder.simple(comp2, "test2");

		List<Object> rawValues = Lists.transform(Arrays.asList(hCol1, hCol2),
				transformer.buildRawValueTransformer());

		assertThat(rawValues).containsExactly("test1", "test2");
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_build_ttl_transformer() throws Exception
	{
		Composite comp1 = CompositeTestBuilder.builder().buildSimple();
		Composite comp2 = CompositeTestBuilder.builder().buildSimple();
		HColumn<Composite, String> hCol1 = HColumnTestBuilder.simple(comp1, "test1", 12);
		HColumn<Composite, String> hCol2 = HColumnTestBuilder.simple(comp2, "test2", 13);

		List<Integer> rawValues = Lists.transform(Arrays.asList(hCol1, hCol2),
				transformer.buildTtlTransformer());

		assertThat(rawValues).containsExactly(12, 13);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_build_key_value_transformer() throws Exception
	{
		Composite comp1 = CompositeTestBuilder.builder().values(11).buildSimple();
		Composite comp2 = CompositeTestBuilder.builder().values(12).buildSimple();
		HColumn<Composite, String> hCol1 = HColumnTestBuilder.simple(comp1, "test1", 456);
		HColumn<Composite, String> hCol2 = HColumnTestBuilder.simple(comp2, "test2", 789);

		PropertyMeta<Integer, String> propertyMeta = PropertyMetaTestBuilder
				.noClass(Integer.class, String.class).type(WIDE_MAP).build();

		List<KeyValue<Integer, String>> keyValues = Lists.transform(Arrays.asList(hCol1, hCol2),
				transformer.buildKeyValueTransformer(propertyMeta));

		assertThat(keyValues).hasSize(2);

		assertThat(keyValues.get(0).getKey()).isEqualTo(11);
		assertThat(keyValues.get(0).getValue()).isEqualTo("test1");
		assertThat(keyValues.get(0).getTtl()).isEqualTo(456);

		assertThat(keyValues.get(1).getKey()).isEqualTo(12);
		assertThat(keyValues.get(1).getValue()).isEqualTo("test2");
		assertThat(keyValues.get(1).getTtl()).isEqualTo(789);
	}
}
