package info.archinnov.achilles.iterator.factory;

import static info.archinnov.achilles.entity.metadata.PropertyType.WIDE_MAP;
import static info.archinnov.achilles.entity.type.ConsistencyLevel.ALL;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.dao.Pair;
import info.archinnov.achilles.entity.PropertyHelper;
import info.archinnov.achilles.entity.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.operations.EntityProxifier;
import info.archinnov.achilles.entity.type.ConsistencyLevel;
import info.archinnov.achilles.entity.type.KeyValue;

import java.util.Arrays;
import java.util.List;

import mapping.entity.TweetMultiKey;
import mapping.entity.UserBean;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

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

	@Mock
	private EntityProxifier proxifier;

	@Mock
	private PersistenceContext<Long> context;

	@Mock
	private PersistenceContext<Long> joinContext;

	@Before
	public void setUp()
	{
		Whitebox.setInternalState(transformer, "helper", helper);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_build_single_key_transformer() throws Exception
	{
		Composite comp1 = CompositeTestBuilder.builder().values(45).buildSimple();
		Composite comp2 = CompositeTestBuilder.builder().values(51).buildSimple();
		HColumn<Composite, String> hCol1 = HColumnTestBuilder.simple(comp1, "test1");
		HColumn<Composite, String> hCol2 = HColumnTestBuilder.simple(comp2, "test2");

		PropertyMeta<Integer, String> propertyMeta = PropertyMetaTestBuilder //
				.noClass(Integer.class, String.class)//
				.type(WIDE_MAP)//
				.consistencyLevels(new Pair<ConsistencyLevel, ConsistencyLevel>(ALL, ALL))//
				.build();

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

		when(helper.buildMultiKeyFromComposite(propertyMeta, hCol1.getName().getComponents()))
				.thenReturn(multiKey1);
		when(helper.buildMultiKeyFromComposite(propertyMeta, hCol2.getName().getComponents()))
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

	@Test
	public void should_build_join_value_from_composite() throws Exception
	{
		EntityMeta<Long> joinMeta = new EntityMeta<Long>();
		PropertyMeta<Void, UserBean> propertyMeta = PropertyMetaTestBuilder //
				.completeBean(Void.class, UserBean.class) //
				.field("user") //
				.joinMeta(joinMeta) //
				.type(PropertyType.JOIN_SIMPLE) //
				.build();

		UserBean user = new UserBean();
		Composite comp = new Composite();
		HColumn<Composite, UserBean> hColumn = HColumnTestBuilder.simple(comp, user);

		when(context.newPersistenceContext(joinMeta, hColumn.getValue())).thenReturn(joinContext);
		when(proxifier.buildProxy(hColumn.getValue(), joinContext)).thenReturn(user);
		UserBean actual = transformer.buildValue(context, propertyMeta, hColumn);

		assertThat(actual).isSameAs(user);
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
				.noClass(Integer.class, String.class) //
				.type(WIDE_MAP)//
				.consistencyLevels(new Pair<ConsistencyLevel, ConsistencyLevel>(ALL, ALL))//
				.build();

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
}
