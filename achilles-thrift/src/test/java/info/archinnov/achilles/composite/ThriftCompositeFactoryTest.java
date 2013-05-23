package info.archinnov.achilles.composite;

import static info.archinnov.achilles.entity.metadata.PropertyType.SIMPLE;
import static info.archinnov.achilles.serializer.ThriftSerializerUtils.*;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.entity.metadata.MultiKeyProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.helper.ThriftCompositeHelper;
import info.archinnov.achilles.proxy.AchillesMethodInvoker;
import info.archinnov.achilles.type.WideMap.BoundingMode;
import info.archinnov.achilles.type.WideMap.OrderingMode;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import mapping.entity.TweetMultiKey;
import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.Composite;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import testBuilders.PropertyMetaTestBuilder;

/**
 * ThriftCompositeFactoryTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class ThriftCompositeFactoryTest
{
	@Rule
	public ExpectedException expectedEx = ExpectedException.none();

	@InjectMocks
	private ThriftCompositeFactory factory;

	@Mock
	private ThriftCompositeHelper helper;

	@Mock
	private AchillesMethodInvoker invoker;

	@Mock
	private PropertyMeta<Integer, String> wideMapMeta;

	@Mock
	private PropertyMeta<TweetMultiKey, String> multiKeyWideMapMeta;

	@Mock
	private MultiKeyProperties multiKeyProperties;

	@Before
	public void setUp()
	{
		when(wideMapMeta.isSingleKey()).thenReturn(true);
		when(wideMapMeta.getPropertyName()).thenReturn("property");
		when((Class<Integer>) wideMapMeta.getKeyClass()).thenReturn(Integer.class);

		when(multiKeyWideMapMeta.isSingleKey()).thenReturn(false);
		when(multiKeyWideMapMeta.getPropertyName()).thenReturn("property");
		when(multiKeyWideMapMeta.getMultiKeyProperties()).thenReturn(multiKeyProperties);
	}

	@Test
	public void should_create_for_insert() throws Exception
	{
		Composite comp = factory.createBaseComposite(wideMapMeta, 12);

		assertThat(comp.getComponents()).hasSize(1);
		assertThat((Integer) comp.getComponents().get(0).getValue()).isEqualTo(12);
	}

	@Test
	public void should_create_multikey_for_insert() throws Exception
	{
		UUID uuid = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
		TweetMultiKey tweetMultiKey = prepareData(1, "a", uuid);

		Composite comp = factory.createBaseComposite(multiKeyWideMapMeta, tweetMultiKey);

		assertThat(comp.getComponents()).hasSize(3);
		assertThat((Integer) comp.getComponents().get(0).getValue()).isEqualTo(1);
		assertThat((String) comp.getComponents().get(1).getValue()).isEqualTo("a");
		assertThat((UUID) comp.getComponents().get(2).getValue()).isEqualTo(uuid);
	}

	@Test
	public void should_exception_when_missing_value() throws Exception
	{
		TweetMultiKey tweetMultiKey = prepareData(1, "a");

		expectedEx.expect(AchillesException.class);
		expectedEx.expectMessage("There should be 3 values for the key of WideMap 'property'");

		factory.createBaseComposite(multiKeyWideMapMeta, tweetMultiKey);

	}

	@Test
	public void should_exception_when_null_value() throws Exception
	{
		TweetMultiKey tweetMultiKey = prepareData(1, "a", null);

		expectedEx.expect(AchillesException.class);
		expectedEx
				.expectMessage("The values for the for the key of WideMap 'property' should not be null");

		factory.createBaseComposite(multiKeyWideMapMeta, tweetMultiKey);

	}

	@Test
	public void should_create_for_query() throws Exception
	{

		Composite comp = factory.createForQuery(wideMapMeta, 123, LESS_THAN_EQUAL);

		assertThat(comp.getComponents()).hasSize(1);
		assertThat((Integer) comp.getComponents().get(0).getValue()).isEqualTo(123);
		assertThat(comp.getComponents().get(0).getEquality()).isEqualTo(LESS_THAN_EQUAL);
	}

	@Test
	public void should_create_null_for_query() throws Exception
	{
		Composite comp = factory.createForQuery(wideMapMeta, null, LESS_THAN_EQUAL);
		assertThat(comp).isNull();
	}

	@Test
	public void should_create_multikey_for_query() throws Exception
	{
		List<Object> keyValues = Arrays.asList((Object) 1, "a", null);
		TweetMultiKey tweetMultiKey = prepareData(1, "a", null);

		when(helper.findLastNonNullIndexForComponents("property", keyValues)).thenReturn(1);

		Composite comp = factory
				.createForQuery(multiKeyWideMapMeta, tweetMultiKey, LESS_THAN_EQUAL);

		assertThat(comp.getComponents()).hasSize(2);

		assertThat(comp.getComponent(0).getEquality()).isEqualTo(EQUAL);
		assertThat(comp.getComponent(0).getValue()).isEqualTo(1);

		assertThat(comp.getComponent(1).getEquality()).isEqualTo(LESS_THAN_EQUAL);
		assertThat(comp.getComponent(1).getValue()).isEqualTo("a");

	}

	@Test
	public void should_create_composites_for_query() throws Exception
	{

		when(
				helper.determineEquality(BoundingMode.INCLUSIVE_START_BOUND_ONLY,
						OrderingMode.ASCENDING)) //
				.thenReturn(new ComponentEquality[]
				{
						EQUAL,
						LESS_THAN_EQUAL
				});
		Composite[] composites = factory.createForQuery(wideMapMeta, 12, 15,
				BoundingMode.INCLUSIVE_START_BOUND_ONLY, OrderingMode.ASCENDING);

		assertThat(composites).hasSize(2);
		assertThat(composites[0].getComponent(0).getEquality()).isEqualTo(EQUAL);
		assertThat(composites[0].getComponent(0).getValue()).isEqualTo(12);
		assertThat(composites[1].getComponent(0).getEquality()).isEqualTo(LESS_THAN_EQUAL);
		assertThat(composites[1].getComponent(0).getValue()).isEqualTo(15);
	}

	@Test
	public void should_create_multikey_composites_for_query() throws Exception
	{
		TweetMultiKey tweetKey1 = new TweetMultiKey();
		TweetMultiKey tweetKey2 = new TweetMultiKey();
		List<Method> componentGetters = mock(List.class);
		List<Class<?>> componentClasses = Arrays.asList((Class<?>) Integer.class, String.class,
				UUID.class);
		List<Object> keyValues1 = Arrays.asList((Object) 1, "a", null);
		UUID uuid = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
		List<Object> keyValues2 = Arrays.asList((Object) 5, "c", uuid);

		when(
				helper.determineEquality(BoundingMode.INCLUSIVE_END_BOUND_ONLY,
						OrderingMode.ASCENDING)) //
				.thenReturn(new ComponentEquality[]
				{
						LESS_THAN_EQUAL,
						GREATER_THAN_EQUAL
				});
		when(multiKeyProperties.getComponentGetters()).thenReturn(componentGetters);
		when(invoker.determineMultiKeyValues(tweetKey1, componentGetters)).thenReturn(keyValues1);
		when(invoker.determineMultiKeyValues(tweetKey2, componentGetters)).thenReturn(keyValues2);

		when(multiKeyProperties.getComponentClasses()).thenReturn(componentClasses);

		when(helper.findLastNonNullIndexForComponents("property", keyValues1)).thenReturn(1);
		when(helper.findLastNonNullIndexForComponents("property", keyValues2)).thenReturn(2);

		Composite[] composites = factory.createForQuery(
				//
				multiKeyWideMapMeta, tweetKey1, tweetKey2, BoundingMode.INCLUSIVE_END_BOUND_ONLY,
				OrderingMode.ASCENDING);

		assertThat(composites).hasSize(2);
		assertThat(composites[0].getComponent(0).getEquality()).isEqualTo(EQUAL);
		assertThat(composites[0].getComponent(0).getValue()).isEqualTo(1);
		assertThat(composites[0].getComponent(1).getEquality()).isEqualTo(LESS_THAN_EQUAL);
		assertThat(composites[0].getComponent(1).getValue()).isEqualTo("a");

		assertThat(composites[1].getComponent(0).getEquality()).isEqualTo(EQUAL);
		assertThat(composites[1].getComponent(0).getValue()).isEqualTo(5);
		assertThat(composites[1].getComponent(1).getEquality()).isEqualTo(EQUAL);
		assertThat(composites[1].getComponent(1).getValue()).isEqualTo("c");
		assertThat(composites[1].getComponent(2).getEquality()).isEqualTo(GREATER_THAN_EQUAL);
		assertThat(composites[1].getComponent(2).getValue()).isEqualTo(uuid);
	}

	@Test
	public void should_create_key_for_counter() throws Exception
	{
		PropertyMeta<Void, Long> idMeta = PropertyMetaTestBuilder.valueClass(Long.class).build();

		Composite comp = factory.createKeyForCounter("fqcn", 11L, idMeta);

		assertThat(comp.getComponents()).hasSize(2);
		assertThat(comp.getComponent(0).getValue(STRING_SRZ)).isEqualTo("fqcn");
		assertThat(comp.getComponent(1).getValue(STRING_SRZ)).isEqualTo("11");
	}

	@Test
	public void should_create_base_for_get() throws Exception
	{
		PropertyMeta<Void, Long> meta = PropertyMetaTestBuilder
				.valueClass(Long.class)
				.type(SIMPLE)
				.field("name")
				.build();

		Composite comp = factory.createBaseForGet(meta);

		assertThat(comp.getComponents()).hasSize(3);
		assertThat(comp.getComponent(0).getValue(BYTE_SRZ)).isEqualTo(SIMPLE.flag());
		assertThat(comp.getComponent(0).getEquality()).isEqualTo(EQUAL);
		assertThat(comp.getComponent(1).getValue(STRING_SRZ)).isEqualTo("name");
		assertThat(comp.getComponent(1).getEquality()).isEqualTo(EQUAL);
		assertThat(comp.getComponent(2).getValue(INT_SRZ)).isEqualTo(0);
		assertThat(comp.getComponent(2).getEquality()).isEqualTo(EQUAL);
	}

	@Test
	public void should_create_base_for_counter_get() throws Exception
	{
		PropertyMeta<Void, Long> meta = PropertyMetaTestBuilder
				.valueClass(Long.class)
				.type(SIMPLE)
				.field("name")
				.build();

		Composite comp = factory.createBaseForCounterGet(meta);

		assertThat(comp.getComponents()).hasSize(1);
		assertThat(comp.getComponent(0).getValue(STRING_SRZ)).isEqualTo("name");
		assertThat(comp.getComponent(0).getEquality()).isEqualTo(EQUAL);
	}

	@Test
	public void should_create_base_for_query() throws Exception
	{
		PropertyMeta<Void, Long> meta = PropertyMetaTestBuilder
				.valueClass(Long.class)
				.type(SIMPLE)
				.field("name")
				.build();

		Composite comp = factory.createBaseForQuery(meta, GREATER_THAN_EQUAL);

		assertThat(comp.getComponents()).hasSize(2);
		assertThat(comp.getComponent(0).getValue(BYTE_SRZ)).isEqualTo(SIMPLE.flag());
		assertThat(comp.getComponent(0).getEquality()).isEqualTo(EQUAL);
		assertThat(comp.getComponent(1).getValue(STRING_SRZ)).isEqualTo("name");
		assertThat(comp.getComponent(1).getEquality()).isEqualTo(GREATER_THAN_EQUAL);
	}

	@Test
	public void should_create_for_batch_insert_single() throws Exception
	{
		PropertyMeta<Void, Long> meta = PropertyMetaTestBuilder
				.valueClass(Long.class)
				.type(SIMPLE)
				.field("name")
				.build();

		Composite comp = factory.createForBatchInsertSingleValue(meta);

		assertThat(comp.getComponents()).hasSize(3);
		assertThat(comp.getComponent(0).getValue(BYTE_SRZ)).isEqualTo(SIMPLE.flag());
		assertThat(comp.getComponent(1).getValue(STRING_SRZ)).isEqualTo("name");
		assertThat(comp.getComponent(2).getValue(INT_SRZ)).isEqualTo(0);
	}

	@Test
	public void should_create_for_batch_insert_single_counter() throws Exception
	{
		PropertyMeta<Void, Long> meta = PropertyMetaTestBuilder
				.valueClass(Long.class)
				.type(SIMPLE)
				.field("name")
				.build();

		Composite comp = factory.createForBatchInsertSingleCounter(meta);

		assertThat(comp.getComponents()).hasSize(1);
		assertThat(comp.getComponent(0).getValue(STRING_SRZ)).isEqualTo("name");
	}

	@Test
	public void should_create_for_batch_insert_multiple() throws Exception
	{
		PropertyMeta<Void, Long> meta = PropertyMetaTestBuilder
				.valueClass(Long.class)
				.type(SIMPLE)
				.field("name")
				.build();

		Composite comp = factory.createForBatchInsertMultiValue(meta, 21);

		assertThat(comp.getComponents()).hasSize(3);
		assertThat(comp.getComponent(0).getValue(BYTE_SRZ)).isEqualTo(SIMPLE.flag());
		assertThat(comp.getComponent(1).getValue(STRING_SRZ)).isEqualTo("name");
		assertThat(comp.getComponent(2).getValue(INT_SRZ)).isEqualTo(21);
	}

	private TweetMultiKey prepareData(Object... objects)
	{
		TweetMultiKey tweetMultiKey = new TweetMultiKey();
		List<Method> componentGetters = mock(List.class);
		List<Class<?>> componentClasses = Arrays.asList((Class<?>) Integer.class, String.class,
				UUID.class);
		List<Object> keyValues = Arrays.asList(objects);

		when(multiKeyProperties.getComponentClasses()).thenReturn(componentClasses);
		when(multiKeyProperties.getComponentGetters()).thenReturn(componentGetters);
		when(invoker.determineMultiKeyValues(tweetMultiKey, componentGetters))
				.thenReturn(keyValues);
		return tweetMultiKey;
	}
}
