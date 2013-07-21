package info.archinnov.achilles.compound;

import static info.archinnov.achilles.entity.metadata.PropertyType.EMBEDDED_ID;
import static info.archinnov.achilles.serializer.ThriftSerializerUtils.*;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.serializer.ThriftEnumSerializer;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.TweetCompoundKey;
import info.archinnov.achilles.test.parser.entity.CompoundKey;
import info.archinnov.achilles.test.parser.entity.CompoundKeyByConstructor;
import info.archinnov.achilles.test.parser.entity.CompoundKeyByConstructorWithEnum;
import info.archinnov.achilles.test.parser.entity.CompoundKeyWithEnum;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import me.prettyprint.cassandra.model.HColumnImpl;
import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;

import org.apache.commons.lang.math.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

@RunWith(MockitoJUnitRunner.class)
public class ThriftCompoundKeyMapperTest
{

	@Rule
	public ExpectedException expectedEx = ExpectedException.none();

	@InjectMocks
	private ThriftCompoundKeyMapper mapper;

	@Mock
	private PropertyMeta<TweetCompoundKey, String> compoundKeyMeta;

	@Mock
	private PropertyMeta<CompoundKeyWithEnum, String> compoundKeyWithEnumMeta;

	@Mock
	private PropertyMeta<CompoundKeyByConstructor, String> compoundKeyByConstructorMeta;

	@Mock
	private PropertyMeta<CompoundKeyByConstructorWithEnum, String> compoundKeyByConstructorWithEnumMeta;

	@Test
	public void should_build_compound_key() throws Exception
	{
		Method authorSetter = TweetCompoundKey.class.getDeclaredMethod("setAuthor", String.class);
		Method idSetter = TweetCompoundKey.class.getDeclaredMethod("setId", UUID.class);
		Method retweetCountSetter = TweetCompoundKey.class.getDeclaredMethod("setRetweetCount",
				Integer.class);

		Constructor<TweetCompoundKey> constructor = TweetCompoundKey.class.getConstructor();

		UUID uuid1 = TimeUUIDUtils.getUniqueTimeUUIDinMillis();

		HColumn<Composite, String> hCol1 = buildHColumn(buildComposite("author1", uuid1, 11),
				"val1");

		when(compoundKeyMeta.hasDefaultConstructorForCompoundKey()).thenReturn(true);
		when(compoundKeyMeta.<TweetCompoundKey> getCompoundKeyConstructor())
				.thenReturn(constructor);
		when(compoundKeyMeta.getKeyClass()).thenReturn(TweetCompoundKey.class);
		when(compoundKeyMeta.getComponentClasses()).thenReturn(
				Arrays.<Class<?>> asList(String.class, UUID.class, Integer.class));
		when(compoundKeyMeta.getComponentSetters()).thenReturn(
				Arrays.asList(authorSetter, idSetter, retweetCountSetter));

		TweetCompoundKey compoundKey = mapper.fromCompositeToCompound(compoundKeyMeta, hCol1
				.getName()
				.getComponents());

		assertThat(compoundKey.getAuthor()).isEqualTo("author1");
		assertThat(compoundKey.getId()).isEqualTo(uuid1);
		assertThat(compoundKey.getRetweetCount()).isEqualTo(11);
	}

	@Test
	public void should_build_embedded_id() throws Exception
	{
		Long userId = RandomUtils.nextLong();
		String name = "name";
		Method userIdSetter = CompoundKey.class.getDeclaredMethod("setUserId", Long.class);
		Method nameSetter = CompoundKey.class.getDeclaredMethod("setName", String.class);

		Constructor<CompoundKey> constructor = CompoundKey.class.getConstructor();

		HColumn<Composite, String> hCol1 = buildHColumn(buildComposite(name), "val1");

		PropertyMeta<Void, CompoundKey> idMeta = PropertyMetaTestBuilder
				.valueClass(CompoundKey.class)
				.compClasses(Arrays.<Class<?>> asList(Long.class, String.class))
				.compSetters(Arrays.asList(userIdSetter, nameSetter))
				.build();
		idMeta.getCompoundKeyProperties().setConstructor(constructor);

		CompoundKey compoundKey = mapper.fromCompositeToEmbeddedId(idMeta, hCol1
				.getName()
				.getComponents(), userId);

		assertThat(compoundKey.getUserId()).isEqualTo(userId);
		assertThat(compoundKey.getName()).isEqualTo(name);
	}

	@Test
	public void should_build_embedded_id_by_constructor() throws Exception
	{
		Long partitionKey = RandomUtils.nextLong();
		String name = "name";

		CompoundKeyByConstructor compoundKey = new CompoundKeyByConstructor(partitionKey, name);
		Constructor<CompoundKeyByConstructor> constructor = CompoundKeyByConstructor.class
				.getConstructor(Long.class,
						String.class);

		HColumn<Composite, String> hCol1 = buildHColumn(buildComposite(name), "val1");

		PropertyMeta<Void, CompoundKeyByConstructor> idMeta = PropertyMetaTestBuilder
				.valueClass(CompoundKeyByConstructor.class)
				.compClasses(Arrays.<Class<?>> asList(Long.class, String.class)).build();
		idMeta.getCompoundKeyProperties().setConstructor(constructor);

		ReflectionInvoker invoker = mock(ReflectionInvoker.class);
		Whitebox.setInternalState(mapper, "invoker", invoker);

		when(invoker.instanciate(eq(constructor), anyVararg())).thenReturn(compoundKey);

		CompoundKeyByConstructor actual = mapper.fromCompositeToEmbeddedId(idMeta, hCol1.getName()
				.getComponents(), partitionKey);

		assertThat(actual).isSameAs(compoundKey);
	}

	@Test
	public void should_build_compound_key_with_enum() throws Exception
	{
		Method idSetter = CompoundKeyWithEnum.class.getDeclaredMethod("setId", Long.class);
		Method typeSetter = CompoundKeyWithEnum.class.getDeclaredMethod("setType",
				PropertyType.class);
		Constructor<CompoundKeyWithEnum> constructor = CompoundKeyWithEnum.class.getConstructor();

		Long id = RandomUtils.nextLong();

		HColumn<Composite, String> hCol1 = buildHColumn(buildCompositeWithEnum(id, EMBEDDED_ID),
				"val1");

		when(compoundKeyWithEnumMeta.hasDefaultConstructorForCompoundKey()).thenReturn(true);
		when(compoundKeyWithEnumMeta.<CompoundKeyWithEnum> getCompoundKeyConstructor()).thenReturn(
				constructor);
		when(compoundKeyWithEnumMeta.getKeyClass()).thenReturn(CompoundKeyWithEnum.class);
		when(compoundKeyWithEnumMeta.getComponentClasses()).thenReturn(
				Arrays.<Class<?>> asList(Long.class, PropertyType.class));
		when(compoundKeyWithEnumMeta.getComponentSetters()).thenReturn(
				Arrays.asList(idSetter, typeSetter));

		CompoundKeyWithEnum compoundKey = mapper.fromCompositeToCompound(compoundKeyWithEnumMeta,
				hCol1.getName()
						.getComponents());

		assertThat(compoundKey.getId()).isEqualTo(id);
		assertThat(compoundKey.getType()).isEqualTo(EMBEDDED_ID);
	}

	@Test
	public void should_build_compound_key_from_constructor() throws Exception
	{

		Constructor<CompoundKeyByConstructor> constructor = CompoundKeyByConstructor.class
				.getConstructor(Long.class,
						String.class);

		HColumn<Composite, String> hCol1 = buildHColumn(buildComposite(11L, "name"), "val1");

		when(compoundKeyByConstructorMeta.hasDefaultConstructorForCompoundKey()).thenReturn(false);
		when(compoundKeyByConstructorMeta.<CompoundKeyByConstructor> getCompoundKeyConstructor())
				.thenReturn(
						constructor);
		when(compoundKeyByConstructorMeta.getKeyClass()).thenReturn(CompoundKeyByConstructor.class);
		when(compoundKeyByConstructorMeta.getComponentClasses()).thenReturn(
				Arrays.<Class<?>> asList(Long.class, String.class));

		CompoundKeyByConstructor compoundKey = mapper.fromCompositeToCompound(
				compoundKeyByConstructorMeta, hCol1
						.getName().getComponents());

		assertThat(compoundKey.getId()).isEqualTo(11L);
		assertThat(compoundKey.getName()).isEqualTo("name");
	}

	@Test
	public void should_build_compound_key_from_constructor_with_enum() throws Exception
	{

		Constructor<CompoundKeyByConstructorWithEnum> constructor = CompoundKeyByConstructorWithEnum.class
				.getConstructor(Long.class, PropertyType.class);

		HColumn<Composite, String> hCol1 = buildHColumn(buildCompositeWithEnum(11L, EMBEDDED_ID),
				"val1");

		when(compoundKeyByConstructorWithEnumMeta.hasDefaultConstructorForCompoundKey())
				.thenReturn(false);
		when(
				compoundKeyByConstructorWithEnumMeta
						.<CompoundKeyByConstructorWithEnum> getCompoundKeyConstructor())
				.thenReturn(constructor);
		when(compoundKeyByConstructorWithEnumMeta.getKeyClass()).thenReturn(
				CompoundKeyByConstructorWithEnum.class);
		when(compoundKeyByConstructorWithEnumMeta.getComponentClasses()).thenReturn(
				Arrays.<Class<?>> asList(Long.class, PropertyType.class));

		CompoundKeyByConstructorWithEnum compoundKey = mapper.fromCompositeToCompound(
				compoundKeyByConstructorWithEnumMeta, hCol1.getName().getComponents());

		assertThat(compoundKey.getId()).isEqualTo(11L);
		assertThat(compoundKey.getType()).isEqualTo(EMBEDDED_ID);
	}

	@Test
	public void should_create_composite_for_compound_key_insert() throws Exception
	{
		Long id = RandomUtils.nextLong();
		CompoundKeyWithEnum cpKey = new CompoundKeyWithEnum();
		cpKey.setId(id);
		cpKey.setType(EMBEDDED_ID);

		Method idGetter = CompoundKeyWithEnum.class.getDeclaredMethod("getId");
		Method typeGetter = CompoundKeyWithEnum.class.getDeclaredMethod("getType");

		when(compoundKeyMeta.isEmbeddedId()).thenReturn(false);
		when(compoundKeyMeta.getComponentClasses()).thenReturn(
				Arrays.<Class<?>> asList(Long.class, PropertyType.class));
		when(compoundKeyMeta.getComponentGetters()).thenReturn(Arrays.asList(idGetter, typeGetter));

		Composite comp = mapper.fromCompoundToCompositeForInsertOrGet(cpKey, compoundKeyMeta);

		Serializer<Enum<PropertyType>> srz = new ThriftEnumSerializer<PropertyType>(
				PropertyType.class);

		assertThat(comp.getComponents()).hasSize(2);
		assertThat(comp.getComponents().get(0).getValue(LONG_SRZ)).isEqualTo(id);
		assertThat(comp.getComponents().get(1).getValue(srz)).isEqualTo(EMBEDDED_ID);
	}

	@Test
	public void should_create_composite_for_embedded_id_insert() throws Exception
	{
		Long id = RandomUtils.nextLong();
		CompoundKeyWithEnum cpKey = new CompoundKeyWithEnum();
		cpKey.setId(id);
		cpKey.setType(EMBEDDED_ID);

		Method idGetter = CompoundKeyWithEnum.class.getDeclaredMethod("getId");
		Method typeGetter = CompoundKeyWithEnum.class.getDeclaredMethod("getType");

		when(compoundKeyMeta.isEmbeddedId()).thenReturn(true);
		when(compoundKeyMeta.getComponentClasses()).thenReturn(
				Arrays.<Class<?>> asList(Long.class, PropertyType.class));
		when(compoundKeyMeta.getComponentGetters()).thenReturn(Arrays.asList(idGetter, typeGetter));

		Composite comp = mapper.fromCompoundToCompositeForInsertOrGet(cpKey, compoundKeyMeta);

		Serializer<Enum<PropertyType>> srz = new ThriftEnumSerializer<PropertyType>(
				PropertyType.class);

		assertThat(comp.getComponents()).hasSize(1);
		assertThat(comp.getComponents().get(0).getValue(srz)).isEqualTo(EMBEDDED_ID);
	}

	@Test
	public void should_exception_when_null_value() throws Exception
	{
		CompoundKeyWithEnum cpKey = new CompoundKeyWithEnum();
		Method idGetter = CompoundKeyWithEnum.class.getDeclaredMethod("getId");
		Method typeGetter = CompoundKeyWithEnum.class.getDeclaredMethod("getType");

		when(compoundKeyMeta.getPropertyName()).thenReturn("compound_key");
		when(compoundKeyMeta.getComponentClasses()).thenReturn(
				Arrays.<Class<?>> asList(Long.class, PropertyType.class));
		when(compoundKeyMeta.getComponentGetters()).thenReturn(Arrays.asList(idGetter, typeGetter));
		expectedEx.expect(AchillesException.class);
		expectedEx
				.expectMessage("The values for the @CompoundKey 'compound_key' should not be null");

		mapper.fromCompoundToCompositeForInsertOrGet(cpKey, compoundKeyMeta);

	}

	@Test
	public void should_create_composite_for_compound_key_query() throws Exception
	{
		UUID uuid = TimeUUIDUtils.getUniqueTimeUUIDinMillis();

		Method idGetter = TweetCompoundKey.class.getDeclaredMethod("getId");
		Method authorGetter = TweetCompoundKey.class.getDeclaredMethod("getAuthor");
		Method retweetCountGetter = TweetCompoundKey.class.getDeclaredMethod("getRetweetCount");

		when(compoundKeyMeta.isEmbeddedId()).thenReturn(false);
		when(compoundKeyMeta.getComponentClasses()).thenReturn(
				Arrays.<Class<?>> asList(UUID.class, String.class, Integer.class));
		when(compoundKeyMeta.getComponentGetters()).thenReturn(
				Arrays.asList(idGetter, authorGetter, retweetCountGetter));

		TweetCompoundKey tweetMultiKey = new TweetCompoundKey(uuid, "a", 15);

		Composite comp = mapper.fromCompoundToCompositeForQuery(tweetMultiKey, compoundKeyMeta,
				LESS_THAN_EQUAL);

		assertThat(comp.getComponents()).hasSize(3);

		assertThat(comp.getComponent(0).getEquality()).isEqualTo(EQUAL);
		assertThat(comp.getComponent(0).getValue()).isEqualTo(uuid);

		assertThat(comp.getComponent(1).getEquality()).isEqualTo(EQUAL);
		assertThat(comp.getComponent(1).getValue()).isEqualTo("a");

		assertThat(comp.getComponent(2).getEquality()).isEqualTo(LESS_THAN_EQUAL);
		assertThat(comp.getComponent(2).getValue()).isEqualTo(15);
	}

	@Test
	public void should_create_composite_for_embedded_id_query() throws Exception
	{
		UUID uuid = TimeUUIDUtils.getUniqueTimeUUIDinMillis();

		Method idGetter = TweetCompoundKey.class.getDeclaredMethod("getId");
		Method authorGetter = TweetCompoundKey.class.getDeclaredMethod("getAuthor");
		Method retweetCountGetter = TweetCompoundKey.class.getDeclaredMethod("getRetweetCount");

		when(compoundKeyMeta.isEmbeddedId()).thenReturn(true);
		when(compoundKeyMeta.getComponentClasses()).thenReturn(
				Arrays.<Class<?>> asList(UUID.class, String.class, Integer.class));
		when(compoundKeyMeta.getComponentGetters()).thenReturn(
				Arrays.asList(idGetter, authorGetter, retweetCountGetter));

		TweetCompoundKey tweetMultiKey = new TweetCompoundKey(uuid, "a", 15);

		Composite comp = mapper.fromCompoundToCompositeForQuery(tweetMultiKey, compoundKeyMeta,
				LESS_THAN_EQUAL);

		assertThat(comp.getComponents()).hasSize(2);

		assertThat(comp.getComponent(0).getEquality()).isEqualTo(EQUAL);
		assertThat(comp.getComponent(0).getValue()).isEqualTo("a");

		assertThat(comp.getComponent(1).getEquality()).isEqualTo(LESS_THAN_EQUAL);
		assertThat(comp.getComponent(1).getValue()).isEqualTo(15);
	}

	@Test
	public void should_exception_when_more_values_than_components() throws Exception
	{
		UUID uuid = TimeUUIDUtils.getUniqueTimeUUIDinMillis();

		Method idGetter = TweetCompoundKey.class.getDeclaredMethod("getId");
		Method authorGetter = TweetCompoundKey.class.getDeclaredMethod("getAuthor");
		Method retweetCountGetter = TweetCompoundKey.class.getDeclaredMethod("getRetweetCount");

		when(compoundKeyMeta.isEmbeddedId()).thenReturn(false);
		when(compoundKeyMeta.getPropertyName()).thenReturn("compound_key");
		when(compoundKeyMeta.getComponentClasses()).thenReturn(
				Arrays.<Class<?>> asList(UUID.class, String.class));
		when(compoundKeyMeta.getComponentGetters()).thenReturn(
				Arrays.asList(idGetter, authorGetter, retweetCountGetter));

		TweetCompoundKey tweetMultiKey = new TweetCompoundKey(uuid, "a", null);

		expectedEx.expect(AchillesException.class);
		expectedEx
				.expectMessage("There should be at most 2 values for the @CompoundKey 'compound_key'");

		mapper.fromCompoundToCompositeForQuery(tweetMultiKey, compoundKeyMeta, LESS_THAN_EQUAL);

	}

	@Test
	public void should_determine_compound_key() throws Exception
	{
		Method idGetter = TweetCompoundKey.class.getDeclaredMethod("getId");
		Method authorGetter = TweetCompoundKey.class.getDeclaredMethod("getAuthor");
		Method retweetCountGetter = TweetCompoundKey.class.getDeclaredMethod("getRetweetCount");

		TweetCompoundKey multiKey = new TweetCompoundKey();
		UUID uuid = new UUID(10L, 100L);

		multiKey.setId(uuid);
		multiKey.setAuthor("author");
		multiKey.setRetweetCount(12);

		List<Object> multiKeyList = mapper.fromCompoundToComponents(multiKey,
				Arrays.asList(idGetter, authorGetter, retweetCountGetter));

		assertThat(multiKeyList).hasSize(3);
		assertThat(multiKeyList.get(0)).isEqualTo(uuid);
		assertThat(multiKeyList.get(1)).isEqualTo("author");
		assertThat(multiKeyList.get(2)).isEqualTo(12);
	}

	@Test
	public void should_determine_compound_key_with_enum() throws Exception
	{
		long id = RandomUtils.nextLong();
		Method idGetter = CompoundKeyWithEnum.class.getDeclaredMethod("getId");
		Method typeGetter = CompoundKeyWithEnum.class.getDeclaredMethod("getType");

		CompoundKeyWithEnum compoundKey = new CompoundKeyWithEnum();

		compoundKey.setId(id);
		compoundKey.setType(EMBEDDED_ID);

		List<Object> components = mapper.fromCompoundToComponents(compoundKey,
				Arrays.asList(idGetter, typeGetter));

		assertThat(components).hasSize(2);
		assertThat(components.get(0)).isEqualTo(id);
		assertThat(components.get(1)).isEqualTo(EMBEDDED_ID);
	}

	@Test
	public void should_determine_compound_components_with_null() throws Exception
	{
		Method idGetter = TweetCompoundKey.class.getDeclaredMethod("getId");
		Method authorGetter = TweetCompoundKey.class.getDeclaredMethod("getAuthor");
		Method retweetCountGetter = TweetCompoundKey.class.getDeclaredMethod("getRetweetCount");

		TweetCompoundKey multiKey = new TweetCompoundKey();
		UUID uuid = new UUID(10L, 100L);

		multiKey.setId(uuid);
		multiKey.setRetweetCount(12);

		List<Object> multiKeyList = mapper.fromCompoundToComponents(multiKey,
				Arrays.asList(idGetter, authorGetter, retweetCountGetter));

		assertThat(multiKeyList).hasSize(3);
		assertThat(multiKeyList.get(0)).isEqualTo(uuid);
		assertThat(multiKeyList.get(1)).isNull();
		assertThat(multiKeyList.get(2)).isEqualTo(12);
	}

	@Test
	public void should_return_empty_compound_key_when_null_entity() throws Exception
	{
		assertThat(mapper.fromCompoundToComponents(null, new ArrayList<Method>())).isEmpty();
	}

	@Test
	public void should_validate_no_hole() throws Exception
	{
		List<Object> keyValues = Arrays.asList((Object) "a", "b", null, null);

		int lastNotNullIndex = mapper.validateNoHoleAndReturnLastNonNullIndex(keyValues);

		assertThat(lastNotNullIndex).isEqualTo(1);

	}

	@Test(expected = IllegalArgumentException.class)
	public void should_exception_when_hole() throws Exception
	{
		List<Object> keyValues = Arrays.asList((Object) "a", null, "b");

		mapper.validateNoHoleAndReturnLastNonNullIndex(keyValues);
	}

	@Test(expected = IllegalArgumentException.class)
	public void should_exception_when_starting_with_hole() throws Exception
	{
		List<Object> keyValues = Arrays.asList((Object) null, "a", "b");

		mapper.validateNoHoleAndReturnLastNonNullIndex(keyValues);
	}

	private Composite buildComposite(String author, UUID uuid, int retweetCount)
	{
		Composite composite = new Composite();
		composite.setComponent(0, author, STRING_SRZ, STRING_SRZ.getComparatorType().getTypeName());
		composite.setComponent(1, uuid, UUID_SRZ, UUID_SRZ.getComparatorType().getTypeName());
		composite.setComponent(2, retweetCount, INT_SRZ, INT_SRZ.getComparatorType().getTypeName());

		return composite;
	}

	private Composite buildComposite(String name)
	{
		Composite composite = new Composite();
		composite.setComponent(0, name, STRING_SRZ, STRING_SRZ.getComparatorType().getTypeName());
		return composite;
	}

	private Composite buildComposite(Long id, String name)
	{
		Composite composite = new Composite();
		composite.setComponent(0, id, LONG_SRZ, LONG_SRZ.getComparatorType().getTypeName());
		composite.setComponent(1, name, STRING_SRZ, STRING_SRZ.getComparatorType().getTypeName());
		return composite;
	}

	private Composite buildCompositeWithEnum(Long id, PropertyType type)
	{
		ThriftEnumSerializer<PropertyType> enumSrz = new ThriftEnumSerializer<PropertyType>(
				PropertyType.class);

		Composite composite = new Composite();
		composite.setComponent(0, id, LONG_SRZ, LONG_SRZ.getComparatorType().getTypeName());
		composite.setComponent(1, type, enumSrz, enumSrz.getComparatorType().getTypeName());

		return composite;
	}

	private HColumn<Composite, String> buildHColumn(Composite comp, String value)
	{
		HColumn<Composite, String> hColumn = new HColumnImpl<Composite, String>(COMPOSITE_SRZ,
				STRING_SRZ);

		hColumn.setName(comp);
		hColumn.setValue(value);
		return hColumn;
	}

}
