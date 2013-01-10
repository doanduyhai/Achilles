package fr.doan.achilles.entity;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

import mapping.entity.TweetMultiKey;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import parser.entity.CorrectMultiKey;
import parser.entity.MultiKeyIncorrectType;
import parser.entity.MultiKeyNotInstantiable;
import parser.entity.MultiKeyWithDuplicateOrder;
import parser.entity.MultiKeyWithNegativeOrder;
import parser.entity.MultiKeyWithNoAnnotation;

import com.google.common.collect.Maps;

import fr.doan.achilles.annotations.Lazy;
import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.entity.metadata.MultiKeyProperties;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.metadata.PropertyType;
import fr.doan.achilles.exception.BeanMappingException;
import fr.doan.achilles.exception.AchillesException;

/**
 * PropertyHelperTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class PropertyHelperTest
{
	@Rule
	public ExpectedException expectedEx = ExpectedException.none();

	@InjectMocks
	private PropertyHelper helper;

	@Mock
	private EntityHelper entityHelper;

	@Test
	public void should_parse_multi_key() throws Exception
	{
		Field nameField = CorrectMultiKey.class.getDeclaredField("name");
		Method nameGetter = CorrectMultiKey.class.getMethod("getName");
		Method nameSetter = CorrectMultiKey.class.getMethod("setName", String.class);

		Field rankField = CorrectMultiKey.class.getDeclaredField("rank");
		Method rankGetter = CorrectMultiKey.class.getMethod("getRank");
		Method rankSetter = CorrectMultiKey.class.getMethod("setRank", int.class);

		when(entityHelper.findGetter(CorrectMultiKey.class, nameField)).thenReturn(nameGetter);
		when(entityHelper.findGetter(CorrectMultiKey.class, rankField)).thenReturn(rankGetter);

		when(entityHelper.findSetter(CorrectMultiKey.class, nameField)).thenReturn(nameSetter);
		when(entityHelper.findSetter(CorrectMultiKey.class, rankField)).thenReturn(rankSetter);

		MultiKeyProperties props = helper.parseMultiKey(CorrectMultiKey.class);

		assertThat(props.getComponentGetters()).containsExactly(nameGetter, rankGetter);
		assertThat(props.getComponentSetters()).containsExactly(nameSetter, rankSetter);
		assertThat(props.getComponentClasses()).containsExactly(String.class, int.class);

	}

	@Test
	public void should_exception_when_multi_key_incorrect_type() throws Exception
	{
		expectedEx.expect(AchillesException.class);
		expectedEx
				.expectMessage("The class 'java.util.List' is not a valid key type for the MultiKey class '"
						+ MultiKeyIncorrectType.class.getCanonicalName() + "'");

		helper.parseMultiKey(MultiKeyIncorrectType.class);
	}

	@Test
	public void should_exception_when_multi_key_wrong_key_order() throws Exception
	{
		expectedEx.expect(AchillesException.class);
		expectedEx.expectMessage("The key orders is wrong for MultiKey class '"
				+ MultiKeyWithNegativeOrder.class.getCanonicalName() + "'");

		helper.parseMultiKey(MultiKeyWithNegativeOrder.class);
	}

	@Test
	public void should_exception_when_multi_key_has_no_annotation() throws Exception
	{
		expectedEx.expect(AchillesException.class);
		expectedEx.expectMessage("No field with @Key annotation found in the class '"
				+ MultiKeyWithNoAnnotation.class.getCanonicalName() + "'");

		helper.parseMultiKey(MultiKeyWithNoAnnotation.class);
	}

	@Test
	public void should_exception_when_multi_key_has_duplicate_order() throws Exception
	{
		expectedEx.expect(BeanMappingException.class);

		expectedEx.expectMessage("The order '1' is duplicated in MultiKey '"
				+ MultiKeyWithDuplicateOrder.class.getCanonicalName() + "'");

		helper.parseMultiKey(MultiKeyWithDuplicateOrder.class);
	}

	@Test
	public void should_exception_when_multi_key_not_instantiable() throws Exception
	{
		expectedEx.expect(AchillesException.class);
		expectedEx.expectMessage("The class '" + MultiKeyNotInstantiable.class.getCanonicalName()
				+ "' should have a public default constructor");

		helper.parseMultiKey(MultiKeyNotInstantiable.class);
	}

	@SuppressWarnings(
	{
			"rawtypes",
			"unchecked",
			"unused"
	})
	@Test
	public void should_infer_value_class_from_list() throws Exception
	{
		class Test
		{
			private List<String> friends;
		}

		Type type = Test.class.getDeclaredField("friends").getGenericType();

		Class<?> infered = helper.inferValueClass(type);

		assertThat(infered).isEqualTo((Class) String.class);
	}

	@SuppressWarnings(
	{
			"rawtypes",
			"unchecked",
			"unused"
	})
	@Test
	public void should_infer_value_class_from_raw_list() throws Exception
	{
		class Test
		{
			private List friends;
		}

		Type type = Test.class.getDeclaredField("friends").getGenericType();

		Class<?> infered = helper.inferValueClass(type);

		assertThat(infered).isEqualTo((Class) Object.class);
	}

	@SuppressWarnings(
	{
			"rawtypes",
			"unchecked",
			"unused"
	})
	@Test
	public void should_infer_simple_key_class() throws Exception
	{
		class Test
		{
			private Map<Integer, String> preferences;
		}

		Type type = Test.class.getDeclaredField("preferences").getGenericType();

		Class<?> infered = helper.inferKeyClass(type);

		assertThat(infered).isEqualTo((Class) Integer.class);
	}

	@SuppressWarnings(
	{
			"rawtypes",
			"unchecked",
			"unused"
	})
	@Test
	public void should_infer_multi_key_class() throws Exception
	{
		class Test
		{
			private Map<TweetMultiKey, String> preferences;
		}

		Type type = Test.class.getDeclaredField("preferences").getGenericType();

		Class<?> infered = helper.inferKeyClass(type);

		assertThat(infered).isEqualTo((Class) TweetMultiKey.class);
	}

	@Test
	public void should_find_lazy() throws Exception
	{

		class Test
		{
			@Lazy
			private String name;
		}

		Field field = Test.class.getDeclaredField("name");

		assertThat(helper.isLazy(field)).isTrue();
	}

	@Test
	public void should_determine_composite_type_alias_for_widerow() throws Exception
	{
		EntityMeta<Long> entityMeta = new EntityMeta<Long>();
		PropertyMeta<Integer, String> propertyMeta = new PropertyMeta<Integer, String>();
		propertyMeta.setType(PropertyType.WIDE_MAP);
		propertyMeta.setKeyClass(Integer.class);
		Map<String, PropertyMeta<?, ?>> propertyMap = Maps.newHashMap();
		propertyMap.put("map", propertyMeta);
		entityMeta.setPropertyMetas(propertyMap);

		String compatatorTypeAlias = helper.determineCompatatorTypeAliasForCompositeCF(
				propertyMeta, true);

		assertThat(compatatorTypeAlias).isEqualTo("(BytesType)");
	}

	@Test
	public void should_determine_composite_type_alias_for_multikey_widerow() throws Exception
	{
		EntityMeta<Long> entityMeta = new EntityMeta<Long>();
		PropertyMeta<TweetMultiKey, String> propertyMeta = new PropertyMeta<TweetMultiKey, String>();
		propertyMeta.setType(PropertyType.WIDE_MAP);
		propertyMeta.setKeyClass(TweetMultiKey.class);
		Map<String, PropertyMeta<?, ?>> propertyMap = Maps.newHashMap();
		propertyMap.put("values", propertyMeta);
		entityMeta.setPropertyMetas(propertyMap);

		String compatatorTypeAlias = helper.determineCompatatorTypeAliasForCompositeCF(
				propertyMeta, true);

		assertThat(compatatorTypeAlias).isEqualTo("(UUIDType,UTF8Type,BytesType)");
	}

	@Test
	public void should_determine_composite_type_alias_for_widerow_check() throws Exception
	{
		EntityMeta<Long> entityMeta = new EntityMeta<Long>();
		PropertyMeta<Integer, String> propertyMeta = new PropertyMeta<Integer, String>();
		propertyMeta.setType(PropertyType.WIDE_MAP);
		propertyMeta.setKeyClass(Integer.class);
		Map<String, PropertyMeta<?, ?>> propertyMap = Maps.newHashMap();
		propertyMap.put("map", propertyMeta);
		entityMeta.setPropertyMetas(propertyMap);

		String compatatorTypeAlias = helper.determineCompatatorTypeAliasForCompositeCF(
				propertyMeta, false);

		assertThat(compatatorTypeAlias).isEqualTo(
				"CompositeType(org.apache.cassandra.db.marshal.BytesType)");
	}

	@Test
	public void should_determine_composite_type_alias_for_multikey_widerow_check() throws Exception
	{
		EntityMeta<Long> entityMeta = new EntityMeta<Long>();
		PropertyMeta<TweetMultiKey, String> propertyMeta = new PropertyMeta<TweetMultiKey, String>();
		propertyMeta.setType(PropertyType.WIDE_MAP);
		propertyMeta.setKeyClass(TweetMultiKey.class);
		Map<String, PropertyMeta<?, ?>> propertyMap = Maps.newHashMap();
		propertyMap.put("values", propertyMeta);
		entityMeta.setPropertyMetas(propertyMap);

		String compatatorTypeAlias = helper.determineCompatatorTypeAliasForCompositeCF(
				propertyMeta, false);

		assertThat(compatatorTypeAlias)
				.isEqualTo(
						"CompositeType(org.apache.cassandra.db.marshal.UUIDType,org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.BytesType)");
	}
}
