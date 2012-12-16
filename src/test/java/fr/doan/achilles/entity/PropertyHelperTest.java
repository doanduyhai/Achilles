package fr.doan.achilles.entity;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.inOrder;
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
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import parser.entity.CorrectMultiKey;
import parser.entity.MultiKeyIncorrectType;
import parser.entity.MultiKeyNotInstantiable;
import parser.entity.MultiKeyWithNegativeOrder;
import parser.entity.MultiKeyWithNoAnnotation;
import fr.doan.achilles.annotations.Lazy;
import fr.doan.achilles.exception.ValidationException;

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

	@Mock
	private List<Class<?>> componentClasses;

	@Mock
	private List<Method> componentGetters;

	@Mock
	private List<Method> componentSetters;

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

		helper.parseMultiKey(componentClasses, componentGetters, componentSetters,
				CorrectMultiKey.class);

		InOrder orderGetters = inOrder(componentGetters);
		orderGetters.verify(componentGetters).add(nameGetter);
		orderGetters.verify(componentGetters).add(rankGetter);

		InOrder orderSetters = inOrder(componentSetters);
		orderSetters.verify(componentSetters).add(nameSetter);
		orderSetters.verify(componentSetters).add(rankSetter);

		InOrder orderClasses = inOrder(componentClasses);
		orderClasses.verify(componentClasses).add(String.class);
		orderClasses.verify(componentClasses).add(int.class);
	}

	@Test
	public void should_exception_when_multi_key_incorrect_type() throws Exception
	{
		expectedEx.expect(ValidationException.class);
		expectedEx
				.expectMessage("The class 'java.util.List' is not a valid key type for the MultiKey class '"
						+ MultiKeyIncorrectType.class.getCanonicalName() + "'");

		helper.parseMultiKey(componentClasses, componentGetters, componentSetters,
				MultiKeyIncorrectType.class);
	}

	@Test
	public void should_exception_when_multi_key_wrong_key_order() throws Exception
	{
		expectedEx.expect(ValidationException.class);
		expectedEx.expectMessage("The key orders is wrong for MultiKey class '"
				+ MultiKeyWithNegativeOrder.class.getCanonicalName() + "'");

		helper.parseMultiKey(componentClasses, componentGetters, componentSetters,
				MultiKeyWithNegativeOrder.class);
	}

	@Test
	public void should_exception_when_multi_key_has_no_annotation() throws Exception
	{
		when(componentClasses.isEmpty()).thenReturn(true);
		expectedEx.expect(ValidationException.class);
		expectedEx.expectMessage("No field with @Key annotation found in the class '"
				+ MultiKeyWithNoAnnotation.class.getCanonicalName() + "'");

		helper.parseMultiKey(componentClasses, componentGetters, componentSetters,
				MultiKeyWithNoAnnotation.class);
	}

	@Test
	public void should_exception_when_multi_key_not_instantiable() throws Exception
	{
		when(componentClasses.isEmpty()).thenReturn(false);
		expectedEx.expect(ValidationException.class);
		expectedEx.expectMessage("The class '" + MultiKeyNotInstantiable.class.getCanonicalName()
				+ "' should have a public default constructor");

		helper.parseMultiKey(componentClasses, componentGetters, componentSetters,
				MultiKeyNotInstantiable.class);
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

}
