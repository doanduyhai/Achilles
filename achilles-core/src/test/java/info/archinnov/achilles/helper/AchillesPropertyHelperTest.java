package info.archinnov.achilles.helper;

import static info.archinnov.achilles.type.ConsistencyLevel.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.annotations.Consistency;
import info.archinnov.achilles.annotations.Lazy;
import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.entity.metadata.MultiKeyProperties;
import info.archinnov.achilles.exception.AchillesBeanMappingException;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Pair;
import info.archinnov.achilles.type.WideMap;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.List;

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

/**
 * AchillesPropertyHelperTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class AchillesPropertyHelperTest
{
	@Rule
	public ExpectedException expectedEx = ExpectedException.none();

	@InjectMocks
	private AchillesPropertyHelper helper;

	@Mock
	private AchillesConsistencyLevelPolicy policy;

	@Test
	public void should_parse_multi_key() throws Exception
	{
		Method nameGetter = CorrectMultiKey.class.getMethod("getName");
		Method nameSetter = CorrectMultiKey.class.getMethod("setName", String.class);

		Method rankGetter = CorrectMultiKey.class.getMethod("getRank");
		Method rankSetter = CorrectMultiKey.class.getMethod("setRank", int.class);

		MultiKeyProperties props = helper.parseMultiKey(CorrectMultiKey.class);

		assertThat(props.getComponentGetters()).containsExactly(nameGetter, rankGetter);
		assertThat(props.getComponentSetters()).containsExactly(nameSetter, rankSetter);
		assertThat(props.getComponentClasses()).containsExactly(String.class, int.class);

	}

	@Test
	public void should_exception_when_multi_key_incorrect_type() throws Exception
	{
		expectedEx.expect(AchillesBeanMappingException.class);
		expectedEx
				.expectMessage("The class 'java.util.List' is not a valid key type for the MultiKey class '"
						+ MultiKeyIncorrectType.class.getCanonicalName() + "'");

		helper.parseMultiKey(MultiKeyIncorrectType.class);
	}

	@Test
	public void should_exception_when_multi_key_wrong_key_order() throws Exception
	{
		expectedEx.expect(AchillesBeanMappingException.class);
		expectedEx.expectMessage("The key orders is wrong for MultiKey class '"
				+ MultiKeyWithNegativeOrder.class.getCanonicalName() + "'");

		helper.parseMultiKey(MultiKeyWithNegativeOrder.class);
	}

	@Test
	public void should_exception_when_multi_key_has_no_annotation() throws Exception
	{
		expectedEx.expect(AchillesBeanMappingException.class);
		expectedEx.expectMessage("No field with @Key annotation found in the class '"
				+ MultiKeyWithNoAnnotation.class.getCanonicalName() + "'");

		helper.parseMultiKey(MultiKeyWithNoAnnotation.class);
	}

	@Test
	public void should_exception_when_multi_key_has_duplicate_order() throws Exception
	{
		expectedEx.expect(AchillesBeanMappingException.class);

		expectedEx.expectMessage("The order '1' is duplicated in MultiKey class '"
				+ MultiKeyWithDuplicateOrder.class.getCanonicalName() + "'");

		helper.parseMultiKey(MultiKeyWithDuplicateOrder.class);
	}

	@Test
	public void should_exception_when_multi_key_not_instantiable() throws Exception
	{
		expectedEx.expect(AchillesBeanMappingException.class);
		expectedEx.expectMessage("The class '" + MultiKeyNotInstantiable.class.getCanonicalName()
				+ "' should have a public default constructor");

		helper.parseMultiKey(MultiKeyNotInstantiable.class);
	}

	@Test
	public void should_infer_value_class_from_list() throws Exception
	{
		@SuppressWarnings("unused")
		class Test
		{
			private List<String> friends;
		}

		Type type = Test.class.getDeclaredField("friends").getGenericType();

		Class<String> infered = helper.inferValueClassForListOrSet(type, Test.class);

		assertThat(infered).isEqualTo(String.class);
	}

	@Test
	public void should_exception_when_infering_value_type_from_raw_list() throws Exception
	{
		@SuppressWarnings(
		{
				"rawtypes",
				"unused"
		})
		class Test
		{
			private List friends;
		}

		Type type = Test.class.getDeclaredField("friends").getGenericType();

		expectedEx.expect(AchillesBeanMappingException.class);
		expectedEx.expectMessage("The type '" + type.getClass().getCanonicalName()
				+ "' of the entity 'null' should be parameterized");

		helper.inferValueClassForListOrSet(type, Test.class);

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
	public void should_check_consistency_annotation() throws Exception
	{
		class Test
		{
			@Consistency
			private String consistency;
		}

		Field field = Test.class.getDeclaredField("consistency");

		assertThat(helper.hasConsistencyAnnotation(field)).isTrue();
	}

	@Test
	public void should_not_find_counter_if_not_long_type() throws Exception
	{

	}

	@Test
	public void should_find_any_any_consistency_level() throws Exception
	{
		class Test
		{
			@Consistency(read = ANY, write = LOCAL_QUORUM)
			private WideMap<Integer, String> field;
		}

		when(policy.getDefaultGlobalReadConsistencyLevel()).thenReturn(ONE);
		when(policy.getDefaultGlobalWriteConsistencyLevel()).thenReturn(ONE);

		Pair<ConsistencyLevel, ConsistencyLevel> levels = helper.findConsistencyLevels(
				Test.class.getDeclaredField("field"), policy);

		assertThat(levels.left).isEqualTo(ANY);
		assertThat(levels.right).isEqualTo(LOCAL_QUORUM);
	}

	@Test
	public void should_find_quorum_consistency_level_by_default() throws Exception
	{
		class Test
		{
			@SuppressWarnings("unused")
			private WideMap<Integer, String> field;
		}

		when(policy.getDefaultGlobalReadConsistencyLevel()).thenReturn(ConsistencyLevel.QUORUM);
		when(policy.getDefaultGlobalWriteConsistencyLevel()).thenReturn(ConsistencyLevel.QUORUM);

		Pair<ConsistencyLevel, ConsistencyLevel> levels = helper.findConsistencyLevels(
				Test.class.getDeclaredField("field"), policy);

		assertThat(levels.left).isEqualTo(ConsistencyLevel.QUORUM);
		assertThat(levels.right).isEqualTo(ConsistencyLevel.QUORUM);
	}

	@Test
	public void should_return_true_when_type_supported() throws Exception
	{
		assertThat(AchillesPropertyHelper.isSupportedType(Long.class)).isTrue();
	}

}
