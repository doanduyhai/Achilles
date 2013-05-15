package info.archinnov.achilles.helper;

import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.EQUAL;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.GREATER_THAN_EQUAL;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.LESS_THAN_EQUAL;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import info.archinnov.achilles.entity.AchillesEntityIntrospector;
import info.archinnov.achilles.entity.metadata.MultiKeyProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.type.WideMap.BoundingMode;
import info.archinnov.achilles.entity.type.WideMap.OrderingMode;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.helper.CompositeHelper;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import parser.entity.CorrectMultiKey;

/**
 * CompositeHelperTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class CompositeHelperTest
{

	@InjectMocks
	private CompositeHelper helper;

	@Rule
	public ExpectedException expectedEx = ExpectedException.none();

	@Mock
	private List<Method> componentGetters;

	@Mock
	private AchillesEntityIntrospector achillesEntityIntrospector;

	@Mock
	private PropertyMeta<Integer, String> wideMapMeta;

	@Mock
	private PropertyMeta<CorrectMultiKey, String> multiKeyWideMapMeta;

	@Mock
	MultiKeyProperties multiKeyProperties;

	@Before
	public void setUp()
	{
		when(wideMapMeta.isSingleKey()).thenReturn(true);
		when(multiKeyWideMapMeta.isSingleKey()).thenReturn(false);
	}

	@Test
	public void should_validate_no_hole() throws Exception
	{
		List<Object> keyValues = Arrays.asList((Object) "a", "b", null, null);

		int lastNotNullIndex = helper.findLastNonNullIndexForComponents("sdfsdf", keyValues);

		assertThat(lastNotNullIndex).isEqualTo(1);

	}

	@Test(expected = IllegalArgumentException.class)
	public void should_exception_when_hole() throws Exception
	{
		List<Object> keyValues = Arrays.asList((Object) "a", null, "b");

		helper.findLastNonNullIndexForComponents("sdfsdf", keyValues);
	}

	@Test(expected = IllegalArgumentException.class)
	public void should_exception_when_starting_with_hole() throws Exception
	{
		List<Object> keyValues = Arrays.asList((Object) null, "a", "b");

		helper.findLastNonNullIndexForComponents("sdfsdf", keyValues);
	}

	@Test
	public void should_validate_bounds() throws Exception
	{
		helper.checkBounds(wideMapMeta, 12, 15, OrderingMode.ASCENDING);
	}

	@Test
	public void should_validate_asc_bounds_with_start_null() throws Exception
	{
		helper.checkBounds(wideMapMeta, null, 15, OrderingMode.ASCENDING);
	}

	@Test
	public void should_validate_asc_bounds_with_end_null() throws Exception
	{
		helper.checkBounds(wideMapMeta, 12, null, OrderingMode.ASCENDING);
	}

	@Test
	public void should_exception_when_asc_start_greater_than_end() throws Exception
	{

		expectedEx.expect(AchillesException.class);
		expectedEx.expectMessage("For range query, start value should be lesser or equal to end");

		helper.checkBounds(wideMapMeta, 15, 12, OrderingMode.ASCENDING);
	}

	@Test
	public void should_exception_when_desc_start_lesser_than_end() throws Exception
	{

		expectedEx.expect(AchillesException.class);
		expectedEx
				.expectMessage("For reverse range query, start value should be greater or equal to end value");

		helper.checkBounds(wideMapMeta, 12, 15, OrderingMode.DESCENDING);
	}

	@Test
	public void should_validate_multi_key_bounds() throws Exception
	{
		CorrectMultiKey start = new CorrectMultiKey();
		CorrectMultiKey end = new CorrectMultiKey();

		List<Object> startComponentValues = Arrays.asList((Object) "abc", 12);
		List<Object> endComponentValues = Arrays.asList((Object) "abc", 20);

		when(multiKeyWideMapMeta.getMultiKeyProperties()).thenReturn(multiKeyProperties);
		when(multiKeyProperties.getComponentGetters()).thenReturn(componentGetters);
		when(multiKeyWideMapMeta.getPropertyName()).thenReturn("any_property");
		when(achillesEntityIntrospector.determineMultiKeyValues(start, componentGetters)).thenReturn(
				startComponentValues);
		when(achillesEntityIntrospector.determineMultiKeyValues(end, componentGetters)).thenReturn(endComponentValues);

		helper.checkBounds(multiKeyWideMapMeta, start, end, OrderingMode.ASCENDING);
	}

	@Test
	public void should_validate_multi_key_asc_bounds_with_nulls() throws Exception
	{
		CorrectMultiKey start = new CorrectMultiKey();
		CorrectMultiKey end = new CorrectMultiKey();

		List<Object> startComponentValues = Arrays.asList((Object) "abc", null);
		List<Object> endComponentValues = Arrays.asList((Object) "abd", null);

		when(multiKeyWideMapMeta.getMultiKeyProperties()).thenReturn(multiKeyProperties);
		when(multiKeyProperties.getComponentGetters()).thenReturn(componentGetters);
		when(multiKeyWideMapMeta.getPropertyName()).thenReturn("any_property");
		when(achillesEntityIntrospector.determineMultiKeyValues(start, componentGetters)).thenReturn(
				startComponentValues);
		when(achillesEntityIntrospector.determineMultiKeyValues(end, componentGetters)).thenReturn(endComponentValues);

		helper.checkBounds(multiKeyWideMapMeta, start, end, OrderingMode.ASCENDING);
	}

	@Test
	public void should_exception_when_multi_key_asc_start_greater_than_end() throws Exception
	{
		CorrectMultiKey start = new CorrectMultiKey();
		CorrectMultiKey end = new CorrectMultiKey();

		List<Object> startComponentValues = Arrays.asList((Object) "abc", 12);
		List<Object> endComponentValues = Arrays.asList((Object) "abc", 10);

		when(multiKeyWideMapMeta.getMultiKeyProperties()).thenReturn(multiKeyProperties);
		when(multiKeyProperties.getComponentGetters()).thenReturn(componentGetters);
		when(multiKeyWideMapMeta.getPropertyName()).thenReturn("any_property");
		when(achillesEntityIntrospector.determineMultiKeyValues(start, componentGetters)).thenReturn(
				startComponentValues);
		when(achillesEntityIntrospector.determineMultiKeyValues(end, componentGetters)).thenReturn(endComponentValues);

		expectedEx.expect(AchillesException.class);
		expectedEx
				.expectMessage("For multiKey ascending range query, startKey value should be lesser or equal to end endKey");

		helper.checkBounds(multiKeyWideMapMeta, start, end, OrderingMode.ASCENDING);
	}

	@Test
	public void should_exception_when_multi_key_asc_hole_in_start() throws Exception
	{
		CorrectMultiKey start = new CorrectMultiKey();
		CorrectMultiKey end = new CorrectMultiKey();

		List<Object> startComponentValues = Arrays.asList((Object) null, 10);
		List<Object> endComponentValues = Arrays.asList((Object) "abc", 10);

		when(multiKeyWideMapMeta.getMultiKeyProperties()).thenReturn(multiKeyProperties);
		when(multiKeyProperties.getComponentGetters()).thenReturn(componentGetters);
		when(multiKeyWideMapMeta.getPropertyName()).thenReturn("any_property");
		when(achillesEntityIntrospector.determineMultiKeyValues(start, componentGetters)).thenReturn(
				startComponentValues);
		when(achillesEntityIntrospector.determineMultiKeyValues(end, componentGetters)).thenReturn(endComponentValues);
		expectedEx.expect(IllegalArgumentException.class);
		expectedEx
				.expectMessage("There should not be any null value between two non-null keys of WideMap 'any_property'");

		helper.checkBounds(multiKeyWideMapMeta, start, end, OrderingMode.ASCENDING);
	}

	@Test
	public void should_exception_when_multi_key_desc_start_lesser_than_end() throws Exception
	{
		CorrectMultiKey start = new CorrectMultiKey();
		CorrectMultiKey end = new CorrectMultiKey();

		List<Object> startComponentValues = Arrays.asList((Object) "abc", 12);
		List<Object> endComponentValues = Arrays.asList((Object) "def", 10);

		when(multiKeyWideMapMeta.getMultiKeyProperties()).thenReturn(multiKeyProperties);
		when(multiKeyProperties.getComponentGetters()).thenReturn(componentGetters);
		when(multiKeyWideMapMeta.getPropertyName()).thenReturn("any_property");
		when(achillesEntityIntrospector.determineMultiKeyValues(start, componentGetters)).thenReturn(
				startComponentValues);
		when(achillesEntityIntrospector.determineMultiKeyValues(end, componentGetters)).thenReturn(endComponentValues);

		expectedEx.expect(AchillesException.class);
		expectedEx
				.expectMessage("For multiKey descending range query, startKey value should be greater or equal to end endKey");

		helper.checkBounds(multiKeyWideMapMeta, start, end, OrderingMode.DESCENDING);
	}

	// Ascending order
	@Test
	public void should_return_determine_equalities_for_inclusive_start_and_end_asc()
			throws Exception
	{
		ComponentEquality[] equality = helper.determineEquality(BoundingMode.INCLUSIVE_BOUNDS, OrderingMode.ASCENDING);
		assertThat(equality[0]).isEqualTo(EQUAL);
		assertThat(equality[1]).isEqualTo(GREATER_THAN_EQUAL);
	}

	@Test
	public void should_return_determine_equalities_for_exclusive_start_and_end_asc()
			throws Exception
	{
		ComponentEquality[] equality = helper.determineEquality(BoundingMode.EXCLUSIVE_BOUNDS, OrderingMode.ASCENDING);
		assertThat(equality[0]).isEqualTo(GREATER_THAN_EQUAL);
		assertThat(equality[1]).isEqualTo(LESS_THAN_EQUAL);
	}

	@Test
	public void should_return_determine_equalities_for_inclusive_start_exclusive_end_asc()
			throws Exception
	{
		ComponentEquality[] equality = helper.determineEquality(BoundingMode.INCLUSIVE_START_BOUND_ONLY, OrderingMode.ASCENDING);
		assertThat(equality[0]).isEqualTo(EQUAL);
		assertThat(equality[1]).isEqualTo(LESS_THAN_EQUAL);
	}

	@Test
	public void should_return_determine_equalities_for_exclusive_start_inclusive_end_asc()
			throws Exception
	{
		ComponentEquality[] equality = helper.determineEquality(BoundingMode.INCLUSIVE_END_BOUND_ONLY, OrderingMode.ASCENDING);
		assertThat(equality[0]).isEqualTo(GREATER_THAN_EQUAL);
		assertThat(equality[1]).isEqualTo(GREATER_THAN_EQUAL);
	}

	// Descending order
	@Test
	public void should_return_determine_equalities_for_inclusive_start_and_end_desc()
			throws Exception
	{
		ComponentEquality[] equality = helper.determineEquality(BoundingMode.INCLUSIVE_BOUNDS, OrderingMode.DESCENDING);
		assertThat(equality[0]).isEqualTo(GREATER_THAN_EQUAL);
		assertThat(equality[1]).isEqualTo(EQUAL);
	}

	@Test
	public void should_return_determine_equalities_for_exclusive_start_and_end_desc()
			throws Exception
	{
		ComponentEquality[] equality = helper.determineEquality(BoundingMode.EXCLUSIVE_BOUNDS, OrderingMode.DESCENDING);
		assertThat(equality[0]).isEqualTo(LESS_THAN_EQUAL);
		assertThat(equality[1]).isEqualTo(GREATER_THAN_EQUAL);
	}

	@Test
	public void should_return_determine_equalities_for_inclusive_start_exclusive_end_desc()
			throws Exception
	{
		ComponentEquality[] equality = helper.determineEquality(BoundingMode.INCLUSIVE_START_BOUND_ONLY, OrderingMode.DESCENDING);
		assertThat(equality[0]).isEqualTo(GREATER_THAN_EQUAL);
		assertThat(equality[1]).isEqualTo(GREATER_THAN_EQUAL);
	}

	@Test
	public void should_return_determine_equalities_for_exclusive_start_inclusive_end_desc()
			throws Exception
	{
		ComponentEquality[] equality = helper.determineEquality(BoundingMode.INCLUSIVE_END_BOUND_ONLY, OrderingMode.DESCENDING);
		assertThat(equality[0]).isEqualTo(LESS_THAN_EQUAL);
		assertThat(equality[1]).isEqualTo(EQUAL);
	}
}
