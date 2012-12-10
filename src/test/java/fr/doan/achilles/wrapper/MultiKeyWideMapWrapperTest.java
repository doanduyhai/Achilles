package fr.doan.achilles.wrapper;

import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.GREATER_THAN_EQUAL;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import me.prettyprint.hector.api.Serializer;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import parser.entity.CorrectMultiKey;
import fr.doan.achilles.entity.metadata.MultiKeyWideMapMeta;
import fr.doan.achilles.exception.ValidationException;
import fr.doan.achilles.proxy.EntityWrapperUtil;
import fr.doan.achilles.wrapper.factory.DynamicCompositeKeyFactory;

/**
 * MultiKeyWideMapWrapperTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class MultiKeyWideMapWrapperTest
{
	@Mock
	private List<Serializer<?>> componentSerializers;

	@Mock
	private List<Method> componentGetters;

	@Mock
	private EntityWrapperUtil util;

	@Mock
	private DynamicCompositeKeyFactory keyFactory;

	@Mock
	private MultiKeyWideMapMeta<CorrectMultiKey, String> wideMapMeta;

	@InjectMocks
	private MultiKeyWideMapWrapper<Long, CorrectMultiKey, String> wrapper;

	@Rule
	public ExpectedException expectedEx = ExpectedException.none();

	@Test
	public void should_build_composite() throws Exception
	{
		CorrectMultiKey multiKey = new CorrectMultiKey();

		List<Object> componentValues = Arrays.asList((Object) "name", 12);
		when(util.determineMultiKey(multiKey, componentGetters)).thenReturn(componentValues);

		wrapper.buildComposite(multiKey);

		verify(keyFactory).buildForProperty(wideMapMeta.getPropertyName(),
				wideMapMeta.propertyType(), componentValues, componentSerializers);
	}

	@Test
	public void should_build_query_composite_start() throws Exception
	{

		CorrectMultiKey multiKey = new CorrectMultiKey();

		List<Object> componentValues = Arrays.asList((Object) "name", 12);
		when(util.determineMultiKey(multiKey, componentGetters)).thenReturn(componentValues);

		wrapper.buildQueryComposite(multiKey, GREATER_THAN_EQUAL);

		verify(keyFactory).buildQueryComparator(wideMapMeta.getPropertyName(),
				wideMapMeta.propertyType(), componentValues, componentSerializers,
				GREATER_THAN_EQUAL);
	}

	@Test
	public void should_validate_bounds() throws Exception
	{
		CorrectMultiKey start = new CorrectMultiKey();
		CorrectMultiKey end = new CorrectMultiKey();

		List<Object> startComponentValues = Arrays.asList((Object) "abc", 12);
		List<Object> endComponentValues = Arrays.asList((Object) "abc", 20);
		when(util.determineMultiKey(start, componentGetters)).thenReturn(startComponentValues);
		when(util.determineMultiKey(end, componentGetters)).thenReturn(endComponentValues);

		wrapper.validateBounds(start, end, false);
	}

	@Test
	public void should_validate_asc_bounds_with_nulls() throws Exception
	{
		CorrectMultiKey start = new CorrectMultiKey();
		CorrectMultiKey end = new CorrectMultiKey();

		List<Object> startComponentValues = Arrays.asList((Object) "abc", null);
		List<Object> endComponentValues = Arrays.asList((Object) "abd", null);
		when(util.determineMultiKey(start, componentGetters)).thenReturn(startComponentValues);
		when(util.determineMultiKey(end, componentGetters)).thenReturn(endComponentValues);

		wrapper.validateBounds(start, end, false);
	}

	@Test
	public void should_exception_when_asc_start_greater_than_end() throws Exception
	{
		CorrectMultiKey start = new CorrectMultiKey();
		CorrectMultiKey end = new CorrectMultiKey();

		List<Object> startComponentValues = Arrays.asList((Object) "abc", 12);
		List<Object> endComponentValues = Arrays.asList((Object) "abc", 10);
		when(util.determineMultiKey(start, componentGetters)).thenReturn(startComponentValues);
		when(util.determineMultiKey(end, componentGetters)).thenReturn(endComponentValues);

		expectedEx.expect(ValidationException.class);
		expectedEx
				.expectMessage("For multiKey ascending range query, startKey value should be lesser or equal to end endKey");

		wrapper.validateBounds(start, end, false);
	}

	@Test
	public void should_exception_when_asc_hole_in_start() throws Exception
	{
		CorrectMultiKey start = new CorrectMultiKey();
		CorrectMultiKey end = new CorrectMultiKey();

		List<Object> startComponentValues = Arrays.asList((Object) null, 10);
		List<Object> endComponentValues = Arrays.asList((Object) "abc", 10);
		when(util.determineMultiKey(start, componentGetters)).thenReturn(startComponentValues);
		when(util.determineMultiKey(end, componentGetters)).thenReturn(endComponentValues);
		when(wideMapMeta.getPropertyName()).thenReturn("name");

		when(keyFactory.validateNoHole("name", startComponentValues))
				.thenThrow(
						new ValidationException(
								"There should not be any null value between two non-null keys of WideMap 'name'"));
		expectedEx.expect(ValidationException.class);
		expectedEx
				.expectMessage("There should not be any null value between two non-null keys of WideMap 'name'");

		wrapper.validateBounds(start, end, false);
	}

	@Test
	public void should_exception_when_desc_start_lesser_than_end() throws Exception
	{
		CorrectMultiKey start = new CorrectMultiKey();
		CorrectMultiKey end = new CorrectMultiKey();

		List<Object> startComponentValues = Arrays.asList((Object) "abc", 12);
		List<Object> endComponentValues = Arrays.asList((Object) "def", 10);
		when(util.determineMultiKey(start, componentGetters)).thenReturn(startComponentValues);
		when(util.determineMultiKey(end, componentGetters)).thenReturn(endComponentValues);

		expectedEx.expect(ValidationException.class);
		expectedEx
				.expectMessage("For multiKey descending range query, startKey value should be greater or equal to end endKey");

		wrapper.validateBounds(start, end, true);
	}
}
