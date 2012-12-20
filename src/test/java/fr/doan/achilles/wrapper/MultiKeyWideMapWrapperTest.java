package fr.doan.achilles.wrapper;

import static fr.doan.achilles.entity.metadata.PropertyType.WIDE_MAP;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.EQUAL;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.GREATER_THAN_EQUAL;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import parser.entity.CorrectMultiKey;
import fr.doan.achilles.dao.GenericEntityDao;
import fr.doan.achilles.entity.metadata.MultiKeyWideMapMeta;
import fr.doan.achilles.entity.type.KeyValue;
import fr.doan.achilles.helper.CompositeHelper;
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
	@InjectMocks
	private MultiKeyWideMapWrapper<Long, CorrectMultiKey, String> wrapper;

	@Mock
	private List<Serializer<?>> componentSerializers;

	@Mock
	private GenericEntityDao<Long> dao;

	@Mock
	private List<Method> componentGetters;

	@Mock
	private List<Method> componentSetters;

	@Mock
	private EntityWrapperUtil util;

	@Mock
	private DynamicCompositeKeyFactory keyFactory;

	@Mock
	private CompositeHelper helper;

	@Mock
	private MultiKeyWideMapMeta<CorrectMultiKey, String> wideMapMeta;

	@Rule
	public ExpectedException expectedEx = ExpectedException.none();

	@Test
	public void should_build_composite() throws Exception
	{
		CorrectMultiKey multiKey = new CorrectMultiKey();

		List<Object> componentValues = Arrays.asList((Object) "name", 12);
		when(util.determineMultiKey(multiKey, componentGetters)).thenReturn(componentValues);

		wrapper.buildComposite(multiKey);

		verify(keyFactory).buildForInsert(wideMapMeta.getPropertyName(),
				wideMapMeta.propertyType(), componentValues, componentSerializers);
	}

	@Test
	public void should_build_query_composite() throws Exception
	{

		CorrectMultiKey multiKey = new CorrectMultiKey();

		List<Object> componentValues = Arrays.asList((Object) "name", 12);
		when(util.determineMultiKey(multiKey, componentGetters)).thenReturn(componentValues);

		wrapper.buildQueryComposite(multiKey, GREATER_THAN_EQUAL);

		verify(keyFactory).buildQueryComparator(wideMapMeta.getPropertyName(),
				wideMapMeta.propertyType(), componentValues, componentSerializers,

				GREATER_THAN_EQUAL);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_find_values() throws Exception
	{
		CorrectMultiKey start = new CorrectMultiKey();
		CorrectMultiKey end = new CorrectMultiKey();
		DynamicComposite startComp = new DynamicComposite();
		DynamicComposite endComp = new DynamicComposite();
		List<Object> startCompValues = mock(List.class);
		List<Object> endCompValues = mock(List.class);
		HColumn<DynamicComposite, Object> hColumn = mock(HColumn.class);
		List<HColumn<DynamicComposite, Object>> hColumns = Arrays.asList(hColumn);
		KeyValue<CorrectMultiKey, String> keyValue = new KeyValue<CorrectMultiKey, String>(start,
				"value");
		List<KeyValue<CorrectMultiKey, String>> keyValues = Arrays.asList(keyValue);

		when(helper.determineEquality(true, true, false)).thenReturn(new ComponentEquality[]
		{
				EQUAL,
				GREATER_THAN_EQUAL
		});
		when(wideMapMeta.getPropertyName()).thenReturn("prop");
		when(wideMapMeta.propertyType()).thenReturn(WIDE_MAP);
		when(util.determineMultiKey(start, componentGetters)).thenReturn(startCompValues);
		when(util.determineMultiKey(end, componentGetters)).thenReturn(endCompValues);
		when(
				keyFactory.buildQueryComparator("prop", WIDE_MAP, startCompValues,
						componentSerializers, EQUAL)).thenReturn(startComp);
		when(
				keyFactory.buildQueryComparator("prop", WIDE_MAP, endCompValues,
						componentSerializers, GREATER_THAN_EQUAL)).thenReturn(endComp);

		when(dao.findRawColumnsRange(10L, startComp, endComp, false, 10)).thenReturn(hColumns);
		when(wideMapMeta.getKeyClass()).thenReturn(CorrectMultiKey.class);
		when(
				util.buildMultiKeyListForDynamicComposite(CorrectMultiKey.class, wideMapMeta,
						hColumns, componentSetters)).thenReturn(keyValues);

		List<KeyValue<CorrectMultiKey, String>> result = wrapper.findValues(start, end, false, 10);

		assertThat(result).hasSize(1);
		assertThat(result.get(0)).isSameAs(keyValue);
	}

	@Before
	public void setUp()
	{
		ReflectionTestUtils.setField(wrapper, "id", 10L);
	}
}
