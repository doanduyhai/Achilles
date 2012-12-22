package fr.doan.achilles.wrapper;

import static fr.doan.achilles.entity.metadata.PropertyType.SIMPLE;
import static fr.doan.achilles.entity.metadata.PropertyType.WIDE_MAP;
import static fr.doan.achilles.serializer.Utils.DYNA_COMP_SRZ;
import static fr.doan.achilles.serializer.Utils.INT_SRZ;
import static fr.doan.achilles.serializer.Utils.OBJECT_SRZ;
import static fr.doan.achilles.serializer.Utils.STRING_SRZ;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.EQUAL;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.GREATER_THAN_EQUAL;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import me.prettyprint.cassandra.model.HColumnImpl;
import me.prettyprint.cassandra.service.ColumnSliceIterator;
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
import fr.doan.achilles.helper.CompositeHelper;
import fr.doan.achilles.holder.KeyValue;
import fr.doan.achilles.iterator.DynamicCompositeMultiKeyValueIterator;
import fr.doan.achilles.proxy.EntityWrapperUtil;
import fr.doan.achilles.serializer.Utils;
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

		verify(keyFactory).createForInsertMultiKey(wideMapMeta.getPropertyName(),
				wideMapMeta.propertyType(), componentValues, componentSerializers);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_find_range() throws Exception
	{
		CorrectMultiKey start = new CorrectMultiKey(), end = new CorrectMultiKey();
		DynamicComposite startComp = new DynamicComposite(), endComp = new DynamicComposite();
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
		when(keyFactory.createForMultiKeyQuery( //
				"prop", //
				WIDE_MAP, //
				componentSerializers, //
				componentGetters, //
				start, //
				true, //
				end, //
				true, //
				false)) //
				.thenReturn(new DynamicComposite[]
				{
						startComp,
						endComp
				});

		when(dao.findRawColumnsRange(10L, startComp, endComp, false, 10)).thenReturn(hColumns);
		when(
				util.buildMultiKeyListForDynamicComposite(CorrectMultiKey.class, wideMapMeta,
						hColumns, componentSetters)).thenReturn(keyValues);

		List<KeyValue<CorrectMultiKey, String>> result = wrapper.findRange(start, end, false, 10);

		assertThat(result).isSameAs(keyValues);
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes",
	})
	@Test
	public void should_return_iterator() throws Exception
	{
		CorrectMultiKey start = new CorrectMultiKey();
		CorrectMultiKey end = new CorrectMultiKey();
		DynamicComposite startComp = new DynamicComposite();
		DynamicComposite endComp = new DynamicComposite();
		DynamicComposite dynComp = new DynamicComposite();
		dynComp.setComponent(0, SIMPLE.flag(), Utils.BYTE_SRZ);
		dynComp.setComponent(1, "name", STRING_SRZ);
		dynComp.setComponent(2, "name", STRING_SRZ);
		dynComp.setComponent(3, 10, INT_SRZ);
		HColumn<DynamicComposite, Object> hColumn = new HColumnImpl<DynamicComposite, Object>(
				DYNA_COMP_SRZ, OBJECT_SRZ);
		hColumn.setName(dynComp);
		hColumn.setValue("test");
		hColumn.setTtl(12);

		ColumnSliceIterator<Long, DynamicComposite, Object> columnSliceIterator = mock(ColumnSliceIterator.class);

		when(helper.determineEquality(true, true, false)).thenReturn(new ComponentEquality[]
		{
				EQUAL,
				GREATER_THAN_EQUAL
		});

		when(keyFactory.createForMultiKeyQuery( //
				"prop", //
				WIDE_MAP, //
				componentSerializers, //
				componentGetters, //
				start, //
				true, //
				end, //
				true, //
				false)) //
				.thenReturn(new DynamicComposite[]
				{
						startComp,
						endComp
				});
		when(dao.getColumnsIterator(10L, startComp, endComp, false, 10)).thenReturn(
				columnSliceIterator);

		when(columnSliceIterator.hasNext()).thenReturn(true, false);
		when(columnSliceIterator.next()).thenReturn(hColumn);

		Method nameSetter = CorrectMultiKey.class.getDeclaredMethod("setName", String.class);
		Method rankSetter = CorrectMultiKey.class.getDeclaredMethod("setRank", int.class);

		when(componentSerializers.get(0)).thenReturn((Serializer) STRING_SRZ);
		when(componentSerializers.get(1)).thenReturn((Serializer) INT_SRZ);
		when(componentSetters.get(0)).thenReturn(nameSetter);
		when(componentSetters.get(1)).thenReturn(rankSetter);

		DynamicCompositeMultiKeyValueIterator<CorrectMultiKey, String> result = wrapper.iterator(start, end, false,
				10);

		KeyValue<CorrectMultiKey, String> actual = result.next();
		assertThat(actual.getKey().getName()).isEqualTo("name");
	}

	@Test
	public void should_remove_range() throws Exception
	{
		CorrectMultiKey start = new CorrectMultiKey(), end = new CorrectMultiKey();
		DynamicComposite startComp = new DynamicComposite(), endComp = new DynamicComposite();

		when(helper.determineEquality(true, true, false)).thenReturn(new ComponentEquality[]
		{
				EQUAL,
				GREATER_THAN_EQUAL
		});
		when(keyFactory.createForMultiKeyQuery( //
				"prop", //
				WIDE_MAP, //
				componentSerializers, //
				componentGetters, //
				start, //
				true, //
				end, //
				true, //
				false)) //
				.thenReturn(new DynamicComposite[]
				{
						startComp,
						endComp
				});

		wrapper.removeRange(start, true, end, true);

		verify(dao).removeColumnRange(10L, startComp, endComp);
	}

	@Before
	public void setUp()
	{
		ReflectionTestUtils.setField(wrapper, "id", 10L);
		when(wideMapMeta.getPropertyName()).thenReturn("prop");
		when(wideMapMeta.propertyType()).thenReturn(WIDE_MAP);
		when(wideMapMeta.getKeyClass()).thenReturn(CorrectMultiKey.class);
		when(wideMapMeta.getComponentSerializers()).thenReturn(componentSerializers);
	}
}
