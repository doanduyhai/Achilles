package fr.doan.achilles.wrapper;

import static fr.doan.achilles.entity.metadata.PropertyType.WIDE_MAP;
import static fr.doan.achilles.serializer.Utils.DYNA_COMP_SRZ;
import static fr.doan.achilles.serializer.Utils.INT_SRZ;
import static fr.doan.achilles.serializer.Utils.OBJECT_SRZ;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.EQUAL;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.GREATER_THAN_EQUAL;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.LESS_THAN_EQUAL;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import me.prettyprint.cassandra.model.HColumnImpl;
import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import fr.doan.achilles.dao.GenericDao;
import fr.doan.achilles.entity.metadata.WideMapMeta;
import fr.doan.achilles.entity.type.KeyValue;
import fr.doan.achilles.entity.type.KeyValueIterator;
import fr.doan.achilles.exception.ValidationException;
import fr.doan.achilles.serializer.Utils;
import fr.doan.achilles.wrapper.factory.DynamicCompositeKeyFactory;

/**
 * InternalWideMapWrapperTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class WideMapWrapperTest
{

	@InjectMocks
	private WideMapWrapper<Long, Integer, String> wrapper;

	@Mock
	private GenericDao<Long> dao;

	@Mock
	private WideMapMeta<Integer, String> propertyMeta;

	@Mock
	private DynamicCompositeKeyFactory keyFactory;

	@Mock
	private ColumnSliceIterator<Long, DynamicComposite, Object> iterator;

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Before
	public void setUp()
	{
		wrapper.setId(1L);

		when(propertyMeta.getPropertyName()).thenReturn("name");
		when(propertyMeta.propertyType()).thenReturn(WIDE_MAP);
		when(propertyMeta.getKeySerializer()).thenReturn((Serializer) INT_SRZ);
	}

	@Test
	public void should_get_value() throws Exception
	{
		DynamicComposite composite = new DynamicComposite();
		when(keyFactory.buildForProperty("name", WIDE_MAP, 1, Utils.INT_SRZ)).thenReturn(composite);

		wrapper.getValue(1);

		verify(dao).getValue(1L, composite);

	}

	@Test
	public void should_insert_value() throws Exception
	{
		DynamicComposite composite = new DynamicComposite();
		when(keyFactory.buildForProperty("name", WIDE_MAP, 1, INT_SRZ)).thenReturn(composite);

		wrapper.insertValue(1, "test");

		verify(dao).setValue(1L, composite, "test");

	}

	@Test
	public void should_insert_value_with_ttl() throws Exception
	{
		DynamicComposite composite = new DynamicComposite();
		when(keyFactory.buildForProperty("name", WIDE_MAP, 1, INT_SRZ)).thenReturn(composite);

		wrapper.insertValue(1, "test", 12);

		verify(dao).setValue(1L, composite, "test", 12);

	}

	@Test
	public void should_find_values_asc_bounds_default() throws Exception
	{
		DynamicComposite start = new DynamicComposite();
		when(keyFactory.buildQueryComparator("name", WIDE_MAP, (Object) 1, EQUAL))
				.thenReturn(start);

		DynamicComposite end = new DynamicComposite();
		when(keyFactory.buildQueryComparator("name", WIDE_MAP, (Object) 2, GREATER_THAN_EQUAL))
				.thenReturn(end);

		wrapper.findValues(1, 2, false, 10);
		verify(dao).findRawColumnsRange(1L, start, end, false, 10);
	}

	@Test
	public void should_find_values_asc_bounds_inclusive() throws Exception
	{
		DynamicComposite start = new DynamicComposite();
		when(keyFactory.buildQueryComparator("name", WIDE_MAP, (Object) 1, EQUAL))
				.thenReturn(start);

		DynamicComposite end = new DynamicComposite();
		when(keyFactory.buildQueryComparator("name", WIDE_MAP, (Object) 2, GREATER_THAN_EQUAL))
				.thenReturn(end);

		wrapper.findValues(1, 2, true, false, 10);

		verify(dao).findRawColumnsRange(1L, start, end, false, 10);
	}

	@Test
	public void should_find_values_asc_bounds_exclusive() throws Exception
	{
		DynamicComposite start = new DynamicComposite();
		when(keyFactory.buildQueryComparator("name", WIDE_MAP, (Object) 1, GREATER_THAN_EQUAL))
				.thenReturn(start);

		DynamicComposite end = new DynamicComposite();
		when(keyFactory.buildQueryComparator("name", WIDE_MAP, (Object) 2, LESS_THAN_EQUAL))
				.thenReturn(end);

		wrapper.findValues(1, 2, false, false, 10);

		verify(dao).findRawColumnsRange(1L, start, end, false, 10);
	}

	@Test
	public void should_find_values_asc_inclusive_start_exclusive_end() throws Exception
	{
		DynamicComposite start = new DynamicComposite();
		when(keyFactory.buildQueryComparator("name", WIDE_MAP, (Object) 1, EQUAL))
				.thenReturn(start);

		DynamicComposite end = new DynamicComposite();
		when(keyFactory.buildQueryComparator("name", WIDE_MAP, (Object) 2, LESS_THAN_EQUAL))
				.thenReturn(end);

		wrapper.findValues(1, true, 2, false, false, 10);

		verify(dao).findRawColumnsRange(1L, start, end, false, 10);
	}

	@Test(expected = ValidationException.class)
	public void should_exception_when_find_values_asc_and_start_greater_than_end() throws Exception
	{
		wrapper.findValues(2, 1, false, 10);
	}

	@Test(expected = ValidationException.class)
	public void should_exception_when_find_values_desc_and_start_lesser_than_end() throws Exception
	{
		wrapper.findValues(1, 2, true, 10);
	}

	@SuppressWarnings(
	{
			"rawtypes",
			"unchecked"
	})
	@Test
	public void should_return_iterator_default() throws Exception
	{
		DynamicComposite start = new DynamicComposite();
		when(keyFactory.buildQueryComparator("name", WIDE_MAP, (Object) 1, EQUAL))
				.thenReturn(start);

		DynamicComposite end = new DynamicComposite();
		when(keyFactory.buildQueryComparator("name", WIDE_MAP, (Object) 2, GREATER_THAN_EQUAL))
				.thenReturn(end);

		when(propertyMeta.getKeySerializer()).thenReturn((Serializer) INT_SRZ);
		when(dao.getColumnsIterator(1L, start, end, false, 10)).thenReturn(iterator);
		when(iterator.hasNext()).thenReturn(true);

		HColumn<DynamicComposite, Object> hColumn = new HColumnImpl<DynamicComposite, Object>(
				DYNA_COMP_SRZ, OBJECT_SRZ);
		DynamicComposite dynComp = new DynamicComposite();
		dynComp.setComponent(0, 10, INT_SRZ);
		dynComp.setComponent(1, 10, INT_SRZ);
		dynComp.setComponent(2, 1, INT_SRZ);
		hColumn.setName(dynComp);
		hColumn.setValue("test");
		hColumn.setTtl(12);

		when(iterator.next()).thenReturn(hColumn);
		KeyValueIterator<Integer, String> keyValueIter = wrapper.iterator(1, 2, false, 10);

		assertThat(keyValueIter.hasNext()).isTrue();
		KeyValue<Integer, String> keyValue = keyValueIter.next();
		assertThat(keyValue.getKey()).isEqualTo(1);
		assertThat(keyValue.getValue()).isEqualTo("test");
		assertThat(keyValue.getTtl()).isEqualTo(12);

	}

	@SuppressWarnings(
	{
			"rawtypes",
			"unchecked"
	})
	@Test
	public void should_return_iterator_exclusive_bounds() throws Exception
	{
		DynamicComposite start = new DynamicComposite();
		when(keyFactory.buildQueryComparator("name", WIDE_MAP, (Object) 1, GREATER_THAN_EQUAL))
				.thenReturn(start);

		DynamicComposite end = new DynamicComposite();
		when(keyFactory.buildQueryComparator("name", WIDE_MAP, (Object) 3, LESS_THAN_EQUAL))
				.thenReturn(end);

		when(propertyMeta.getKeySerializer()).thenReturn((Serializer) INT_SRZ);
		when(dao.getColumnsIterator(1L, start, end, false, 10)).thenReturn(iterator);
		when(iterator.hasNext()).thenReturn(true);

		HColumn<DynamicComposite, Object> hColumn1 = new HColumnImpl<DynamicComposite, Object>(
				DYNA_COMP_SRZ, OBJECT_SRZ);
		DynamicComposite dynComp1 = new DynamicComposite();
		dynComp1.setComponent(0, 10, INT_SRZ);
		dynComp1.setComponent(1, 10, INT_SRZ);
		dynComp1.setComponent(2, 1, INT_SRZ);
		hColumn1.setName(dynComp1);
		hColumn1.setValue("test1");
		hColumn1.setTtl(11);

		HColumn<DynamicComposite, Object> hColumn2 = new HColumnImpl<DynamicComposite, Object>(
				DYNA_COMP_SRZ, OBJECT_SRZ);
		DynamicComposite dynComp2 = new DynamicComposite();
		dynComp2.setComponent(0, 10, INT_SRZ);
		dynComp2.setComponent(1, 10, INT_SRZ);
		dynComp2.setComponent(2, 2, INT_SRZ);
		hColumn2.setName(dynComp2);
		hColumn2.setValue("test2");
		hColumn2.setTtl(12);

		HColumn<DynamicComposite, Object> hColumn3 = new HColumnImpl<DynamicComposite, Object>(
				DYNA_COMP_SRZ, OBJECT_SRZ);
		DynamicComposite dynComp3 = new DynamicComposite();
		dynComp3.setComponent(0, 10, INT_SRZ);
		dynComp3.setComponent(1, 10, INT_SRZ);
		dynComp3.setComponent(2, 3, INT_SRZ);
		hColumn3.setName(dynComp3);
		hColumn3.setValue("test3");
		hColumn3.setTtl(13);

		when(iterator.next()).thenReturn(hColumn2);
		KeyValueIterator<Integer, String> keyValueIter = wrapper.iterator(1, 3, false, false, 10);

		KeyValue<Integer, String> keyValue = keyValueIter.next();
		assertThat(keyValue.getKey()).isEqualTo(2);
		assertThat(keyValue.getValue()).isEqualTo("test2");
		assertThat(keyValue.getTtl()).isEqualTo(12);

	}

	@Test
	public void should_remove() throws Exception
	{
		DynamicComposite comp = new DynamicComposite();
		when(keyFactory.buildForProperty("name", WIDE_MAP, 5, INT_SRZ)).thenReturn(comp);

		wrapper.removeValue(5);

		verify(dao).removeColumn(1L, comp);
	}

	@Test
	public void should_remove_columns() throws Exception
	{
		DynamicComposite start = new DynamicComposite();
		when(keyFactory.buildQueryComparator("name", WIDE_MAP, (Object) 5, EQUAL))
				.thenReturn(start);

		DynamicComposite end = new DynamicComposite();
		when(keyFactory.buildQueryComparator("name", WIDE_MAP, (Object) 10, GREATER_THAN_EQUAL))
				.thenReturn(end);

		wrapper.removeValues(5, 10);

		verify(dao).removeColumnRange(1L, start, end);
	}
}
