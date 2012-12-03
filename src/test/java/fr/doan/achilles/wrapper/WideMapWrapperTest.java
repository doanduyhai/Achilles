package fr.doan.achilles.wrapper;

import static fr.doan.achilles.entity.metadata.PropertyType.WIDE_MAP;
import static fr.doan.achilles.serializer.Utils.INT_SRZ;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.EQUAL;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.GREATER_THAN_EQUAL;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import me.prettyprint.hector.api.beans.DynamicComposite;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import fr.doan.achilles.dao.GenericDao;
import fr.doan.achilles.entity.metadata.WideMapMeta;
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

	@Before
	public void setUp()
	{
		wrapper.setId(1L);
	}

	@Test
	public void should_get_value() throws Exception
	{
		when(propertyMeta.getPropertyName()).thenReturn("name");
		when(propertyMeta.getKeySerializer()).thenReturn(INT_SRZ);
		DynamicComposite composite = new DynamicComposite();
		when(keyFactory.buildForProperty("name", WIDE_MAP, 1, Utils.INT_SRZ)).thenReturn(composite);

		wrapper.getValue(1);

		verify(dao).getValue(1L, composite);

	}

	@Test
	public void should_insert_value() throws Exception
	{
		when(propertyMeta.getPropertyName()).thenReturn("name");
		when(propertyMeta.getKeySerializer()).thenReturn(INT_SRZ);
		DynamicComposite composite = new DynamicComposite();
		when(keyFactory.buildForProperty("name", WIDE_MAP, 1, INT_SRZ)).thenReturn(composite);

		wrapper.insertValue(1, "test");

		verify(dao).setValue(1L, composite, "test");

	}

	@Test
	public void should_insert_value_with_ttl() throws Exception
	{
		when(propertyMeta.getPropertyName()).thenReturn("name");
		when(propertyMeta.getKeySerializer()).thenReturn(INT_SRZ);
		DynamicComposite composite = new DynamicComposite();
		when(keyFactory.buildForProperty("name", WIDE_MAP, 1, INT_SRZ)).thenReturn(composite);

		wrapper.insertValue(1, "test", 12);

		verify(dao).setValue(1L, composite, "test", 12);

	}

	@Test
	public void should_find_values_asc_bounds_inclusive() throws Exception
	{
		when(propertyMeta.getPropertyName()).thenReturn("name");
		when(propertyMeta.getKeySerializer()).thenReturn(INT_SRZ);

		DynamicComposite start = new DynamicComposite();
		when(keyFactory.buildQueryComparator("name", WIDE_MAP, EQUAL)).thenReturn(start);

		DynamicComposite end = new DynamicComposite();
		when(keyFactory.buildQueryComparator("name", WIDE_MAP, GREATER_THAN_EQUAL)).thenReturn(end);

	}
}
