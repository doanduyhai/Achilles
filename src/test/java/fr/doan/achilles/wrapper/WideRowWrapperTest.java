package fr.doan.achilles.wrapper;

import static fr.doan.achilles.serializer.Utils.INT_SRZ;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.HColumn;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import fr.doan.achilles.dao.GenericWideRowDao;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.helper.CompositeHelper;
import fr.doan.achilles.holder.KeyValue;
import fr.doan.achilles.holder.factory.KeyValueFactory;
import fr.doan.achilles.iterator.factory.IteratorFactory;

/**
 * WideRowWrapperTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class WideRowWrapperTest
{
	@InjectMocks
	private WideRowWrapper<Long, Integer, String> wrapper;

	@Mock
	private GenericWideRowDao<Long, Integer> dao;

	@Mock
	private PropertyMeta<Integer, String> wideMapMeta;

	@Mock
	private CompositeHelper helper;

	@Mock
	private KeyValueFactory keyValueFactory;

	@Mock
	private IteratorFactory iteratorFactory;

	private Long id;

	@SuppressWarnings(
	{
			"rawtypes",
			"unchecked"
	})
	@Before
	public void setUp()
	{
		ReflectionTestUtils.setField(wrapper, "helper", helper);
		ReflectionTestUtils.setField(wrapper, "keyValueFactory", keyValueFactory);
		ReflectionTestUtils.setField(wrapper, "iteratorFactory", iteratorFactory);
		ReflectionTestUtils.setField(wrapper, "id", id);

		when(wideMapMeta.getKeySerializer()).thenReturn((Serializer) INT_SRZ);
	}

	@Test
	public void should_get_value() throws Exception
	{
		when(dao.getValue(id, 12)).thenReturn("test");
		when(wideMapMeta.getValue("test")).thenReturn("test");

		Object expected = wrapper.get(12);

		assertThat(expected).isEqualTo("test");
	}

	@Test
	public void should_insert_value() throws Exception
	{
		wrapper.insert(12, "test");
		verify(dao).setValue(id, 12, "test");
	}

	@Test
	public void should_insert_value_with_ttl() throws Exception
	{
		wrapper.insert(12, "test", 452);
		verify(dao).setValue(id, 12, "test", 452);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_find_range() throws Exception
	{
		List<HColumn<Integer, Object>> hColumns = mock(List.class);
		List<KeyValue<Integer, String>> keyValues = mock(List.class);

		when(dao.findRawColumnsRange(id, 12, 15, false, 10)).thenReturn(hColumns);
		when(keyValueFactory.createFromColumnList(hColumns, INT_SRZ, wideMapMeta)).thenReturn(
				keyValues).thenReturn(keyValues);

		List<KeyValue<Integer, String>> expected = wrapper.findRange(12, 15, false, 10);

		assertThat(expected).isSameAs(keyValues);
	}
}
