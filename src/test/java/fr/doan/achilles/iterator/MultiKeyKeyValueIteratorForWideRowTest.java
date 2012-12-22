package fr.doan.achilles.iterator;

import static fr.doan.achilles.serializer.Utils.COMPOSITE_SRZ;
import static fr.doan.achilles.serializer.Utils.INT_SRZ;
import static fr.doan.achilles.serializer.Utils.OBJECT_SRZ;
import static fr.doan.achilles.serializer.Utils.STRING_SRZ;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.List;
import java.util.NoSuchElementException;

import me.prettyprint.cassandra.model.HColumnImpl;
import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import parser.entity.CorrectMultiKey;
import fr.doan.achilles.entity.metadata.MultiKeyWideMapMeta;
import fr.doan.achilles.holder.KeyValue;
import fr.doan.achilles.proxy.EntityWrapperUtil;

/**
 * CompositeKeyValueIteratorTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class MultiKeyKeyValueIteratorForWideRowTest
{

	@InjectMocks
	private MultiKeyKeyValueIteratorForWideRow<CorrectMultiKey, String> iterator;

	@Mock
	private ColumnSliceIterator<CorrectMultiKey, Composite, Object> columnSliceIterator;

	@Mock
	private List<Method> componentSetters;

	@Mock
	private MultiKeyWideMapMeta<CorrectMultiKey, String> multiKeyWideMapMeta;

	@Mock
	private EntityWrapperUtil util = new EntityWrapperUtil();

	@Before
	public void setUp()
	{
		ReflectionTestUtils.setField(iterator, "util", util);
		when(multiKeyWideMapMeta.getKeyClass()).thenReturn(CorrectMultiKey.class);
	}

	@Test
	public void should_has_next() throws Exception
	{
		when(columnSliceIterator.hasNext()).thenReturn(true, true, false);

		assertThat(iterator.hasNext()).isTrue();
		assertThat(iterator.hasNext()).isTrue();
		assertThat(iterator.hasNext()).isFalse();

	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_give_next_element() throws Exception
	{
		KeyValue<CorrectMultiKey, String> keyValue = mock(KeyValue.class);
		HColumn<Composite, Object> hColumn = new HColumnImpl<Composite, Object>(COMPOSITE_SRZ,
				OBJECT_SRZ);
		Composite comp = new Composite();
		comp.setComponent(0, 12, INT_SRZ);
		comp.setComponent(1, "name", STRING_SRZ);
		hColumn.setName(comp);
		hColumn.setValue("test");
		hColumn.setTtl(1);

		when(columnSliceIterator.hasNext()).thenReturn(true, false);
		when(columnSliceIterator.next()).thenReturn(hColumn);

		when(
				util.buildMultiKeyForComposite(CorrectMultiKey.class, multiKeyWideMapMeta, hColumn,
						componentSetters)).thenReturn(keyValue);

		KeyValue<CorrectMultiKey, String> result = iterator.next();

		assertThat(result).isSameAs(keyValue);
	}

	@Test(expected = NoSuchElementException.class)
	public void should_exception_when_no_more_element() throws Exception
	{
		when(columnSliceIterator.hasNext()).thenReturn(false);
		iterator.next();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void should_exception_when_remove_called() throws Exception
	{
		iterator.remove();
	}
}
