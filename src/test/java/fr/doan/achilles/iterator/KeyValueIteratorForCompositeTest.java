package fr.doan.achilles.iterator;

import static fr.doan.achilles.serializer.SerializerUtils.COMPOSITE_SRZ;
import static fr.doan.achilles.serializer.SerializerUtils.INT_SRZ;
import static fr.doan.achilles.serializer.SerializerUtils.STRING_SRZ;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.List;
import java.util.NoSuchElementException;

import me.prettyprint.cassandra.model.HColumnImpl;
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
import fr.doan.achilles.entity.metadata.MultiKeyProperties;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.holder.KeyValue;
import fr.doan.achilles.holder.factory.KeyValueFactory;

/**
 * KeyValueIteratorForWideRowTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class KeyValueIteratorForCompositeTest
{

	@InjectMocks
	private KeyValueIteratorForComposite<CorrectMultiKey, String> iterator;

	@Mock
	private AchillesSliceIterator<CorrectMultiKey, Composite, String> achillesSliceIterator;

	@Mock
	private List<Method> componentSetters;

	@Mock
	private PropertyMeta<CorrectMultiKey, String> multiKeyWideMapMeta;

	@Mock
	private MultiKeyProperties multiKeyProperties;

	@Mock
	private KeyValueFactory factory;

	@Before
	public void setUp()
	{
		ReflectionTestUtils.setField(iterator, "factory", factory);
		when(multiKeyWideMapMeta.getKeyClass()).thenReturn(CorrectMultiKey.class);
		when(multiKeyWideMapMeta.getMultiKeyProperties()).thenReturn(multiKeyProperties);
	}

	@Test
	public void should_has_next() throws Exception
	{
		when(achillesSliceIterator.hasNext()).thenReturn(true, true, false);

		assertThat(iterator.hasNext()).isTrue();
		assertThat(iterator.hasNext()).isTrue();
		assertThat(iterator.hasNext()).isFalse();

	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_give_next_element() throws Exception
	{
		KeyValue<CorrectMultiKey, String> keyValue = mock(KeyValue.class);
		HColumn<Composite, String> hColumn = new HColumnImpl<Composite, String>(COMPOSITE_SRZ,
				STRING_SRZ);
		Composite comp = new Composite();
		comp.setComponent(0, 12, INT_SRZ);
		comp.setComponent(1, "name", STRING_SRZ);
		hColumn.setName(comp);
		hColumn.setValue("test");
		hColumn.setTtl(1);

		when(achillesSliceIterator.hasNext()).thenReturn(true, false);
		when(achillesSliceIterator.next()).thenReturn(hColumn);
		when(multiKeyWideMapMeta.getKeyClass()).thenReturn(CorrectMultiKey.class);
		when(multiKeyProperties.getComponentSetters()).thenReturn(componentSetters);

		when(factory.createKeyValueForComposite(multiKeyWideMapMeta, hColumn)).thenReturn(keyValue);

		KeyValue<CorrectMultiKey, String> result = iterator.next();

		assertThat(result).isSameAs(keyValue);
	}

	@Test(expected = NoSuchElementException.class)
	public void should_exception_when_no_more_element() throws Exception
	{
		when(achillesSliceIterator.hasNext()).thenReturn(false);
		iterator.next();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void should_exception_when_remove_called() throws Exception
	{
		iterator.remove();
	}
}
