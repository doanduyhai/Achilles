package fr.doan.achilles.wrapper;

import static org.fest.assertions.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import fr.doan.achilles.dao.GenericDynamicCompositeDao;
import fr.doan.achilles.entity.type.KeyValueIterator;
import fr.doan.achilles.holder.KeyValue;

/**
 * AbstractWideMapWrapperTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class AbstractWideMapWrapperTest
{

	@Mock
	private GenericDynamicCompositeDao<Long> dao;

	private AbstractWideMapWrapper<Long, String> wrapper;

	private ThreadLocal<VariableCapture> variableCapture = new ThreadLocal<VariableCapture>();

	@Before
	public void setUp()
	{
		wrapper = new AbstractWideMapWrapper<Long, String>()
		{

			@Override
			public void remove(Long start, boolean inclusiveStart, Long end, boolean inclusiveEnd)
			{
				VariableCapture capture = new VariableCapture();
				capture.start = start;
				capture.end = end;
				capture.inclusiveStart = inclusiveStart;
				capture.inclusiveEnd = inclusiveEnd;

				variableCapture.set(capture);
			}

			@Override
			public void remove(Long key)
			{
				VariableCapture capture = new VariableCapture();
				capture.start = key;

				variableCapture.set(capture);
			}

			@Override
			public void removeFirst()
			{
				VariableCapture capture = new VariableCapture();
				capture.start = null;
				capture.end = null;
				capture.reverse = false;
				capture.count = 1;

				variableCapture.set(capture);
			}

			@Override
			public void removeLast()
			{
				VariableCapture capture = new VariableCapture();
				capture.start = null;
				capture.end = null;
				capture.reverse = true;
				capture.count = 1;

				variableCapture.set(capture);
			}

			@Override
			public KeyValueIterator<Long, String> iterator(Long start, boolean inclusiveStart,
					Long end, boolean inclusiveEnd, boolean reverse, int count)
			{
				VariableCapture capture = new VariableCapture();
				capture.start = start;
				capture.end = end;
				capture.inclusiveStart = inclusiveStart;
				capture.inclusiveEnd = inclusiveEnd;
				capture.reverse = reverse;
				capture.count = count;

				variableCapture.set(capture);

				return null;
			}

			@Override
			public void insert(Long key, String value)
			{
				VariableCapture capture = new VariableCapture();
				capture.start = key;

				variableCapture.set(capture);
			}

			@Override
			public void insert(Long key, String value, int ttl)
			{
				VariableCapture capture = new VariableCapture();
				capture.start = key;

				variableCapture.set(capture);
			}

			@Override
			public String get(Long key)
			{
				VariableCapture capture = new VariableCapture();
				capture.start = key;

				variableCapture.set(capture);

				return null;
			}

			@Override
			public List<KeyValue<Long, String>> find(Long start, boolean inclusiveStart, Long end,
					boolean inclusiveEnd, boolean reverse, int count)
			{
				VariableCapture capture = new VariableCapture();
				capture.start = start;
				capture.end = end;
				capture.inclusiveStart = inclusiveStart;
				capture.inclusiveEnd = inclusiveEnd;
				capture.reverse = reverse;
				capture.count = count;

				variableCapture.set(capture);

				return new ArrayList<KeyValue<Long, String>>();
			}

			@Override
			public void removeFirst(int count)
			{
				VariableCapture capture = new VariableCapture();
				capture.start = null;
				capture.end = null;
				capture.inclusiveStart = true;
				capture.inclusiveEnd = true;
				capture.count = count;
				capture.reverse = false;

				variableCapture.set(capture);
			}

			@Override
			public void removeLast(int count)
			{
				VariableCapture capture = new VariableCapture();
				capture.start = null;
				capture.end = null;
				capture.inclusiveStart = true;
				capture.inclusiveEnd = true;
				capture.count = count;
				capture.reverse = true;

				variableCapture.set(capture);
			}

			@Override
			public List<String> findValues(Long start, boolean inclusiveStart, Long end,
					boolean inclusiveEnd, boolean reverse, int count)
			{
				VariableCapture capture = new VariableCapture();
				capture.start = null;
				capture.end = null;
				capture.inclusiveStart = true;
				capture.inclusiveEnd = true;
				capture.count = count;
				capture.reverse = true;

				variableCapture.set(capture);

				return null;
			}

			@Override
			public List<Long> findKeys(Long start, boolean inclusiveStart, Long end,
					boolean inclusiveEnd, boolean reverse, int count)
			{
				VariableCapture capture = new VariableCapture();
				capture.start = null;
				capture.end = null;
				capture.inclusiveStart = true;
				capture.inclusiveEnd = true;
				capture.count = count;
				capture.reverse = true;

				variableCapture.set(capture);

				return null;
			}
		};
	}

	@Test
	public void should_find_bounds_exclusive() throws Exception
	{
		wrapper.findBoundsExclusive(1L, 2L, 12);

		VariableCapture capture = variableCapture.get();

		assertThat(capture.start).isEqualTo(1L);
		assertThat(capture.end).isEqualTo(2L);
		assertThat(capture.inclusiveStart).isEqualTo(false);
		assertThat(capture.inclusiveEnd).isEqualTo(false);
		assertThat(capture.reverse).isEqualTo(false);
		assertThat(capture.count).isEqualTo(12);
	}

	@Test
	public void should_find_reverse() throws Exception
	{
		wrapper.findReverse(9L, 3L, 11);

		VariableCapture capture = variableCapture.get();

		assertThat(capture.start).isEqualTo(9L);
		assertThat(capture.end).isEqualTo(3L);
		assertThat(capture.inclusiveStart).isEqualTo(true);
		assertThat(capture.inclusiveEnd).isEqualTo(true);
		assertThat(capture.reverse).isEqualTo(true);
		assertThat(capture.count).isEqualTo(11);
	}

	@Test
	public void should_find_reverse_bounds_exclusive() throws Exception
	{

		wrapper.findReverseBoundsExclusive(13L, 4L, 7);

		VariableCapture capture = variableCapture.get();

		assertThat(capture.start).isEqualTo(13L);
		assertThat(capture.end).isEqualTo(4L);
		assertThat(capture.inclusiveStart).isEqualTo(false);
		assertThat(capture.inclusiveEnd).isEqualTo(false);
		assertThat(capture.reverse).isEqualTo(true);
		assertThat(capture.count).isEqualTo(7);
	}

	@Test
	public void should_find_first() throws Exception
	{
		wrapper.findFirst();

		VariableCapture capture = variableCapture.get();

		assertThat(capture.start).isEqualTo(null);
		assertThat(capture.end).isEqualTo(null);
		assertThat(capture.inclusiveStart).isEqualTo(true);
		assertThat(capture.inclusiveEnd).isEqualTo(true);
		assertThat(capture.reverse).isEqualTo(false);
		assertThat(capture.count).isEqualTo(1);
	}

	@Test
	public void should_find_first_n() throws Exception
	{
		wrapper.findFirst(5);

		VariableCapture capture = variableCapture.get();

		assertThat(capture.start).isEqualTo(null);
		assertThat(capture.end).isEqualTo(null);
		assertThat(capture.inclusiveStart).isEqualTo(true);
		assertThat(capture.inclusiveEnd).isEqualTo(true);
		assertThat(capture.reverse).isEqualTo(false);
		assertThat(capture.count).isEqualTo(5);
	}

	@Test
	public void should_find_last() throws Exception
	{
		wrapper.findLast();

		VariableCapture capture = variableCapture.get();

		assertThat(capture.start).isEqualTo(null);
		assertThat(capture.end).isEqualTo(null);
		assertThat(capture.inclusiveStart).isEqualTo(true);
		assertThat(capture.inclusiveEnd).isEqualTo(true);
		assertThat(capture.reverse).isEqualTo(true);
		assertThat(capture.count).isEqualTo(1);
	}

	@Test
	public void should_find_last_n() throws Exception
	{
		wrapper.findLast(9);

		VariableCapture capture = variableCapture.get();

		assertThat(capture.start).isEqualTo(null);
		assertThat(capture.end).isEqualTo(null);
		assertThat(capture.inclusiveStart).isEqualTo(true);
		assertThat(capture.inclusiveEnd).isEqualTo(true);
		assertThat(capture.reverse).isEqualTo(true);
		assertThat(capture.count).isEqualTo(9);
	}

	@Test
	public void should_get_iterator() throws Exception
	{
		wrapper.iterator(21L, 25L, 2);

		VariableCapture capture = variableCapture.get();

		assertThat(capture.start).isEqualTo(21L);
		assertThat(capture.end).isEqualTo(25L);
		assertThat(capture.inclusiveStart).isEqualTo(true);
		assertThat(capture.inclusiveEnd).isEqualTo(true);
		assertThat(capture.reverse).isEqualTo(false);
		assertThat(capture.count).isEqualTo(2);
	}

	@Test
	public void should_get_iterator_bounds_exclusive() throws Exception
	{
		wrapper.iteratorBoundsExclusive(2L, 6L, 14);

		VariableCapture capture = variableCapture.get();

		assertThat(capture.start).isEqualTo(2L);
		assertThat(capture.end).isEqualTo(6L);
		assertThat(capture.inclusiveStart).isEqualTo(false);
		assertThat(capture.inclusiveEnd).isEqualTo(false);
		assertThat(capture.reverse).isEqualTo(false);
		assertThat(capture.count).isEqualTo(14);
	}

	@Test
	public void should_get_iterator_reverse() throws Exception
	{
		wrapper.iteratorReverse(7L, 1L, 16);

		VariableCapture capture = variableCapture.get();

		assertThat(capture.start).isEqualTo(7L);
		assertThat(capture.end).isEqualTo(1L);
		assertThat(capture.inclusiveStart).isEqualTo(true);
		assertThat(capture.inclusiveEnd).isEqualTo(true);
		assertThat(capture.reverse).isEqualTo(true);
		assertThat(capture.count).isEqualTo(16);
	}

	@Test
	public void should_get_iterator_reverse_bounds_exclusive() throws Exception
	{
		wrapper.iteratorReverseBoundsExclusive(8L, 2L, 17);

		VariableCapture capture = variableCapture.get();

		assertThat(capture.start).isEqualTo(8L);
		assertThat(capture.end).isEqualTo(2L);
		assertThat(capture.inclusiveStart).isEqualTo(false);
		assertThat(capture.inclusiveEnd).isEqualTo(false);
		assertThat(capture.reverse).isEqualTo(true);
		assertThat(capture.count).isEqualTo(17);
	}

	@Test
	public void should_remove() throws Exception
	{
		wrapper.remove(33L, 45L);

		VariableCapture capture = variableCapture.get();

		assertThat(capture.start).isEqualTo(33L);
		assertThat(capture.end).isEqualTo(45L);
		assertThat(capture.inclusiveStart).isEqualTo(true);
		assertThat(capture.inclusiveEnd).isEqualTo(true);
		assertThat(capture.reverse).isEqualTo(false);
		assertThat(capture.count).isEqualTo(0);
	}

	@Test
	public void should_remove_bounds_exclusive() throws Exception
	{
		wrapper.removeBoundsExclusive(13L, 25L);

		VariableCapture capture = variableCapture.get();

		assertThat(capture.start).isEqualTo(13L);
		assertThat(capture.end).isEqualTo(25L);
		assertThat(capture.inclusiveStart).isEqualTo(false);
		assertThat(capture.inclusiveEnd).isEqualTo(false);
		assertThat(capture.reverse).isEqualTo(false);
		assertThat(capture.count).isEqualTo(0);
	}

	@Test
	public void should_remove_first_n() throws Exception
	{
		wrapper.removeFirst(50);

		VariableCapture capture = variableCapture.get();

		assertThat(capture.start).isEqualTo(null);
		assertThat(capture.end).isEqualTo(null);
		assertThat(capture.reverse).isEqualTo(false);
		assertThat(capture.count).isEqualTo(50);
	}

	@Test
	public void should_remove_last_n() throws Exception
	{
		wrapper.removeLast(51);

		VariableCapture capture = variableCapture.get();

		assertThat(capture.start).isEqualTo(null);
		assertThat(capture.end).isEqualTo(null);
		assertThat(capture.reverse).isEqualTo(true);
		assertThat(capture.count).isEqualTo(51);
	}

	class VariableCapture
	{
		private Long start;
		private Long end;
		private boolean inclusiveStart;
		private boolean inclusiveEnd;
		private boolean reverse;
		private int count;
	}
}
