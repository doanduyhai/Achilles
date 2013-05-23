package info.archinnov.achilles.proxy.wrapper;

import static info.archinnov.achilles.type.ConsistencyLevel.LOCAL_QUORUM;
import static info.archinnov.achilles.type.WideMap.BoundingMode.INCLUSIVE_BOUNDS;
import static info.archinnov.achilles.type.WideMap.OrderingMode.ASCENDING;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.entity.operations.AchillesEntityValidator;
import info.archinnov.achilles.proxy.AchillesEntityInterceptor;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.KeyValue;
import info.archinnov.achilles.type.KeyValueIterator;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.internal.util.reflection.Whitebox;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * ThriftAbstractWideMapWrapperTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class ThriftAbstractWideMapWrapperTest
{

	@Mock
	private ThriftGenericEntityDao dao;

	@Mock
	private AchillesEntityInterceptor<Long> interceptor;

	@Mock
	private AchillesEntityValidator validator;

	@Mock
	private ThriftPersistenceContext context;

	private ThreadLocal<StateCapture> stateCapture = new ThreadLocal<StateCapture>();

	private ThriftAbstractWideMapWrapper<Long, String> wrapper = new ThriftAbstractWideMapWrapper<Long, String>()
	{

		@Override
		public void remove(Long start, Long end, BoundingMode bounds)
		{
			StateCapture capture = new StateCapture();
			capture.start = start;
			capture.end = end;
			capture.inclusiveStart = bounds.isInclusiveStart();
			capture.inclusiveEnd = bounds.isInclusiveEnd();

			stateCapture.set(capture);
		}

		@Override
		public void remove(Long key)
		{
			StateCapture capture = new StateCapture();
			capture.start = key;

			stateCapture.set(capture);
		}

		@Override
		public void removeFirst()
		{
			StateCapture capture = new StateCapture();
			capture.start = null;
			capture.end = null;
			capture.reverse = false;
			capture.count = 1;

			stateCapture.set(capture);
		}

		@Override
		public void removeLast()
		{
			StateCapture capture = new StateCapture();
			capture.start = null;
			capture.end = null;
			capture.reverse = true;
			capture.count = 1;

			stateCapture.set(capture);
		}

		@Override
		public KeyValueIterator<Long, String> iterator(Long start, Long end, int count,
				BoundingMode bounds, OrderingMode ordering)
		{
			StateCapture capture = new StateCapture();
			capture.start = start;
			capture.end = end;
			capture.inclusiveStart = bounds.isInclusiveStart();
			capture.inclusiveEnd = bounds.isInclusiveEnd();
			capture.reverse = ordering.isReverse();
			capture.count = count;

			stateCapture.set(capture);

			return null;
		}

		@Override
		public void insert(Long key, String value)
		{
			StateCapture capture = new StateCapture();
			capture.start = key;

			stateCapture.set(capture);
		}

		@Override
		public void insert(Long key, String value, int ttl)
		{
			StateCapture capture = new StateCapture();
			capture.start = key;

			stateCapture.set(capture);
		}

		@Override
		public String get(Long key)
		{
			StateCapture capture = new StateCapture();
			capture.start = key;

			stateCapture.set(capture);

			return null;
		}

		@Override
		public KeyValue<Long, String> findFirstMatching(final Long key)
		{
			StateCapture capture = new StateCapture();
			capture.start = key;
			capture.end = key;
			capture.inclusiveStart = true;
			capture.inclusiveEnd = true;
			capture.reverse = false;
			capture.count = 1;

			stateCapture.set(capture);

			return null;
		}

		@Override
		public KeyValue<Long, String> findLastMatching(final Long key)
		{
			StateCapture capture = new StateCapture();
			capture.start = key;
			capture.end = key;
			capture.inclusiveStart = true;
			capture.inclusiveEnd = true;
			capture.reverse = true;
			capture.count = 1;

			stateCapture.set(capture);

			return null;
		}

		@Override
		public List<KeyValue<Long, String>> find(Long start, Long end, int count,
				BoundingMode bounds, OrderingMode ordering)
		{
			StateCapture capture = new StateCapture();
			capture.start = start;
			capture.end = end;
			capture.inclusiveStart = bounds.isInclusiveStart();
			capture.inclusiveEnd = bounds.isInclusiveEnd();
			capture.reverse = ordering.isReverse();
			capture.count = count;

			stateCapture.set(capture);

			return new ArrayList<KeyValue<Long, String>>();
		}

		@Override
		public void removeFirst(int count)
		{
			StateCapture capture = new StateCapture();
			capture.start = null;
			capture.end = null;
			capture.inclusiveStart = true;
			capture.inclusiveEnd = true;
			capture.count = count;
			capture.reverse = false;

			stateCapture.set(capture);
		}

		@Override
		public void removeLast(int count)
		{
			StateCapture capture = new StateCapture();
			capture.start = null;
			capture.end = null;
			capture.inclusiveStart = true;
			capture.inclusiveEnd = true;
			capture.count = count;
			capture.reverse = true;

			stateCapture.set(capture);
		}

		@Override
		public String findValue(final Long key)
		{
			StateCapture capture = new StateCapture();
			capture.start = key;
			capture.end = key;
			capture.inclusiveStart = true;
			capture.inclusiveEnd = true;
			capture.reverse = false;
			capture.count = 1;

			stateCapture.set(capture);

			return null;
		}

		@Override
		public List<String> findValues(Long start, Long end, int count, BoundingMode bounds,
				OrderingMode ordering)
		{
			StateCapture capture = new StateCapture();
			capture.start = null;
			capture.end = null;
			capture.inclusiveStart = true;
			capture.inclusiveEnd = true;
			capture.count = count;
			capture.reverse = true;

			stateCapture.set(capture);

			return new ArrayList<String>();
		}

		@Override
		public Long findKey(final Long key)
		{
			StateCapture capture = new StateCapture();
			capture.start = key;
			capture.end = key;
			capture.inclusiveStart = true;
			capture.inclusiveEnd = true;
			capture.reverse = false;
			capture.count = 1;

			stateCapture.set(capture);

			return null;
		}

		@Override
		public List<Long> findKeys(Long start, Long end, int count, BoundingMode bounds,
				OrderingMode ordering)
		{
			StateCapture capture = new StateCapture();
			capture.start = null;
			capture.end = null;
			capture.inclusiveStart = true;
			capture.inclusiveEnd = true;
			capture.count = count;
			capture.reverse = true;

			stateCapture.set(capture);

			return new ArrayList<Long>();
		}

		@Override
		public String get(final Long key, final ConsistencyLevel readLevel)
		{
			super.get(key, readLevel);
			StateCapture capture = new StateCapture();
			capture.readLevel = readLevel;
			stateCapture.set(capture);
			return "";
		}

		@Override
		public void insert(final Long key, final String value, final int ttl,
				final ConsistencyLevel writeLevel)
		{
			super.insert(key, value, ttl, writeLevel);
			StateCapture capture = new StateCapture();
			capture.writeLevel = writeLevel;
			stateCapture.set(capture);
		}

		@Override
		public KeyValue<Long, String> findFirstMatching(final Long key,
				final ConsistencyLevel readLevel)
		{
			StateCapture capture = new StateCapture();
			capture.readLevel = readLevel;
			stateCapture.set(capture);

			return null;
		}

		@Override
		public KeyValue<Long, String> findLastMatching(final Long key,
				final ConsistencyLevel readLevel)
		{
			StateCapture capture = new StateCapture();
			capture.readLevel = readLevel;
			stateCapture.set(capture);

			return null;
		}

		@Override
		public List<KeyValue<Long, String>> find(final Long start, final Long end, final int count,
				final ConsistencyLevel readLevel)
		{
			super.find(start, end, count, readLevel);
			StateCapture capture = new StateCapture();
			capture.readLevel = readLevel;
			stateCapture.set(capture);
			return null;
		}

		@Override
		public List<KeyValue<Long, String>> find(final Long start, final Long end, final int count,
				final BoundingMode bounds, final OrderingMode ordering,
				final ConsistencyLevel readLevel)
		{
			super.find(start, end, count, bounds, ordering, readLevel);
			StateCapture capture = new StateCapture();
			capture.readLevel = readLevel;
			stateCapture.set(capture);
			return null;
		}

		@Override
		public List<KeyValue<Long, String>> findBoundsExclusive(final Long start, final Long end,
				final int count, final ConsistencyLevel readLevel)
		{
			super.findBoundsExclusive(start, end, count, readLevel);
			StateCapture capture = new StateCapture();
			capture.readLevel = readLevel;
			stateCapture.set(capture);
			return null;
		}

		@Override
		public List<KeyValue<Long, String>> findReverse(final Long start, final Long end,
				final int count, final ConsistencyLevel readLevel)
		{
			super.findReverse(start, end, count, readLevel);
			StateCapture capture = new StateCapture();
			capture.readLevel = readLevel;
			stateCapture.set(capture);
			return null;
		}

		@Override
		public List<KeyValue<Long, String>> findReverseBoundsExclusive(final Long start,
				final Long end, final int count, final ConsistencyLevel readLevel)
		{
			super.findReverseBoundsExclusive(start, end, count, readLevel);
			StateCapture capture = new StateCapture();
			capture.readLevel = readLevel;
			stateCapture.set(capture);
			return null;
		}

		@Override
		public KeyValue<Long, String> findFirst(final ConsistencyLevel readLevel)
		{
			super.findFirst(readLevel);
			StateCapture capture = new StateCapture();
			capture.readLevel = readLevel;
			stateCapture.set(capture);
			return null;
		}

		@Override
		public List<KeyValue<Long, String>> findFirst(final int count,
				final ConsistencyLevel readLevel)
		{
			super.findFirst(count, readLevel);
			StateCapture capture = new StateCapture();
			capture.readLevel = readLevel;
			stateCapture.set(capture);
			return null;
		}

		@Override
		public KeyValue<Long, String> findLast(final ConsistencyLevel readLevel)
		{
			super.findLast(readLevel);
			StateCapture capture = new StateCapture();
			capture.readLevel = readLevel;
			stateCapture.set(capture);
			return null;
		}

		@Override
		public List<KeyValue<Long, String>> findLast(final int count,
				final ConsistencyLevel readLevel)
		{
			super.findLast(count, readLevel);
			StateCapture capture = new StateCapture();
			capture.readLevel = readLevel;
			stateCapture.set(capture);
			return null;
		}

		@Override
		public String findValue(final Long key, final ConsistencyLevel readLevel)
		{
			StateCapture capture = new StateCapture();
			capture.readLevel = readLevel;
			stateCapture.set(capture);

			return null;
		}

		@Override
		public List<String> findValues(final Long start, final Long end, final int count,
				final ConsistencyLevel readLevel)
		{
			super.findValues(start, end, count, readLevel);
			StateCapture capture = new StateCapture();
			capture.readLevel = readLevel;
			stateCapture.set(capture);
			return null;
		}

		@Override
		public List<String> findValues(final Long start, final Long end, final int count,
				final BoundingMode bounds, final OrderingMode ordering,
				final ConsistencyLevel readLevel)
		{
			super.findValues(start, end, count, bounds, ordering, readLevel);
			StateCapture capture = new StateCapture();
			capture.readLevel = readLevel;
			stateCapture.set(capture);
			return null;
		}

		@Override
		public List<String> findBoundsExclusiveValues(final Long start, final Long end,
				final int count, final ConsistencyLevel readLevel)
		{
			super.findBoundsExclusiveValues(start, end, count, readLevel);
			StateCapture capture = new StateCapture();
			capture.readLevel = readLevel;
			stateCapture.set(capture);
			return null;
		}

		@Override
		public List<String> findReverseBoundsExclusiveValues(final Long start, final Long end,
				final int count, final ConsistencyLevel readLevel)
		{
			super.findReverseBoundsExclusiveValues(start, end, count, readLevel);
			StateCapture capture = new StateCapture();
			capture.readLevel = readLevel;
			stateCapture.set(capture);
			return null;
		}

		@Override
		public List<String> findReverseValues(final Long start, final Long end, final int count,
				final ConsistencyLevel readLevel)
		{
			super.findReverseValues(start, end, count, readLevel);
			StateCapture capture = new StateCapture();
			capture.readLevel = readLevel;
			stateCapture.set(capture);
			return null;
		}

		@Override
		public List<String> findFirstValues(final int count, final ConsistencyLevel readLevel)
		{
			super.findFirstValues(count, readLevel);
			StateCapture capture = new StateCapture();
			capture.readLevel = readLevel;
			stateCapture.set(capture);
			return null;
		}

		@Override
		public String findLastValue(final ConsistencyLevel readLevel)
		{
			super.findLastValue(readLevel);
			StateCapture capture = new StateCapture();
			capture.readLevel = readLevel;
			stateCapture.set(capture);
			return null;
		}

		@Override
		public List<String> findLastValues(final int count, final ConsistencyLevel readLevel)
		{
			super.findLastValues(count, readLevel);
			StateCapture capture = new StateCapture();
			capture.readLevel = readLevel;
			stateCapture.set(capture);
			return null;
		}

		@Override
		public Long findKey(final Long key, final ConsistencyLevel readLevel)
		{
			StateCapture capture = new StateCapture();
			capture.readLevel = readLevel;
			stateCapture.set(capture);

			return null;
		}

		@Override
		public List<Long> findKeys(final Long start, final Long end, final int count,
				final ConsistencyLevel readLevel)
		{
			super.findKeys(start, end, count, readLevel);
			StateCapture capture = new StateCapture();
			capture.readLevel = readLevel;
			stateCapture.set(capture);
			return null;
		}

		@Override
		public List<Long> findKeys(final Long start, final Long end, final int count,
				final BoundingMode bounds, final OrderingMode ordering,
				final ConsistencyLevel readLevel)
		{
			super.findKeys(start, end, count, bounds, ordering, readLevel);
			StateCapture capture = new StateCapture();
			capture.readLevel = readLevel;
			stateCapture.set(capture);
			return null;
		}

		@Override
		public List<Long> findBoundsExclusiveKeys(final Long start, final Long end,
				final int count, final ConsistencyLevel readLevel)
		{
			super.findBoundsExclusiveKeys(start, end, count, readLevel);
			StateCapture capture = new StateCapture();
			capture.readLevel = readLevel;
			stateCapture.set(capture);
			return null;
		}

		@Override
		public List<Long> findReverseKeys(final Long start, final Long end, final int count,
				final ConsistencyLevel readLevel)
		{
			super.findReverseKeys(start, end, count, readLevel);
			StateCapture capture = new StateCapture();
			capture.readLevel = readLevel;
			stateCapture.set(capture);
			return null;
		}

		@Override
		public List<Long> findReverseBoundsExclusiveKeys(final Long start, final Long end,
				final int count, final ConsistencyLevel readLevel)
		{
			super.findReverseBoundsExclusiveKeys(start, end, count, readLevel);
			StateCapture capture = new StateCapture();
			capture.readLevel = readLevel;
			stateCapture.set(capture);
			return null;
		}

		@Override
		public List<Long> findFirstKeys(final int count, final ConsistencyLevel readLevel)
		{
			super.findFirstKeys(count, readLevel);
			StateCapture capture = new StateCapture();
			capture.readLevel = readLevel;
			stateCapture.set(capture);
			return null;
		}

		@Override
		public Long findLastKey(final ConsistencyLevel readLevel)
		{
			super.findLastKey(readLevel);
			StateCapture capture = new StateCapture();
			capture.readLevel = readLevel;
			stateCapture.set(capture);
			return null;
		}

		@Override
		public List<Long> findLastKeys(final int count, final ConsistencyLevel readLevel)
		{
			super.findLastKeys(count, readLevel);
			StateCapture capture = new StateCapture();
			capture.readLevel = readLevel;
			stateCapture.set(capture);
			return null;
		}

		@Override
		public KeyValueIterator<Long, String> iterator(final Long start, final Long end,
				final int count, final BoundingMode bounds, final OrderingMode ordering,
				final ConsistencyLevel readLevel)
		{
			StateCapture capture = new StateCapture();
			capture.readLevel = readLevel;
			stateCapture.set(capture);
			return null;
		}

		@Override
		public KeyValueIterator<Long, String> iterator(final int count,
				final ConsistencyLevel readLevel)
		{
			super.iterator(count, readLevel);
			StateCapture capture = new StateCapture();
			capture.readLevel = readLevel;
			stateCapture.set(capture);
			return null;
		}

		@Override
		public KeyValueIterator<Long, String> iterator(final Long start, final Long end,
				final int count, final ConsistencyLevel readLevel)
		{
			super.iterator(start, end, count, readLevel);
			StateCapture capture = new StateCapture();
			capture.readLevel = readLevel;
			stateCapture.set(capture);
			return null;
		}

		@Override
		public KeyValueIterator<Long, String> iteratorBoundsExclusive(final Long start,
				final Long end, final int count, final ConsistencyLevel readLevel)
		{
			super.iteratorBoundsExclusive(start, end, count, readLevel);
			StateCapture capture = new StateCapture();
			capture.readLevel = readLevel;
			stateCapture.set(capture);
			return null;
		}

		@Override
		public KeyValueIterator<Long, String> iteratorReverse(final ConsistencyLevel readLevel)
		{
			super.iteratorReverse(readLevel);
			StateCapture capture = new StateCapture();
			capture.readLevel = readLevel;
			stateCapture.set(capture);
			return null;
		}

		@Override
		public KeyValueIterator<Long, String> iteratorReverse(final int count,
				final ConsistencyLevel readLevel)
		{
			super.iteratorReverse(count, readLevel);
			StateCapture capture = new StateCapture();
			capture.readLevel = readLevel;
			stateCapture.set(capture);
			return null;
		}

		@Override
		public KeyValueIterator<Long, String> iteratorReverse(final Long start, final Long end,
				final int count, final ConsistencyLevel readLevel)
		{
			super.iteratorReverse(start, end, count, readLevel);
			StateCapture capture = new StateCapture();
			capture.readLevel = readLevel;
			stateCapture.set(capture);
			return null;
		}

		@Override
		public KeyValueIterator<Long, String> iteratorReverseBoundsExclusive(final Long start,
				final Long end, final int count, final ConsistencyLevel readLevel)
		{
			super.iteratorReverseBoundsExclusive(start, end, count, readLevel);
			StateCapture capture = new StateCapture();
			capture.readLevel = readLevel;
			stateCapture.set(capture);
			return null;
		}

		@Override
		public void remove(final Long key, final ConsistencyLevel writeLevel)
		{
			super.remove(key, writeLevel);
			StateCapture capture = new StateCapture();
			capture.writeLevel = writeLevel;
			stateCapture.set(capture);
		}

		@Override
		public void remove(final Long start, final Long end, final ConsistencyLevel writeLevel)
		{
			super.remove(start, end, writeLevel);
			StateCapture capture = new StateCapture();
			capture.writeLevel = writeLevel;
			stateCapture.set(capture);
		}

		@Override
		public void remove(final Long start, final Long end, final BoundingMode bounds,
				final ConsistencyLevel writeLevel)
		{
			super.remove(start, end, bounds, writeLevel);
			StateCapture capture = new StateCapture();
			capture.writeLevel = writeLevel;
			stateCapture.set(capture);
		}

		@Override
		public void removeBoundsExclusive(final Long start, final Long end,
				final ConsistencyLevel writeLevel)
		{
			super.removeBoundsExclusive(start, end, writeLevel);
			StateCapture capture = new StateCapture();
			capture.writeLevel = writeLevel;
			stateCapture.set(capture);
		}

		@Override
		public void removeFirst(final ConsistencyLevel writeLevel)
		{
			super.removeFirst(writeLevel);
			StateCapture capture = new StateCapture();
			capture.writeLevel = writeLevel;
			stateCapture.set(capture);
		}

		@Override
		public void removeFirst(final int count, final ConsistencyLevel writeLevel)
		{
			super.removeFirst(count, writeLevel);
			StateCapture capture = new StateCapture();
			capture.writeLevel = writeLevel;
			stateCapture.set(capture);
		}

		@Override
		public void removeLast(final ConsistencyLevel writeLevel)
		{
			super.removeLast(writeLevel);
			StateCapture capture = new StateCapture();
			capture.writeLevel = writeLevel;
			stateCapture.set(capture);
		}

		@Override
		public void removeLast(final int count, final ConsistencyLevel writeLevel)
		{
			super.removeLast(count, writeLevel);
			StateCapture capture = new StateCapture();
			capture.writeLevel = writeLevel;
			stateCapture.set(capture);
		}
	};

	@Before
	public void setUp()
	{
		Whitebox.setInternalState(wrapper, "context", context);
		Whitebox.setInternalState(wrapper, "validator", validator);
	}

	@Test
	public void should_find_first_matching() throws Exception
	{
		wrapper.findFirstMatching(1L);

		StateCapture capture = stateCapture.get();

		assertThat(capture.start).isEqualTo(1L);
		assertThat(capture.end).isEqualTo(1L);
		assertThat(capture.inclusiveStart).isEqualTo(true);
		assertThat(capture.inclusiveEnd).isEqualTo(true);
		assertThat(capture.reverse).isEqualTo(false);
		assertThat(capture.count).isEqualTo(1);
	}

	@Test
	public void should_find_last_matching() throws Exception
	{
		wrapper.findLastMatching(1L);

		StateCapture capture = stateCapture.get();

		assertThat(capture.start).isEqualTo(1L);
		assertThat(capture.end).isEqualTo(1L);
		assertThat(capture.inclusiveStart).isEqualTo(true);
		assertThat(capture.inclusiveEnd).isEqualTo(true);
		assertThat(capture.reverse).isEqualTo(true);
		assertThat(capture.count).isEqualTo(1);
	}

	@Test
	public void should_find_bounds_exclusive() throws Exception
	{
		wrapper.findBoundsExclusive(1L, 2L, 12);

		StateCapture capture = stateCapture.get();

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

		StateCapture capture = stateCapture.get();

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

		StateCapture capture = stateCapture.get();

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

		StateCapture capture = stateCapture.get();

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

		StateCapture capture = stateCapture.get();

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

		StateCapture capture = stateCapture.get();

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

		StateCapture capture = stateCapture.get();

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

		StateCapture capture = stateCapture.get();

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

		StateCapture capture = stateCapture.get();

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

		StateCapture capture = stateCapture.get();

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

		StateCapture capture = stateCapture.get();

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

		StateCapture capture = stateCapture.get();

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

		StateCapture capture = stateCapture.get();

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

		StateCapture capture = stateCapture.get();

		assertThat(capture.start).isEqualTo(null);
		assertThat(capture.end).isEqualTo(null);
		assertThat(capture.reverse).isEqualTo(false);
		assertThat(capture.count).isEqualTo(50);
	}

	@Test
	public void should_remove_last_n() throws Exception
	{
		wrapper.removeLast(51);

		StateCapture capture = stateCapture.get();

		assertThat(capture.start).isEqualTo(null);
		assertThat(capture.end).isEqualTo(null);
		assertThat(capture.reverse).isEqualTo(true);
		assertThat(capture.count).isEqualTo(51);
	}

	@Test
	public void should_get_with_consistency() throws Exception
	{
		wrapper.get(11L, LOCAL_QUORUM);
		assertThat(stateCapture.get().readLevel).isEqualTo(LOCAL_QUORUM);
	}

	@Test
	public void should_insert_with_consistency() throws Exception
	{
		wrapper.insert(11L, "test", 10, LOCAL_QUORUM);
		assertThat(stateCapture.get().writeLevel).isEqualTo(LOCAL_QUORUM);
	}

	@Test
	public void should_find_with_consistency() throws Exception
	{
		wrapper.find(11L, 12L, 10, LOCAL_QUORUM);
		assertThat(stateCapture.get().readLevel).isEqualTo(LOCAL_QUORUM);
	}

	@Test
	public void should_find_all_params_with_consistency() throws Exception
	{
		wrapper.find(11L, 12L, 10, INCLUSIVE_BOUNDS, ASCENDING, LOCAL_QUORUM);
		assertThat(stateCapture.get().readLevel).isEqualTo(LOCAL_QUORUM);
	}

	@Test
	public void should_findBoundsExclusive_with_consistency() throws Exception
	{
		wrapper.findBoundsExclusive(11L, 12L, 10, LOCAL_QUORUM);
		assertThat(stateCapture.get().readLevel).isEqualTo(LOCAL_QUORUM);
	}

	@Test
	public void should_findReverse_with_consistency() throws Exception
	{
		wrapper.findReverse(11L, 10L, 10, LOCAL_QUORUM);
		assertThat(stateCapture.get().readLevel).isEqualTo(LOCAL_QUORUM);
	}

	@Test
	public void should_findReverseBoundsExclusive_with_consistency() throws Exception
	{
		wrapper.findReverseBoundsExclusive(11L, 10L, 10, LOCAL_QUORUM);
		assertThat(stateCapture.get().readLevel).isEqualTo(LOCAL_QUORUM);
	}

	@Test
	public void should_findFirst_with_consistency() throws Exception
	{
		wrapper.findFirst(LOCAL_QUORUM);
		assertThat(stateCapture.get().readLevel).isEqualTo(LOCAL_QUORUM);
	}

	@Test
	public void should_findFirst_n_with_consistency() throws Exception
	{
		wrapper.findFirst(10, LOCAL_QUORUM);
		assertThat(stateCapture.get().readLevel).isEqualTo(LOCAL_QUORUM);
	}

	@Test
	public void should_findLast_with_consistency() throws Exception
	{
		wrapper.findLast(LOCAL_QUORUM);
		assertThat(stateCapture.get().readLevel).isEqualTo(LOCAL_QUORUM);
	}

	@Test
	public void should_findLast_n_with_consistency() throws Exception
	{
		wrapper.findLast(10, LOCAL_QUORUM);
		assertThat(stateCapture.get().readLevel).isEqualTo(LOCAL_QUORUM);
	}

	@Test
	public void should_findValues_with_consistency() throws Exception
	{
		wrapper.findValues(11L, 12L, 10, LOCAL_QUORUM);
		assertThat(stateCapture.get().readLevel).isEqualTo(LOCAL_QUORUM);
	}

	@Test
	public void should_findValues_all_params_with_consistency() throws Exception
	{
		wrapper.findValues(11L, 12L, 10, INCLUSIVE_BOUNDS, ASCENDING, LOCAL_QUORUM);
		assertThat(stateCapture.get().readLevel).isEqualTo(LOCAL_QUORUM);
	}

	@Test
	public void should_findBoundsExclusiveValues_with_consistency() throws Exception
	{
		wrapper.findBoundsExclusiveValues(11L, 12L, 10, LOCAL_QUORUM);
		assertThat(stateCapture.get().readLevel).isEqualTo(LOCAL_QUORUM);
	}

	@Test
	public void should_findReverseBoundsExclusiveValues_with_consistency() throws Exception
	{
		wrapper.findReverseBoundsExclusiveValues(11L, 10L, 10, LOCAL_QUORUM);
		assertThat(stateCapture.get().readLevel).isEqualTo(LOCAL_QUORUM);
	}

	@Test
	public void should_findReverseValues_with_consistency() throws Exception
	{
		wrapper.findReverseValues(11L, 10L, 10, LOCAL_QUORUM);
		assertThat(stateCapture.get().readLevel).isEqualTo(LOCAL_QUORUM);
	}

	@Test
	public void should_findFirstValues_n_with_consistency() throws Exception
	{
		wrapper.findFirstValues(10, LOCAL_QUORUM);
		assertThat(stateCapture.get().readLevel).isEqualTo(LOCAL_QUORUM);
	}

	@Test
	public void should_findLastValue_with_consistency() throws Exception
	{
		wrapper.findLastValue(LOCAL_QUORUM);
		assertThat(stateCapture.get().readLevel).isEqualTo(LOCAL_QUORUM);
	}

	@Test
	public void should_findLastValues_n__with_consistency() throws Exception
	{
		wrapper.findLastValues(10, LOCAL_QUORUM);
		assertThat(stateCapture.get().readLevel).isEqualTo(LOCAL_QUORUM);
	}

	@Test
	public void should_findKeys_with_consistency() throws Exception
	{
		wrapper.findKeys(11L, 12L, 10, LOCAL_QUORUM);
		assertThat(stateCapture.get().readLevel).isEqualTo(LOCAL_QUORUM);
	}

	@Test
	public void should_findKeys_all_params_with_consistency() throws Exception
	{
		wrapper.findKeys(11L, 12L, 10, INCLUSIVE_BOUNDS, ASCENDING, LOCAL_QUORUM);
		assertThat(stateCapture.get().readLevel).isEqualTo(LOCAL_QUORUM);
	}

	@Test
	public void should_findBoundsExclusiveKeys_with_consistency() throws Exception
	{
		wrapper.findBoundsExclusiveKeys(11L, 12L, 10, LOCAL_QUORUM);
		assertThat(stateCapture.get().readLevel).isEqualTo(LOCAL_QUORUM);
	}

	@Test
	public void should_findReverseKeys_with_consistency() throws Exception
	{
		wrapper.findReverseKeys(11L, 10L, 10, LOCAL_QUORUM);
		assertThat(stateCapture.get().readLevel).isEqualTo(LOCAL_QUORUM);
	}

	@Test
	public void should_findReverseBoundsExclusiveKeys_with_consistency() throws Exception
	{
		wrapper.findReverseBoundsExclusiveKeys(11L, 10L, 10, LOCAL_QUORUM);
		assertThat(stateCapture.get().readLevel).isEqualTo(LOCAL_QUORUM);
	}

	@Test
	public void should_findFirstKeys_n__with_consistency() throws Exception
	{
		wrapper.findFirstKeys(10, LOCAL_QUORUM);
		assertThat(stateCapture.get().readLevel).isEqualTo(LOCAL_QUORUM);
	}

	@Test
	public void should_findLastKey_with_consistency() throws Exception
	{
		wrapper.findLastKey(LOCAL_QUORUM);
		assertThat(stateCapture.get().readLevel).isEqualTo(LOCAL_QUORUM);
	}

	@Test
	public void should_findLastKeys_n_with_consistency() throws Exception
	{
		wrapper.findLastKeys(10, LOCAL_QUORUM);
		assertThat(stateCapture.get().readLevel).isEqualTo(LOCAL_QUORUM);
	}

	@Test
	public void should_iterate_all_params_with_consistency() throws Exception
	{
		wrapper.iterator(11L, 10L, 10, INCLUSIVE_BOUNDS, ASCENDING, LOCAL_QUORUM);
		assertThat(stateCapture.get().readLevel).isEqualTo(LOCAL_QUORUM);
	}

	@Test
	public void should_iterate_n_with_consistency() throws Exception
	{
		wrapper.iterator(10, LOCAL_QUORUM);
		assertThat(stateCapture.get().readLevel).isEqualTo(LOCAL_QUORUM);
	}

	@Test
	public void should_iterate_bounds_with_consistency() throws Exception
	{
		wrapper.iterator(11L, 10L, 10, LOCAL_QUORUM);
		assertThat(stateCapture.get().readLevel).isEqualTo(LOCAL_QUORUM);
	}

	@Test
	public void should_iteratorBoundsExclusive_with_consistency() throws Exception
	{
		wrapper.iteratorBoundsExclusive(11L, 10L, 10, LOCAL_QUORUM);
		assertThat(stateCapture.get().readLevel).isEqualTo(LOCAL_QUORUM);
	}

	@Test
	public void should_iteratorReverse_with_consistency() throws Exception
	{
		wrapper.iteratorReverse(LOCAL_QUORUM);
		assertThat(stateCapture.get().readLevel).isEqualTo(LOCAL_QUORUM);
	}

	@Test
	public void should_iteratorReverse_n_with_consistency() throws Exception
	{
		wrapper.iteratorReverse(10, LOCAL_QUORUM);
		assertThat(stateCapture.get().readLevel).isEqualTo(LOCAL_QUORUM);
	}

	@Test
	public void should_iteratorReverse_bounds_with_consistency() throws Exception
	{
		wrapper.iteratorReverse(11L, 10L, 10, LOCAL_QUORUM);
		assertThat(stateCapture.get().readLevel).isEqualTo(LOCAL_QUORUM);
	}

	@Test
	public void should_iteratorReverseBoundsExclusive_with_consistency() throws Exception
	{
		wrapper.iteratorReverseBoundsExclusive(11L, 10L, 10, LOCAL_QUORUM);
		assertThat(stateCapture.get().readLevel).isEqualTo(LOCAL_QUORUM);
	}

	@Test
	public void should_remove_with_consistency() throws Exception
	{
		wrapper.remove(11L, LOCAL_QUORUM);
		assertThat(stateCapture.get().writeLevel).isEqualTo(LOCAL_QUORUM);
	}

	@Test
	public void should_remove_bounds_with_consistency() throws Exception
	{
		wrapper.remove(11L, 12L, LOCAL_QUORUM);
		assertThat(stateCapture.get().writeLevel).isEqualTo(LOCAL_QUORUM);
	}

	@Test
	public void should_remove_all_params_with_consistency() throws Exception
	{
		wrapper.remove(11L, 12L, INCLUSIVE_BOUNDS, LOCAL_QUORUM);
		assertThat(stateCapture.get().writeLevel).isEqualTo(LOCAL_QUORUM);
	}

	@Test
	public void should_removeBoundsExclusive_with_consistency() throws Exception
	{
		wrapper.removeBoundsExclusive(11L, 12L, LOCAL_QUORUM);
		assertThat(stateCapture.get().writeLevel).isEqualTo(LOCAL_QUORUM);
	}

	@Test
	public void should_removeFirst_with_consistency() throws Exception
	{
		wrapper.removeFirst(LOCAL_QUORUM);
		assertThat(stateCapture.get().writeLevel).isEqualTo(LOCAL_QUORUM);
	}

	@Test
	public void should_removeFirst_n_with_consistency() throws Exception
	{
		wrapper.removeFirst(10, LOCAL_QUORUM);
		assertThat(stateCapture.get().writeLevel).isEqualTo(LOCAL_QUORUM);
	}

	@Test
	public void should_removeLast_with_consistency() throws Exception
	{
		wrapper.removeLast(LOCAL_QUORUM);
		assertThat(stateCapture.get().writeLevel).isEqualTo(LOCAL_QUORUM);
	}

	@Test
	public void should_removeLast_n_with_consistency() throws Exception
	{
		wrapper.removeLast(10, LOCAL_QUORUM);
		assertThat(stateCapture.get().writeLevel).isEqualTo(LOCAL_QUORUM);
	}

	class StateCapture
	{
		private Long start;
		private Long end;
		private boolean inclusiveStart;
		private boolean inclusiveEnd;
		private boolean reverse;
		private int count;
		private ConsistencyLevel readLevel;
		private ConsistencyLevel writeLevel;
	}
}
