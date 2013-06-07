package info.archinnov.achilles.proxy.wrapper;

import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.context.execution.ThriftSafeExecutionContext;
import info.archinnov.achilles.entity.operations.EntityValidator;
import info.archinnov.achilles.entity.operations.ThriftEntityProxifier;
import info.archinnov.achilles.proxy.ThriftEntityInterceptor;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.KeyValue;
import info.archinnov.achilles.type.KeyValueIterator;
import info.archinnov.achilles.type.WideMap;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ThriftAbstractWideMapWrapper
 * 
 * @author DuyHai DOAN
 * 
 */
public abstract class ThriftAbstractWideMapWrapper<K, V> implements WideMap<K, V>
{
	private static final Logger log = LoggerFactory.getLogger(ThriftAbstractWideMapWrapper.class);

	protected ThriftPersistenceContext context;
	protected ThriftEntityInterceptor<?> interceptor;
	protected EntityValidator validator = new EntityValidator(
			new ThriftEntityProxifier());

	protected static final int DEFAULT_COUNT = 100;

	private <T> T reinitConsistencyLevel(final ThriftSafeExecutionContext<T> executionContext)
	{
		log.trace("Execute safely");
		try
		{
			return executionContext.execute();
		}
		finally
		{
			log.trace("Cleaning up flushing context");
			context.cleanUpFlushContext();
		}
	}

	@Override
	public V get(final K key, final ConsistencyLevel readLevel)
	{
		forceReadConsistencyLevel(readLevel);
		return reinitConsistencyLevel(new ThriftSafeExecutionContext<V>()
		{
			@Override
			public V execute()
			{
				return get(key);
			}
		});
	}

	@Override
	public void insert(final K key, final V value, final ConsistencyLevel writeLevel)
	{
		forceWriteConsistencyLevel(writeLevel);
		reinitConsistencyLevel(new ThriftSafeExecutionContext<Void>()
		{
			@Override
			public Void execute()
			{
				insert(key, value);
				return null;
			}
		});
	}

	@Override
	public void insert(final K key, final V value, final int ttl, final ConsistencyLevel writeLevel)
	{
		forceWriteConsistencyLevel(writeLevel);
		reinitConsistencyLevel(new ThriftSafeExecutionContext<Void>()
		{
			@Override
			public Void execute()
			{
				insert(key, value, ttl);
				return null;
			}
		});
	}

	@Override
	public KeyValue<K, V> findFirstMatching(final K key)
	{
		List<KeyValue<K, V>> found = find(key, key, 1, BoundingMode.INCLUSIVE_BOUNDS,
				OrderingMode.ASCENDING);

		return found.size() > 0 ? found.get(0) : null;
	}

	@Override
	public KeyValue<K, V> findFirstMatching(final K key, final ConsistencyLevel readLevel)
	{
		forceReadConsistencyLevel(readLevel);
		return reinitConsistencyLevel(new ThriftSafeExecutionContext<KeyValue<K, V>>()
		{
			@Override
			public KeyValue<K, V> execute()
			{
				return findFirstMatching(key);
			}
		});
	}

	@Override
	public KeyValue<K, V> findLastMatching(final K key)
	{
		List<KeyValue<K, V>> found = find(key, key, 1, BoundingMode.INCLUSIVE_BOUNDS,
				OrderingMode.DESCENDING);

		return found.size() > 0 ? found.get(0) : null;
	}

	@Override
	public KeyValue<K, V> findLastMatching(final K key, final ConsistencyLevel readLevel)
	{
		forceReadConsistencyLevel(readLevel);
		return reinitConsistencyLevel(new ThriftSafeExecutionContext<KeyValue<K, V>>()
		{
			@Override
			public KeyValue<K, V> execute()
			{
				return findLastMatching(key);
			}
		});
	}

	@Override
	public List<KeyValue<K, V>> find(final K start, final K end, final int count)
	{
		return find(start, end, count, BoundingMode.INCLUSIVE_BOUNDS, OrderingMode.ASCENDING);
	}

	@Override
	public List<KeyValue<K, V>> find(final K start, final K end, final int count,
			final ConsistencyLevel readLevel)
	{
		forceReadConsistencyLevel(readLevel);
		return reinitConsistencyLevel(new ThriftSafeExecutionContext<List<KeyValue<K, V>>>()
		{
			@Override
			public List<KeyValue<K, V>> execute()
			{
				return find(start, end, count, BoundingMode.INCLUSIVE_BOUNDS,
						OrderingMode.ASCENDING);
			}
		});

	}

	@Override
	public List<KeyValue<K, V>> find(final K start, final K end, final int count,
			final BoundingMode bounds, final OrderingMode ordering, final ConsistencyLevel readLevel)
	{
		forceReadConsistencyLevel(readLevel);
		return reinitConsistencyLevel(new ThriftSafeExecutionContext<List<KeyValue<K, V>>>()
		{
			@Override
			public List<KeyValue<K, V>> execute()
			{
				return find(start, end, count, bounds, ordering);
			}
		});
	}

	@Override
	public List<KeyValue<K, V>> findBoundsExclusive(final K start, final K end, final int count)
	{
		return find(start, end, count, BoundingMode.EXCLUSIVE_BOUNDS, OrderingMode.ASCENDING);
	}

	@Override
	public List<KeyValue<K, V>> findBoundsExclusive(final K start, final K end, final int count,
			final ConsistencyLevel readLevel)
	{
		forceReadConsistencyLevel(readLevel);
		return reinitConsistencyLevel(new ThriftSafeExecutionContext<List<KeyValue<K, V>>>()
		{
			@Override
			public List<KeyValue<K, V>> execute()
			{
				return find(start, end, count, BoundingMode.EXCLUSIVE_BOUNDS,
						OrderingMode.ASCENDING);
			}
		});

	}

	@Override
	public List<KeyValue<K, V>> findReverse(final K start, final K end, final int count)
	{
		return find(start, end, count, BoundingMode.INCLUSIVE_BOUNDS, OrderingMode.DESCENDING);
	}

	@Override
	public List<KeyValue<K, V>> findReverse(final K start, final K end, final int count,
			final ConsistencyLevel readLevel)
	{
		forceReadConsistencyLevel(readLevel);
		return reinitConsistencyLevel(new ThriftSafeExecutionContext<List<KeyValue<K, V>>>()
		{
			@Override
			public List<KeyValue<K, V>> execute()
			{
				return find(start, end, count, BoundingMode.INCLUSIVE_BOUNDS,
						OrderingMode.DESCENDING);
			}
		});
	}

	@Override
	public List<KeyValue<K, V>> findReverseBoundsExclusive(final K start, final K end,
			final int count)
	{
		return find(start, end, count, BoundingMode.EXCLUSIVE_BOUNDS, OrderingMode.DESCENDING);
	}

	@Override
	public List<KeyValue<K, V>> findReverseBoundsExclusive(final K start, final K end,
			final int count, final ConsistencyLevel readLevel)
	{
		forceReadConsistencyLevel(readLevel);
		return reinitConsistencyLevel(new ThriftSafeExecutionContext<List<KeyValue<K, V>>>()
		{
			@Override
			public List<KeyValue<K, V>> execute()
			{
				return find(start, end, count, BoundingMode.EXCLUSIVE_BOUNDS,
						OrderingMode.DESCENDING);
			}
		});
	}

	@Override
	public KeyValue<K, V> findFirst()
	{
		List<KeyValue<K, V>> result = this.find(null, null, 1);

		KeyValue<K, V> keyValue = null;
		if (result.size() > 0)
		{
			keyValue = result.get(0);
		}

		return keyValue;
	}

	@Override
	public KeyValue<K, V> findFirst(final ConsistencyLevel readLevel)
	{
		forceReadConsistencyLevel(readLevel);
		return reinitConsistencyLevel(new ThriftSafeExecutionContext<KeyValue<K, V>>()
		{
			@Override
			public KeyValue<K, V> execute()
			{
				return findFirst();
			}
		});
	}

	@Override
	public List<KeyValue<K, V>> findFirst(final int count)
	{
		return find(null, null, count);
	}

	@Override
	public List<KeyValue<K, V>> findFirst(final int count, final ConsistencyLevel readLevel)
	{
		forceReadConsistencyLevel(readLevel);
		return reinitConsistencyLevel(new ThriftSafeExecutionContext<List<KeyValue<K, V>>>()
		{
			@Override
			public List<KeyValue<K, V>> execute()
			{
				return find(null, null, count);
			}
		});
	}

	@Override
	public KeyValue<K, V> findLast()
	{
		List<KeyValue<K, V>> result = this.findReverse(null, null, 1);

		KeyValue<K, V> keyValue = null;
		if (result.size() > 0)
		{
			keyValue = result.get(0);
		}

		return keyValue;
	}

	@Override
	public KeyValue<K, V> findLast(final ConsistencyLevel readLevel)
	{
		forceReadConsistencyLevel(readLevel);
		return reinitConsistencyLevel(new ThriftSafeExecutionContext<KeyValue<K, V>>()
		{
			@Override
			public KeyValue<K, V> execute()
			{
				return findLast();
			}
		});
	}

	@Override
	public List<KeyValue<K, V>> findLast(final int count)
	{
		return findReverse(null, null, count);
	}

	@Override
	public List<KeyValue<K, V>> findLast(final int count, final ConsistencyLevel readLevel)
	{
		forceReadConsistencyLevel(readLevel);
		return reinitConsistencyLevel(new ThriftSafeExecutionContext<List<KeyValue<K, V>>>()
		{
			@Override
			public List<KeyValue<K, V>> execute()
			{
				return findReverse(null, null, count);
			}
		});
	}

	@Override
	public V findValue(final K key)
	{
		List<V> found = findValues(key, key, 1, BoundingMode.INCLUSIVE_BOUNDS,
				OrderingMode.ASCENDING);

		return found.size() > 0 ? found.get(0) : null;
	}

	@Override
	public V findValue(final K key, final ConsistencyLevel readLevel)
	{
		forceReadConsistencyLevel(readLevel);
		return reinitConsistencyLevel(new ThriftSafeExecutionContext<V>()
		{
			@Override
			public V execute()
			{
				return findValue(key);
			}
		});
	}

	@Override
	public List<V> findValues(final K start, final K end, final int count)
	{
		return findValues(start, end, count, BoundingMode.INCLUSIVE_BOUNDS, OrderingMode.ASCENDING);
	}

	@Override
	public List<V> findValues(final K start, final K end, final int count,
			final ConsistencyLevel readLevel)
	{
		forceReadConsistencyLevel(readLevel);
		return reinitConsistencyLevel(new ThriftSafeExecutionContext<List<V>>()
		{
			@Override
			public List<V> execute()
			{
				return findValues(start, end, count, BoundingMode.INCLUSIVE_BOUNDS,
						OrderingMode.ASCENDING);
			}
		});
	}

	@Override
	public List<V> findValues(final K start, final K end, final int count,
			final BoundingMode bounds, final OrderingMode ordering, final ConsistencyLevel readLevel)
	{
		forceReadConsistencyLevel(readLevel);
		return reinitConsistencyLevel(new ThriftSafeExecutionContext<List<V>>()
		{
			@Override
			public List<V> execute()
			{
				return findValues(start, end, count, bounds, ordering);
			}
		});

	}

	@Override
	public List<V> findBoundsExclusiveValues(final K start, final K end, final int count)
	{
		return findValues(start, end, count, BoundingMode.EXCLUSIVE_BOUNDS, OrderingMode.ASCENDING);
	}

	@Override
	public List<V> findBoundsExclusiveValues(final K start, final K end, final int count,
			final ConsistencyLevel readLevel)
	{
		forceReadConsistencyLevel(readLevel);
		return reinitConsistencyLevel(new ThriftSafeExecutionContext<List<V>>()
		{
			@Override
			public List<V> execute()
			{
				return findValues(start, end, count, BoundingMode.EXCLUSIVE_BOUNDS,
						OrderingMode.ASCENDING);
			}
		});
	}

	@Override
	public List<V> findReverseBoundsExclusiveValues(final K start, final K end, final int count)
	{
		return findValues(start, end, count, BoundingMode.EXCLUSIVE_BOUNDS, OrderingMode.DESCENDING);
	}

	@Override
	public List<V> findReverseBoundsExclusiveValues(final K start, final K end, final int count,
			final ConsistencyLevel readLevel)
	{
		forceReadConsistencyLevel(readLevel);
		return reinitConsistencyLevel(new ThriftSafeExecutionContext<List<V>>()
		{
			@Override
			public List<V> execute()
			{
				return findValues(start, end, count, BoundingMode.EXCLUSIVE_BOUNDS,
						OrderingMode.DESCENDING);
			}
		});
	}

	@Override
	public List<V> findReverseValues(final K start, final K end, final int count)
	{
		return findValues(start, end, count, BoundingMode.INCLUSIVE_BOUNDS, OrderingMode.DESCENDING);
	}

	@Override
	public List<V> findReverseValues(final K start, final K end, final int count,
			final ConsistencyLevel readLevel)
	{
		forceReadConsistencyLevel(readLevel);
		return reinitConsistencyLevel(new ThriftSafeExecutionContext<List<V>>()
		{
			@Override
			public List<V> execute()
			{
				return findValues(start, end, count, BoundingMode.INCLUSIVE_BOUNDS,
						OrderingMode.DESCENDING);
			}
		});
	}

	@Override
	public V findFirstValue()
	{
		List<V> result = this.findValues(null, null, 1);

		V value = null;
		if (result.size() > 0)
		{
			value = result.get(0);
		}

		return value;
	}

	@Override
	public V findFirstValue(final ConsistencyLevel readLevel)
	{
		forceReadConsistencyLevel(readLevel);
		return reinitConsistencyLevel(new ThriftSafeExecutionContext<V>()
		{
			@Override
			public V execute()
			{
				return findFirstValue();
			}
		});
	}

	@Override
	public List<V> findFirstValues(final int count)
	{
		return findValues(null, null, count);
	}

	@Override
	public List<V> findFirstValues(final int count, final ConsistencyLevel readLevel)
	{
		forceReadConsistencyLevel(readLevel);
		return reinitConsistencyLevel(new ThriftSafeExecutionContext<List<V>>()
		{
			@Override
			public List<V> execute()
			{
				return findValues(null, null, count);
			}
		});
	}

	@Override
	public V findLastValue()
	{
		List<V> result = this.findReverseValues(null, null, 1);

		V value = null;
		if (result.size() > 0)
		{
			value = result.get(0);
		}

		return value;
	}

	@Override
	public V findLastValue(final ConsistencyLevel readLevel)
	{
		forceReadConsistencyLevel(readLevel);
		return reinitConsistencyLevel(new ThriftSafeExecutionContext<V>()
		{
			@Override
			public V execute()
			{
				return findLastValue();
			}
		});
	}

	@Override
	public List<V> findLastValues(final int count)
	{
		return findReverseValues(null, null, count);
	}

	@Override
	public List<V> findLastValues(final int count, final ConsistencyLevel readLevel)
	{
		forceReadConsistencyLevel(readLevel);
		return reinitConsistencyLevel(new ThriftSafeExecutionContext<List<V>>()
		{
			@Override
			public List<V> execute()
			{
				return findReverseValues(null, null, count);
			}
		});
	}

	@Override
	public K findKey(final K key)
	{
		List<K> found = findKeys(key, key, 1, BoundingMode.INCLUSIVE_BOUNDS, OrderingMode.ASCENDING);

		return found.size() > 0 ? found.get(0) : null;
	}

	@Override
	public K findKey(final K key, final ConsistencyLevel readLevel)
	{
		forceReadConsistencyLevel(readLevel);
		return reinitConsistencyLevel(new ThriftSafeExecutionContext<K>()
		{
			@Override
			public K execute()
			{
				return findKey(key);
			}
		});
	}

	@Override
	public List<K> findKeys(final K start, final K end, final int count)
	{
		return findKeys(start, end, count, BoundingMode.INCLUSIVE_BOUNDS, OrderingMode.ASCENDING);
	}

	@Override
	public List<K> findKeys(final K start, final K end, final int count,
			final ConsistencyLevel readLevel)
	{
		forceReadConsistencyLevel(readLevel);
		return reinitConsistencyLevel(new ThriftSafeExecutionContext<List<K>>()
		{
			@Override
			public List<K> execute()
			{
				return findKeys(start, end, count, BoundingMode.INCLUSIVE_BOUNDS,
						OrderingMode.ASCENDING);
			}
		});
	}

	@Override
	public List<K> findKeys(final K start, final K end, final int count, final BoundingMode bounds,
			final OrderingMode ordering, final ConsistencyLevel readLevel)
	{
		forceReadConsistencyLevel(readLevel);
		return reinitConsistencyLevel(new ThriftSafeExecutionContext<List<K>>()
		{
			@Override
			public List<K> execute()
			{
				return findKeys(start, end, count, bounds, ordering);
			}
		});
	}

	@Override
	public List<K> findBoundsExclusiveKeys(final K start, final K end, final int count)
	{
		return findKeys(start, end, count, BoundingMode.EXCLUSIVE_BOUNDS, OrderingMode.ASCENDING);
	}

	@Override
	public List<K> findBoundsExclusiveKeys(final K start, final K end, final int count,
			final ConsistencyLevel readLevel)
	{
		forceReadConsistencyLevel(readLevel);
		return reinitConsistencyLevel(new ThriftSafeExecutionContext<List<K>>()
		{
			@Override
			public List<K> execute()
			{
				return findKeys(start, end, count, BoundingMode.EXCLUSIVE_BOUNDS,
						OrderingMode.ASCENDING);
			}
		});
	}

	@Override
	public List<K> findReverseKeys(final K start, final K end, final int count)
	{
		return findKeys(start, end, count, BoundingMode.INCLUSIVE_BOUNDS, OrderingMode.DESCENDING);
	}

	@Override
	public List<K> findReverseKeys(final K start, final K end, final int count,
			final ConsistencyLevel readLevel)
	{
		forceReadConsistencyLevel(readLevel);
		return reinitConsistencyLevel(new ThriftSafeExecutionContext<List<K>>()
		{
			@Override
			public List<K> execute()
			{
				return findKeys(start, end, count, BoundingMode.INCLUSIVE_BOUNDS,
						OrderingMode.DESCENDING);
			}
		});
	}

	@Override
	public List<K> findReverseBoundsExclusiveKeys(final K start, final K end, final int count)
	{
		return findKeys(start, end, count, BoundingMode.EXCLUSIVE_BOUNDS, OrderingMode.DESCENDING);
	}

	@Override
	public List<K> findReverseBoundsExclusiveKeys(final K start, final K end, final int count,
			final ConsistencyLevel readLevel)
	{
		forceReadConsistencyLevel(readLevel);
		return reinitConsistencyLevel(new ThriftSafeExecutionContext<List<K>>()
		{
			@Override
			public List<K> execute()
			{
				return findKeys(start, end, count, BoundingMode.EXCLUSIVE_BOUNDS,
						OrderingMode.DESCENDING);
			}
		});
	}

	@Override
	public K findFirstKey()
	{
		List<K> result = this.findKeys(null, null, 1);

		K key = null;
		if (result.size() > 0)
		{
			key = result.get(0);
		}

		return key;
	}

	@Override
	public K findFirstKey(final ConsistencyLevel readLevel)
	{
		forceReadConsistencyLevel(readLevel);
		return reinitConsistencyLevel(new ThriftSafeExecutionContext<K>()
		{
			@Override
			public K execute()
			{
				return findFirstKey();
			}
		});
	}

	@Override
	public List<K> findFirstKeys(final int count)
	{
		return findKeys(null, null, count);
	}

	@Override
	public List<K> findFirstKeys(final int count, final ConsistencyLevel readLevel)
	{
		forceReadConsistencyLevel(readLevel);
		return reinitConsistencyLevel(new ThriftSafeExecutionContext<List<K>>()
		{
			@Override
			public List<K> execute()
			{
				return findKeys(null, null, count);
			}
		});
	}

	@Override
	public K findLastKey()
	{
		List<K> result = this.findReverseKeys(null, null, 1);

		K key = null;
		if (result.size() > 0)
		{
			key = result.get(0);
		}

		return key;
	}

	@Override
	public K findLastKey(final ConsistencyLevel readLevel)
	{
		forceReadConsistencyLevel(readLevel);
		return reinitConsistencyLevel(new ThriftSafeExecutionContext<K>()
		{
			@Override
			public K execute()
			{
				return findLastKey();
			}
		});
	}

	@Override
	public List<K> findLastKeys(final int count)
	{
		return findReverseKeys(null, null, count);
	}

	@Override
	public List<K> findLastKeys(final int count, final ConsistencyLevel readLevel)
	{
		forceReadConsistencyLevel(readLevel);
		return reinitConsistencyLevel(new ThriftSafeExecutionContext<List<K>>()
		{
			@Override
			public List<K> execute()
			{
				return findReverseKeys(null, null, count);
			}
		});
	}

	@Override
	public KeyValueIterator<K, V> iterator()
	{
		return iterator(null, null, DEFAULT_COUNT, BoundingMode.INCLUSIVE_BOUNDS,
				OrderingMode.ASCENDING);
	}

	@Override
	public KeyValueIterator<K, V> iterator(final ConsistencyLevel readLevel)
	{
		forceReadConsistencyLevel(readLevel);
		return reinitConsistencyLevel(new ThriftSafeExecutionContext<KeyValueIterator<K, V>>()
		{
			@Override
			public KeyValueIterator<K, V> execute()
			{
				return iterator(null, null, DEFAULT_COUNT, BoundingMode.INCLUSIVE_BOUNDS,
						OrderingMode.ASCENDING);
			}
		});
	}

	@Override
	public KeyValueIterator<K, V> iterator(final K start, final K end, final int count,
			final BoundingMode bounds, final OrderingMode ordering, final ConsistencyLevel readLevel)
	{
		forceReadConsistencyLevel(readLevel);
		return reinitConsistencyLevel(new ThriftSafeExecutionContext<KeyValueIterator<K, V>>()
		{
			@Override
			public KeyValueIterator<K, V> execute()
			{
				return iterator(null, null, count, bounds, ordering);
			}
		});
	}

	@Override
	public KeyValueIterator<K, V> iterator(final int count)
	{
		return iterator(null, null, count, BoundingMode.INCLUSIVE_BOUNDS, OrderingMode.ASCENDING);
	}

	@Override
	public KeyValueIterator<K, V> iterator(final int count, final ConsistencyLevel readLevel)
	{
		forceReadConsistencyLevel(readLevel);
		return reinitConsistencyLevel(new ThriftSafeExecutionContext<KeyValueIterator<K, V>>()
		{
			@Override
			public KeyValueIterator<K, V> execute()
			{
				return iterator(null, null, count, BoundingMode.INCLUSIVE_BOUNDS,
						OrderingMode.ASCENDING);
			}
		});
	}

	@Override
	public KeyValueIterator<K, V> iterator(final K start, final K end, final int count)
	{
		return iterator(start, end, count, BoundingMode.INCLUSIVE_BOUNDS, OrderingMode.ASCENDING);
	}

	@Override
	public KeyValueIterator<K, V> iterator(final K start, final K end, final int count,
			final ConsistencyLevel readLevel)
	{
		forceReadConsistencyLevel(readLevel);
		return reinitConsistencyLevel(new ThriftSafeExecutionContext<KeyValueIterator<K, V>>()
		{
			@Override
			public KeyValueIterator<K, V> execute()
			{
				return iterator(start, end, count, BoundingMode.INCLUSIVE_BOUNDS,
						OrderingMode.ASCENDING);
			}
		});
	}

	@Override
	public KeyValueIterator<K, V> iteratorBoundsExclusive(final K start, final K end,
			final int count)
	{
		return iterator(start, end, count, BoundingMode.EXCLUSIVE_BOUNDS, OrderingMode.ASCENDING);
	}

	@Override
	public KeyValueIterator<K, V> iteratorBoundsExclusive(final K start, final K end,
			final int count, final ConsistencyLevel readLevel)
	{
		forceReadConsistencyLevel(readLevel);
		return reinitConsistencyLevel(new ThriftSafeExecutionContext<KeyValueIterator<K, V>>()
		{
			@Override
			public KeyValueIterator<K, V> execute()
			{
				return iterator(start, end, count, BoundingMode.EXCLUSIVE_BOUNDS,
						OrderingMode.ASCENDING);
			}
		});
	}

	@Override
	public KeyValueIterator<K, V> iteratorReverse()
	{
		return iterator(null, null, DEFAULT_COUNT, BoundingMode.INCLUSIVE_BOUNDS,
				OrderingMode.DESCENDING);
	}

	@Override
	public KeyValueIterator<K, V> iteratorReverse(final ConsistencyLevel readLevel)
	{
		forceReadConsistencyLevel(readLevel);
		return reinitConsistencyLevel(new ThriftSafeExecutionContext<KeyValueIterator<K, V>>()
		{
			@Override
			public KeyValueIterator<K, V> execute()
			{
				return iterator(null, null, DEFAULT_COUNT, BoundingMode.INCLUSIVE_BOUNDS,
						OrderingMode.DESCENDING);
			}
		});
	}

	@Override
	public KeyValueIterator<K, V> iteratorReverse(final int count)
	{
		return iterator(null, null, count, BoundingMode.INCLUSIVE_BOUNDS, OrderingMode.DESCENDING);
	}

	@Override
	public KeyValueIterator<K, V> iteratorReverse(final int count, final ConsistencyLevel readLevel)
	{
		forceReadConsistencyLevel(readLevel);
		return reinitConsistencyLevel(new ThriftSafeExecutionContext<KeyValueIterator<K, V>>()
		{
			@Override
			public KeyValueIterator<K, V> execute()
			{
				return iterator(null, null, count, BoundingMode.INCLUSIVE_BOUNDS,
						OrderingMode.DESCENDING);
			}
		});
	}

	@Override
	public KeyValueIterator<K, V> iteratorReverse(final K start, final K end, final int count)
	{
		return iterator(start, end, count, BoundingMode.INCLUSIVE_BOUNDS, OrderingMode.DESCENDING);
	}

	@Override
	public KeyValueIterator<K, V> iteratorReverse(final K start, final K end, final int count,
			final ConsistencyLevel readLevel)
	{
		forceReadConsistencyLevel(readLevel);
		return reinitConsistencyLevel(new ThriftSafeExecutionContext<KeyValueIterator<K, V>>()
		{
			@Override
			public KeyValueIterator<K, V> execute()
			{
				return iterator(start, end, count, BoundingMode.INCLUSIVE_BOUNDS,
						OrderingMode.DESCENDING);
			}
		});
	}

	@Override
	public KeyValueIterator<K, V> iteratorReverseBoundsExclusive(final K start, final K end,
			final int count)
	{
		return iterator(start, end, count, BoundingMode.EXCLUSIVE_BOUNDS, OrderingMode.DESCENDING);
	}

	@Override
	public KeyValueIterator<K, V> iteratorReverseBoundsExclusive(final K start, final K end,
			final int count, final ConsistencyLevel readLevel)
	{
		forceReadConsistencyLevel(readLevel);
		return reinitConsistencyLevel(new ThriftSafeExecutionContext<KeyValueIterator<K, V>>()
		{
			@Override
			public KeyValueIterator<K, V> execute()
			{
				return iterator(start, end, count, BoundingMode.EXCLUSIVE_BOUNDS,
						OrderingMode.DESCENDING);
			}
		});
	}

	@Override
	public void remove(final K key, final ConsistencyLevel writeLevel)
	{
		forceWriteConsistencyLevel(writeLevel);
		reinitConsistencyLevel(new ThriftSafeExecutionContext<Void>()
		{
			@Override
			public Void execute()
			{
				remove(key);
				return null;
			}
		});

	}

	@Override
	public void remove(final K start, final K end)
	{
		remove(start, end, BoundingMode.INCLUSIVE_BOUNDS);
	}

	@Override
	public void remove(final K start, final K end, final ConsistencyLevel writeLevel)
	{
		forceWriteConsistencyLevel(writeLevel);
		reinitConsistencyLevel(new ThriftSafeExecutionContext<Void>()
		{
			@Override
			public Void execute()
			{
				remove(start, end, BoundingMode.INCLUSIVE_BOUNDS);
				return null;
			}
		});
	}

	@Override
	public void remove(final K start, final K end, final BoundingMode bounds,
			final ConsistencyLevel writeLevel)
	{
		forceWriteConsistencyLevel(writeLevel);
		reinitConsistencyLevel(new ThriftSafeExecutionContext<Void>()
		{
			@Override
			public Void execute()
			{
				remove(start, end, bounds);
				return null;
			}
		});
	}

	@Override
	public void removeBoundsExclusive(final K start, final K end)
	{
		remove(start, end, BoundingMode.EXCLUSIVE_BOUNDS);
	}

	@Override
	public void removeBoundsExclusive(final K start, final K end, final ConsistencyLevel writeLevel)
	{
		forceWriteConsistencyLevel(writeLevel);
		reinitConsistencyLevel(new ThriftSafeExecutionContext<Void>()
		{
			@Override
			public Void execute()
			{
				remove(start, end, BoundingMode.EXCLUSIVE_BOUNDS);
				return null;
			}
		});
	}

	@Override
	public void removeFirst()
	{
		removeFirst(1);
	}

	@Override
	public void removeFirst(final ConsistencyLevel writeLevel)
	{
		forceWriteConsistencyLevel(writeLevel);
		reinitConsistencyLevel(new ThriftSafeExecutionContext<Void>()
		{
			@Override
			public Void execute()
			{
				removeFirst(1);
				return null;
			}
		});
	}

	@Override
	public void removeFirst(final int count, final ConsistencyLevel writeLevel)
	{
		forceWriteConsistencyLevel(writeLevel);
		reinitConsistencyLevel(new ThriftSafeExecutionContext<Void>()
		{
			@Override
			public Void execute()
			{
				removeFirst(count);
				return null;
			}
		});
	}

	@Override
	public void removeLast()
	{
		removeLast(1);
	}

	@Override
	public void removeLast(final ConsistencyLevel writeLevel)
	{
		forceWriteConsistencyLevel(writeLevel);
		reinitConsistencyLevel(new ThriftSafeExecutionContext<Void>()
		{
			@Override
			public Void execute()
			{
				removeLast(1);
				return null;
			}
		});
	}

	@Override
	public void removeLast(final int count, final ConsistencyLevel writeLevel)
	{
		forceWriteConsistencyLevel(writeLevel);
		reinitConsistencyLevel(new ThriftSafeExecutionContext<Void>()
		{
			@Override
			public Void execute()
			{
				removeLast(count);
				return null;
			}
		});
	}

	private void forceReadConsistencyLevel(final ConsistencyLevel readLevel)
	{
		log.trace("Execute read operation with consistency level {}", readLevel.name());
		ensureNoPendingBatch();
		context.setReadConsistencyLevel(readLevel);
	}

	private void forceWriteConsistencyLevel(final ConsistencyLevel writeLevel)
	{
		log.trace("Execute write operation with consistency level {}", writeLevel.name());
		ensureNoPendingBatch();
		context.setWriteConsistencyLevel(writeLevel);
	}

	private void ensureNoPendingBatch() throws Error
	{
		try
		{
			validator.validateNoPendingBatch(context);
		}
		catch (RuntimeException e)
		{
			log.trace("Cleaning up flushing context");
			context.cleanUpFlushContext();
			throw e;
		}
		catch (Error e)
		{
			log.trace("Cleaning up flushing context");
			context.cleanUpFlushContext();
			throw e;
		}
	}

	public ThriftEntityInterceptor<?> getInterceptor()
	{
		return interceptor;
	}

	public void setInterceptor(final ThriftEntityInterceptor<?> interceptor)
	{
		this.interceptor = interceptor;
	}

	public void setContext(final ThriftPersistenceContext context)
	{
		this.context = context;
	}
}
