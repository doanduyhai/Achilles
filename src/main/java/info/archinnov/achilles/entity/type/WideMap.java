package info.archinnov.achilles.entity.type;

import java.util.List;

/**
 * WideMap
 * 
 * @author DuyHai DOAN
 * 
 */
public interface WideMap<K, V> {

    public enum BoundingMode {
        INCLUSIVE_BOUNDS(true, true), EXCLUSIVE_BOUNDS(false, false), INCLUSIVE_START_BOUND_ONLY(true, false), INCLUSIVE_END_BOUND_ONLY(
                false, true);

        private boolean inclusiveStart;
        private boolean inclusiveEnd;

        private BoundingMode(boolean inclusiveStart, boolean inclusiveEnd) {
            this.inclusiveStart = inclusiveStart;
            this.inclusiveEnd = inclusiveEnd;
        }

        public boolean isInclusiveStart() {
            return inclusiveStart;
        }

        public boolean isInclusiveEnd() {
            return inclusiveEnd;
        }
    }

    public enum OrderingMode {
        DESCENDING(true), ASCENDING(false);

        private boolean reverse;

        private OrderingMode(boolean equivalent) {
            this.reverse = equivalent;
        }

        public boolean isReverse() {
            return reverse;
        }
    }

    /**
     * Insert a new value with ttl
     * 
     * @param key
     *            Search key. Can be a multi key
     * @param value
     *            Value
     * @param ttl
     *            Time to live in seconds
     */
    public void insert(K key, V value, int ttl);

    /**
     * Insert a new value with ttl with the given Consistency Level for write
     * 
     * @param key
     *            Search key. Can be a multi key
     * @param value
     *            Value
     * @param ttl
     *            Time to live in seconds
     * @param writeLevel
     *            Consistency Level for write
     */
    public void insert(K key, V value, int ttl, ConsistencyLevel writeLevel);

    /**
     * Insert a new value
     * 
     * @param key
     *            Search key. Can be a multi key
     * @param value
     *            Value
     */
    public void insert(K key, V value);

    /**
     * Insert a new value with the given Consistency Level for write
     * 
     * @param key
     *            Search key. Can be a multi key
     * @param value
     *            Value
     * @param writeLevel
     *            Consistency Level for write
     */
    public void insert(K key, V value, ConsistencyLevel writeLevel);

    /**
     * Find a value with a key
     * 
     * @param key
     *            Search key. Can be a multi key
     * @return value Found value or null
     */
    public V get(K key);

    /**
     * Find a value with a key with the given Consistency Level for read
     * 
     * @param key
     *            Search key. Can be a multi key
     * @param readLevel
     *            Consistency Level for read
     * @return value Found value or null
     */
    public V get(K key, ConsistencyLevel readLevel);

    /**
     * Find first matching key/value pair with given key, bounds inclusive
     * 
     * @param key
     *            key
     * 
     * @return matching first key/value pair
     * 
     *         This method is a shorthand for find(K key,K key, 1);
     */
    public KeyValue<K, V> findFirstMatching(K key);

    /**
     * Find first matching key/value pair with given key, bounds inclusive and consistency level
     * 
     * @param key
     *            key
     * @param readLevel
     *            Consistency Level for read
     * 
     * @return matching first key/value pair
     * 
     *         This method is a shorthand for find(K key,K key, 1);
     */
    public KeyValue<K, V> findFirstMatching(K key, ConsistencyLevel readLevel);

    /**
     * Find last matching key/value pair with given key, bounds inclusive
     * 
     * @param key
     *            key
     * 
     * @return matching last key/value pair
     * 
     *         This method is a shorthand for findReverse(K key,K key, 1);
     */
    public KeyValue<K, V> findLastMatching(K key);

    /**
     * Find last matching key/value pair with given key, bounds inclusive and consistency level
     * 
     * @param key
     *            key
     * @param readLevel
     *            Consistency Level for read
     * 
     * @return matching last key/value pair
     * 
     *         This method is a shorthand for findReverse(K key,K key, 1);
     */
    public KeyValue<K, V> findLastMatching(K key, ConsistencyLevel readLevel);

    /**
     * Find a range of key/value, bounds inclusive
     * 
     * @param start
     *            Start key, inclusive
     * @param end
     *            End key, inclusive. Should be less than start key with respect to the default comparator
     * @param count
     *            Maximum number of key/value pairs to be fetched
     * @return List of key/value pairs
     */
    public List<KeyValue<K, V>> find(K start, K end, int count);

    /**
     * Find a range of key/value, bounds inclusive, with the given Consistency Level for read
     * 
     * @param start
     *            Start key, inclusive
     * @param end
     *            End key, inclusive. Should be less than start key with respect to the default comparator
     * @param count
     *            Maximum number of key/value pairs to be fetched
     * @param readLevel
     *            Consistency Level for read
     * @return List of key/value pairs
     */
    public List<KeyValue<K, V>> find(K start, K end, int count, ConsistencyLevel readLevel);

    /**
     * Find a range of key/value, bounds exclusive
     * 
     * @param start
     *            Start key, exclusive
     * @param end
     *            End key, exclusive. Should be less than start key with respect to the default comparator
     * @param count
     *            Maximum number of key/value pairs to be fetched
     * @return List of key/value pairs
     */
    public List<KeyValue<K, V>> findBoundsExclusive(K start, K end, int count);

    /**
     * Find a range of key/value, bounds exclusive, with the given Consistency Level for read
     * 
     * @param start
     *            Start key, exclusive
     * @param end
     *            End key, exclusive. Should be less than start key with respect to the default comparator
     * @param count
     *            Maximum number of key/value pairs to be fetched
     * @param readLevel
     *            Consistency Level for read
     * @return List of key/value pairs
     */
    public List<KeyValue<K, V>> findBoundsExclusive(K start, K end, int count, ConsistencyLevel readLevel);

    /**
     * Find a range of key/value, bounds inclusive, in reversed order
     * 
     * @param start
     *            Start key, inclusive
     * @param end
     *            End key, inclusive. Should be greater than start key with respect to the default comparator
     * @param count
     *            Maximum number of key/value pairs to be fetched
     * @return List of key/value pairs
     */
    public List<KeyValue<K, V>> findReverse(K start, K end, int count);

    /**
     * Find a range of key/value, bounds inclusive, in reversed order, with the given Consistency Level for read
     * 
     * @param start
     *            Start key, inclusive
     * @param end
     *            End key, inclusive. Should be greater than start key with respect to the default comparator
     * @param count
     *            Maximum number of key/value pairs to be fetched
     * @param readLevel
     *            Consistency Level for read
     * @return List of key/value pairs
     */
    public List<KeyValue<K, V>> findReverse(K start, K end, int count, ConsistencyLevel readLevel);

    /**
     * Find a range of key/value, bounds exclusive, in reversed order
     * 
     * @param start
     *            Start key, exclusive
     * @param end
     *            End key, exclusive. Should be greater than start key with respect to the default comparator
     * @param count
     *            Maximum number of key/value pairs to be fetched
     * @return List of key/value pairs
     */
    public List<KeyValue<K, V>> findReverseBoundsExclusive(K start, K end, int count);

    /**
     * Find a range of key/value, bounds exclusive, in reversed order, with the given Consistency Level for read
     * 
     * @param start
     *            Start key, exclusive
     * @param end
     *            End key, exclusive. Should be greater than start key with respect to the default comparator
     * @param count
     *            Maximum number of key/value pairs to be fetched
     * @param readLevel
     *            Consistency Level for read
     * @return List of key/value pairs
     */
    public List<KeyValue<K, V>> findReverseBoundsExclusive(K start, K end, int count, ConsistencyLevel readLevel);

    /**
     * Find a range of key/value
     * 
     * @param start
     *            Start key
     * @param end
     *            End key. Should be less/greater than start key depending on the reverse flag
     * @param count
     *            Maximum number of key/value pairs to be fetched
     * @param bounds
     *            Bounds specified mode
     * @param ordering
     *            Order specified mode
     * @return List of key/value pairs
     */
    public List<KeyValue<K, V>> find(K start, K end, int count, BoundingMode bounds, OrderingMode ordering);

    /**
     * Find a range of key/value, with the given Consistency Level for read
     * 
     * @param start
     *            Start key
     * @param end
     *            End key. Should be less/greater than start key depending on the reverse flag
     * @param count
     *            Maximum number of key/value pairs to be fetched
     * @param bounds
     *            Bounds specified mode
     * @param ordering
     *            Order specified mode
     * @param readLevel
     *            Consistency Level for read
     * @return List of key/value pairs
     */
    public List<KeyValue<K, V>> find(K start, K end, int count, BoundingMode bounds, OrderingMode ordering,
            ConsistencyLevel readLevel);

    /**
     * Find first pair of key/value, normal order
     * 
     * @return First key/value pair
     */
    public KeyValue<K, V> findFirst();

    /**
     * Find first pair of key/value, normal order, with the given Consistency Level for read
     * 
     * @param readLevel
     *            Consistency Level for read
     * @return First key/value pair
     */
    public KeyValue<K, V> findFirst(ConsistencyLevel readLevel);

    /**
     * Find maximum n first pairs of key/value, normal order
     * 
     * @param count
     *            Number of first key/value pairs to be fetched
     * @return n first key/value pairs (or less)
     */
    public List<KeyValue<K, V>> findFirst(int count);

    /**
     * Find maximum n first pairs of key/value, normal order, with the given Consistency Level for read
     * 
     * @param count
     *            Number of first key/value pairs to be fetched
     * @param readLevel
     *            Consistency Level for read
     * @return n first key/value pairs (or less)
     */
    public List<KeyValue<K, V>> findFirst(int count, ConsistencyLevel readLevel);

    /**
     * Find last pair of key/value, normal order
     * 
     * @return Last key/value pair
     */
    public KeyValue<K, V> findLast();

    /**
     * Find last pair of key/value, normal order, with the given Consistency Level for read
     * 
     * @param readLevel
     *            Consistency Level for read
     * @return Last key/value pair
     */
    public KeyValue<K, V> findLast(ConsistencyLevel readLevel);

    /**
     * Find maximum n last pairs of key/value, normal order
     * 
     * @param count
     *            Number of last key/value pairs to be fetched
     * @return n last key/value pairs (or less)
     */
    public List<KeyValue<K, V>> findLast(int count);

    /**
     * Find maximum n last pairs of key/value, normal order, with the given Consistency Level for read
     * 
     * @param count
     *            Number of last key/value pairs to be fetched
     * @param readLevel
     *            Consistency Level for read
     * @return n last key/value pairs (or less)
     */
    public List<KeyValue<K, V>> findLast(int count, ConsistencyLevel readLevel);

    /**
     * Find a value with given key, bounds inclusive
     * 
     * @param key
     *            key for searching
     * 
     * @return matching first value
     * 
     *         This method is a shorthand for findValues(K key,K key, 1);
     */
    public V findValue(K key);

    /**
     * Find a value with given key, bounds inclusive
     * 
     * @param key
     *            key for searching
     * 
     * @param readLevel
     *            Consistency Level for read
     * 
     * @return matching first value
     * 
     *         This method is a shorthand for findValues(K key,K key, 1);
     */
    public V findValue(K key, ConsistencyLevel readLevel);

    /**
     * Find a range of value, bounds inclusive
     * 
     * @param start
     *            Start key, inclusive
     * @param end
     *            End key, inclusive. Should be less than start key with respect to the default comparator
     * @param count
     *            Maximum number of values to be fetched
     * @return List of values
     */
    public List<V> findValues(K start, K end, int count);

    /**
     * Find a range of value, bounds inclusive, with the given Consistency Level for read
     * 
     * @param start
     *            Start key, inclusive
     * @param end
     *            End key, inclusive. Should be less than start key with respect to the default comparator
     * @param count
     *            Maximum number of values to be fetched
     * @param readLevel
     *            Consistency Level for read
     * @return List of values
     */
    public List<V> findValues(K start, K end, int count, ConsistencyLevel readLevel);

    /**
     * Find a range of value, bounds exclusive
     * 
     * @param start
     *            Start key, inclusive
     * @param end
     *            End key, inclusive. Should be less than start key with respect to the default comparator
     * @param count
     *            Maximum number of values to be fetched
     * @return List of values
     */
    public List<V> findBoundsExclusiveValues(K start, K end, int count);

    /**
     * Find a range of value, bounds exclusive, with the given Consistency Level for read
     * 
     * @param start
     *            Start key, inclusive
     * @param end
     *            End key, inclusive. Should be less than start key with respect to the default comparator
     * @param count
     *            Maximum number of values to be fetched
     * @param readLevel
     *            Consistency Level for read
     * @return List of values
     */
    public List<V> findBoundsExclusiveValues(K start, K end, int count, ConsistencyLevel readLevel);

    /**
     * Find a range of value, bounds inclusive, in reversed order
     * 
     * @param start
     *            Start key, inclusive
     * @param end
     *            End key, inclusive. Should be greater than start key with respect to the default comparator
     * @param count
     *            Maximum number of values to be fetched
     * @return List of values
     */
    public List<V> findReverseValues(K start, K end, int count);

    /**
     * Find a range of value, bounds inclusive, in reversed order, with the given Consistency Level for read
     * 
     * @param start
     *            Start key, inclusive
     * @param end
     *            End key, inclusive. Should be greater than start key with respect to the default comparator
     * @param count
     *            Maximum number of values to be fetched
     * @param readLevel
     *            Consistency Level for read
     * @return List of values
     */
    public List<V> findReverseValues(K start, K end, int count, ConsistencyLevel readLevel);

    /**
     * Find a range of value, bounds exclusive, in reversed order
     * 
     * @param start
     *            Start key, exclusive
     * @param end
     *            End key, exclusive. Should be greater than start key with respect to the default comparator
     * @param count
     *            Maximum number of values to be fetched
     * @return List of values
     */
    public List<V> findReverseBoundsExclusiveValues(K start, K end, int count);

    /**
     * Find a range of value, bounds exclusive, in reversed order, with the given Consistency Level for read
     * 
     * @param start
     *            Start key, exclusive
     * @param end
     *            End key, exclusive. Should be greater than start key with respect to the default comparator
     * @param count
     *            Maximum number of values to be fetched
     * @param readLevel
     *            Consistency Level for read
     * @return List of values
     */
    public List<V> findReverseBoundsExclusiveValues(K start, K end, int count, ConsistencyLevel readLevel);

    /**
     * Find a range of value
     * 
     * @param start
     *            Start key
     * @param end
     *            End key. Should be less/greater than start key depending on the reverse flag
     * @param count
     *            Maximum number of values to be fetched
     * @param bounds
     *            Bounds specified mode
     * @param ordering
     *            Order specified mode
     * @return List of values
     */
    public List<V> findValues(K start, K end, int count, BoundingMode bounds, OrderingMode ordering);

    /**
     * Find a range of value, with the given Consistency Level for read
     * 
     * @param start
     *            Start key
     * @param end
     *            End key. Should be less/greater than start key depending on the reverse flag
     * @param count
     *            Maximum number of values to be fetched
     * @param bounds
     *            Bounds specified mode
     * @param ordering
     *            Order specified mode
     * @param readLevel
     *            Consistency Level for read
     * @return List of values
     */
    public List<V> findValues(K start, K end, int count, BoundingMode bounds, OrderingMode ordering,
            ConsistencyLevel readLevel);

    /**
     * Find first value, normal order
     * 
     * @return First value
     */
    public V findFirstValue();

    /**
     * Find first value, normal order, with the given Consistency Level for read
     * 
     * @param readLevel
     *            Consistency Level for read
     * @return First value
     */
    public V findFirstValue(ConsistencyLevel readLevel);

    /**
     * Find maximum n first values, normal order
     * 
     * @param count
     *            Number of first values to be fetched
     * @return n first values (or less)
     */
    public List<V> findFirstValues(int count);

    /**
     * Find maximum n first values, normal order, with the given Consistency Level for read
     * 
     * @param count
     *            Number of first values to be fetched
     * @param readLevel
     *            Consistency Level for read
     * @return n first values (or less)
     */
    public List<V> findFirstValues(int count, ConsistencyLevel readLevel);

    /**
     * Find last value, normal order
     * 
     * @return Last value
     */
    public V findLastValue();

    /**
     * Find last value, normal order, with the given Consistency Level for read
     * 
     * @param readLevel
     *            Consistency Level for read
     * @return Last value
     */
    public V findLastValue(ConsistencyLevel readLevel);

    /**
     * Find maximum n last values, normal order
     * 
     * @param count
     *            Number of last values to be fetched
     * @return n last values (or less)
     */
    public List<V> findLastValues(int count);

    /**
     * Find maximum n last values, normal order, with the given Consistency Level for read
     * 
     * @param count
     *            Number of last values to be fetched
     * @param readLevel
     *            Consistency Level for read
     * @return n last values (or less)
     */
    public List<V> findLastValues(int count, ConsistencyLevel readLevel);

    /**
     * Find a key with given key, bounds inclusive
     * 
     * @param key
     *            key
     * 
     * @return matching first key
     * 
     *         This method is a shorthand for findKeys(K key,K key, 1);
     */
    public K findKey(K key);

    /**
     * Find a key with given key, bounds inclusive
     * 
     * @param key
     *            key
     * 
     * @param readLevel
     *            Consistency Level for read
     * 
     * @return matching first key
     * 
     *         This method is a shorthand for findKeys(K key,K key, 1);
     */
    public K findKey(K key, ConsistencyLevel readLevel);

    /**
     * Find a range of key, bounds inclusive
     * 
     * @param start
     *            Start key, inclusive
     * @param end
     *            End key, inclusive. Should be less than start key with respect to the default comparator
     * @param count
     *            Maximum number of keys to be fetched
     * @return List of keys
     */
    public List<K> findKeys(K start, K end, int count);

    /**
     * Find a range of key, bounds inclusive, with the given Consistency Level for read
     * 
     * @param start
     *            Start key, inclusive
     * @param end
     *            End key, inclusive. Should be less than start key with respect to the default comparator
     * @param count
     *            Maximum number of keys to be fetched
     * @param readLevel
     *            Consistency Level for read
     * @return List of keys
     */
    public List<K> findKeys(K start, K end, int count, ConsistencyLevel readLevel);

    /**
     * Find a range of key, bounds exclusive
     * 
     * @param start
     *            Start key, inclusive
     * @param end
     *            End key, inclusive. Should be less than start key with respect to the default comparator
     * @param count
     *            Maximum number of keys to be fetched
     * @return List of keys
     */
    public List<K> findBoundsExclusiveKeys(K start, K end, int count);

    /**
     * Find a range of key, bounds exclusive, with the given Consistency Level for read
     * 
     * @param start
     *            Start key, inclusive
     * @param end
     *            End key, inclusive. Should be less than start key with respect to the default comparator
     * @param count
     *            Maximum number of keys to be fetched
     * @param readLevel
     *            Consistency Level for read
     * @return List of keys
     */
    public List<K> findBoundsExclusiveKeys(K start, K end, int count, ConsistencyLevel readLevel);

    /**
     * Find a range of key, bounds inclusive, in reversed order
     * 
     * @param start
     *            Start key, inclusive
     * @param end
     *            End key, inclusive. Should be greater than start key with respect to the default comparator
     * @param count
     *            Maximum number of keys to be fetched
     * @return List of keys
     */
    public List<K> findReverseKeys(K start, K end, int count);

    /**
     * Find a range of key, bounds inclusive, in reversed order, with the given Consistency Level for read
     * 
     * @param start
     *            Start key, inclusive
     * @param end
     *            End key, inclusive. Should be greater than start key with respect to the default comparator
     * @param count
     *            Maximum number of keys to be fetched
     * @param readLevel
     *            Consistency Level for read
     * @return List of keys
     */
    public List<K> findReverseKeys(K start, K end, int count, ConsistencyLevel readLevel);

    /**
     * Find a range of key, bounds exclusive, in reversed order
     * 
     * @param start
     *            Start key, exclusive
     * @param end
     *            End key, exclusive. Should be greater than start key with respect to the default comparator
     * @param count
     *            Maximum number of keys to be fetched
     * @return List of keys
     */
    public List<K> findReverseBoundsExclusiveKeys(K start, K end, int count);

    /**
     * Find a range of key, bounds exclusive, in reversed order, with the given Consistency Level for read
     * 
     * @param start
     *            Start key, exclusive
     * @param end
     *            End key, exclusive. Should be greater than start key with respect to the default comparator
     * @param count
     *            Maximum number of keys to be fetched
     * @param readLevel
     *            Consistency Level for read
     * @return List of keys
     */
    public List<K> findReverseBoundsExclusiveKeys(K start, K end, int count, ConsistencyLevel readLevel);

    /**
     * Find a range of key
     * 
     * @param start
     *            Start key
     * @param end
     *            End key. Should be less/greater than start key depending on the reverse flag
     * @param count
     *            Maximum number of keys to be fetched
     * @param bounds
     *            Bounds specified mode
     * @param ordering
     *            Order specified mode
     * @return List of keys
     */
    public List<K> findKeys(K start, K end, int count, BoundingMode bounds, OrderingMode ordering);

    /**
     * Find a range of key, with the given Consistency Level for read
     * 
     * @param start
     *            Start key
     * @param end
     *            End key. Should be less/greater than start key depending on the reverse flag
     * @param count
     *            Maximum number of keys to be fetched
     * @param bounds
     *            Bounds specified mode
     * @param ordering
     *            Order specified mode
     * @param readLevel
     *            Consistency Level for read
     * @return List of keys
     */
    public List<K> findKeys(K start, K end, int count, BoundingMode bounds, OrderingMode ordering,
            ConsistencyLevel readLevel);

    /**
     * Find first key, normal order
     * 
     * @return First key
     */
    public K findFirstKey();

    /**
     * Find first key, normal order, with the given Consistency Level for read
     * 
     * @param readLevel
     *            Consistency Level for read
     * @return First key
     */
    public K findFirstKey(ConsistencyLevel readLevel);

    /**
     * Find maximum n first keys, normal order
     * 
     * @param count
     *            Number of first keys to find
     * @return n first keys (or less)
     */
    public List<K> findFirstKeys(int count);

    /**
     * Find maximum n first keys, normal order, with the given Consistency Level for read
     * 
     * @param count
     *            Number of first keys to find
     * @param readLevel
     *            Consistency Level for read
     * @return n first keys (or less)
     */
    public List<K> findFirstKeys(int count, ConsistencyLevel readLevel);

    /**
     * Find last key, normal order
     * 
     * @return Last key
     */
    public K findLastKey();

    /**
     * Find last key, normal order, with the given Consistency Level for read
     * 
     * @param readLevel
     *            Consistency Level for read
     * @return Last key
     */
    public K findLastKey(ConsistencyLevel readLevel);

    /**
     * Find maximum n last keys, normal order
     * 
     * @return n last keys (or less)
     */
    public List<K> findLastKeys(int count);

    /**
     * Find maximum n last keys, normal order, with the given Consistency Level for read
     * 
     * @param count
     *            Number of last keys to find
     * @param readLevel
     *            Consistency Level for read
     * @return 'count' last keys (or less)
     */
    public List<K> findLastKeys(int count, ConsistencyLevel readLevel);

    /**
     * Find a key/value iterator. Start = null & end = null
     * 
     * Default count = 100;
     * 
     * @return KeyValue iterator
     */
    public KeyValueIterator<K, V> iterator();

    /**
     * Find a key/value iterator. Start = null & end = null, with the given Consistency Level for read
     * 
     * Default count = 100;
     * 
     * @param readLevel
     *            Consistency Level for read
     * @return KeyValue iterator
     */
    public KeyValueIterator<K, V> iterator(ConsistencyLevel readLevel);

    /**
     * Find a key/value iterator. Start = null & end = null
     * 
     * @param count
     *            Maximum number of key/value pairs to be fetched
     * 
     * @return KeyValue iterator
     */
    public KeyValueIterator<K, V> iterator(int count);

    /**
     * Find a key/value iterator. Start = null & end = null, with the given Consistency Level for read
     * 
     * @param count
     *            Maximum number of key/value pairs to be fetched
     * @param readLevel
     *            Consistency Level for read
     * @return KeyValue iterator
     */
    public KeyValueIterator<K, V> iterator(int count, ConsistencyLevel readLevel);

    /**
     * Find a key/value iterator
     * 
     * @param start
     *            Start key, inclusive
     * @param end
     *            End key, inclusive. Should be less than start key with respect to the default comparator
     * @param count
     *            Maximum number of key/value pairs to be fetched
     * @return KeyValue iterator
     */
    public KeyValueIterator<K, V> iterator(K start, K end, int count);

    /**
     * Find a key/value iterator, with the given Consistency Level for read
     * 
     * @param start
     *            Start key, inclusive
     * @param end
     *            End key, inclusive. Should be less than start key with respect to the default comparator
     * @param count
     *            Maximum number of key/value pairs to be fetched
     * @param readLevel
     *            Consistency Level for read
     * @return KeyValue iterator
     */
    public KeyValueIterator<K, V> iterator(K start, K end, int count, ConsistencyLevel readLevel);

    /**
     * Find a key/value iterator, bounds exclusive
     * 
     * @param start
     *            Start key, inclusive
     * @param end
     *            End key, inclusive. Should be less than start key with respect to the default comparator
     * @param count
     *            Maximum number of key/value pairs to be fetched
     * @return KeyValue iterator
     */
    public KeyValueIterator<K, V> iteratorBoundsExclusive(K start, K end, int count);

    /**
     * Find a key/value iterator, bounds exclusive, with the given Consistency Level for read
     * 
     * @param start
     *            Start key, inclusive
     * @param end
     *            End key, inclusive. Should be less than start key with respect to the default comparator
     * @param count
     *            Maximum number of key/value pairs to be fetched
     * @param readLevel
     *            Consistency Level for read
     * @return KeyValue iterator
     */
    public KeyValueIterator<K, V> iteratorBoundsExclusive(K start, K end, int count, ConsistencyLevel readLevel);

    /**
     * Find a key/value iterator in reverse order. Start = null & end = null
     * 
     * Default count = 100;
     * 
     * @return KeyValue iterator
     */
    public KeyValueIterator<K, V> iteratorReverse();

    /**
     * Find a key/value iterator in reverse order. Start = null & end = null, with the given Consistency Level for read
     * 
     * Default count = 100;
     * 
     * @param readLevel
     *            Consistency Level for read
     * @return KeyValue iterator
     */
    public KeyValueIterator<K, V> iteratorReverse(ConsistencyLevel readLevel);

    /**
     * Find a key/value iterator in reverse order. Start = null & end = null
     * 
     * @param count
     *            Maximum number of key/value pairs to be fetched
     * 
     * @return KeyValue iterator
     */
    public KeyValueIterator<K, V> iteratorReverse(int count);

    /**
     * Find a key/value iterator in reverse order. Start = null & end = null, with the given Consistency Level for read
     * 
     * @param count
     *            Maximum number of key/value pairs to be fetched
     * @param readLevel
     *            Consistency Level for read
     * @return KeyValue iterator
     */
    public KeyValueIterator<K, V> iteratorReverse(int count, ConsistencyLevel readLevel);

    /**
     * Find a key/value iterator, bounds inclusive, in reversed order
     * 
     * @param start
     *            Start key, inclusive
     * @param end
     *            End key, inclusive. Should be greater than start key with respect to the default comparator
     * @param count
     *            Maximum number of key/value pairs to be fetched
     * @return KeyValue iterator
     */
    public KeyValueIterator<K, V> iteratorReverse(K start, K end, int count);

    /**
     * Find a key/value iterator, bounds inclusive, in reversed order, with the given Consistency Level for read
     * 
     * @param start
     *            Start key, inclusive
     * @param end
     *            End key, inclusive. Should be greater than start key with respect to the default comparator
     * @param count
     *            Maximum number of key/value pairs to be fetched
     * @param readLevel
     *            Consistency Level for read
     * @return KeyValue iterator
     */
    public KeyValueIterator<K, V> iteratorReverse(K start, K end, int count, ConsistencyLevel readLevel);

    /**
     * Find a key/value iterator, bounds exclusive, in reversed order
     * 
     * @param start
     *            Start key, exclusive
     * @param end
     *            End key, exclusive. Should be greater than start key with respect to the default comparator
     * @param count
     *            Maximum number of key/value pairs to be fetched
     * @return KeyValue iterator
     */
    public KeyValueIterator<K, V> iteratorReverseBoundsExclusive(K start, K end, int count);

    /**
     * Find a key/value iterator, bounds exclusive, in reversed order, with the given Consistency Level for read
     * 
     * @param start
     *            Start key, exclusive
     * @param end
     *            End key, exclusive. Should be greater than start key with respect to the default comparator
     * @param count
     *            Maximum number of key/value pairs to be fetched
     * @param readLevel
     *            Consistency Level for read
     * @return KeyValue iterator
     */
    public KeyValueIterator<K, V> iteratorReverseBoundsExclusive(K start, K end, int count, ConsistencyLevel readLevel);

    /**
     * Find a key/value iterator
     * 
     * @param start
     *            Start key
     * @param end
     *            End key. Should be less/greater than start key depending on the reverse flag
     * @param count
     *            Maximum number of key/value pairs to be fetched
     * @param bounds
     *            Bounds specified mode
     * @param ordering
     *            Order specified mode
     * @return KeyValue iterator
     */
    public KeyValueIterator<K, V> iterator(K start, K end, int count, BoundingMode bounds, OrderingMode ordering);

    /**
     * Find a key/value iterator, with the given Consistency Level for read
     * 
     * @param start
     *            Start key
     * @param end
     *            End key. Should be less/greater than start key depending on the reverse flag
     * @param count
     *            Maximum number of key/value pairs to be fetched
     * @param bounds
     *            Bounds specified mode
     * @param ordering
     *            Order specified mode
     * @param readLevel
     *            Consistency Level for read
     * @return KeyValue iterator
     */
    public KeyValueIterator<K, V> iterator(K start, K end, int count, BoundingMode bounds, OrderingMode ordering,
            ConsistencyLevel readLevel);

    /**
     * Remove a key/value pair by key
     * 
     * @param key
     *            Key to remove
     */
    public void remove(K key);

    /**
     * Remove a key/value pair by key, with the given Consistency Level for write
     * 
     * @param key
     *            Key to remove
     * @param writeLevel
     *            Consistency Level for write
     */
    public void remove(K key, ConsistencyLevel writeLevel);

    /**
     * Remove a rang of key/value pairs, bounds inclusive
     * 
     * @param start
     *            Start key, inclusive
     * @param end
     *            End key, inclusive
     */
    public void remove(K start, K end);

    /**
     * Remove a rang of key/value pairs, bounds inclusive, with the given Consistency Level for write
     * 
     * @param start
     *            Start key, inclusive
     * @param end
     *            End key, inclusive
     * @param writeLevel
     *            Consistency Level for write
     */
    public void remove(K start, K end, ConsistencyLevel writeLevel);

    /**
     * Remove a rang of key/value pairs, bounds exclusive
     * 
     * @param start
     *            Start key, exclusive
     * @param end
     *            End key, exclusive
     */
    public void removeBoundsExclusive(K start, K end);

    /**
     * Remove a rang of key/value pairs, bounds exclusive, with the given Consistency Level for write
     * 
     * @param start
     *            Start key, exclusive
     * @param end
     *            End key, exclusive
     * @param writeLevel
     *            Consistency Level for write
     */
    public void removeBoundsExclusive(K start, K end, ConsistencyLevel writeLevel);

    /**
     * Remove a rang of key/value pairs
     * 
     * @param start
     *            Start key, exclusive
     * @param end
     *            End key, exclusive
     * @param bounds
     *            Bounds specified mode
     */
    public void remove(K start, K end, BoundingMode bounds);

    /**
     * Remove a rang of key/value pairs, with the given Consistency Level for write
     * 
     * @param start
     *            Start key, exclusive
     * @param end
     *            End key, exclusive
     * @param bounds
     *            Bounds specified mode
     * @param writeLevel
     *            Consistency Level for write
     */
    public void remove(K start, K end, BoundingMode bounds, ConsistencyLevel writeLevel);

    /**
     * Remove the first key/value pair
     */
    public void removeFirst();

    /**
     * Remove the first key/value pair, with the given Consistency Level for write
     * 
     * @param writeLevel
     *            Consistency Level for write
     */
    public void removeFirst(ConsistencyLevel readLevel);

    /**
     * Remove the n first key/value pairs
     * 
     * @param count
     *            First 'count' number of columns to be removed
     */
    public void removeFirst(int count);

    /**
     * Remove the n first key/value pairs, with the given Consistency Level for write
     * 
     * @param count
     *            Number of first key/value pairs to be removed
     * @param writeLevel
     *            Consistency Level for write
     */
    public void removeFirst(int count, ConsistencyLevel writeLevel);

    /**
     * Remove the last key/value pair
     */
    public void removeLast();

    /**
     * Remove the last key/value pair, with the given Consistency Level for write
     * 
     * @param writeLevel
     *            Consistency Level for write
     */
    public void removeLast(ConsistencyLevel writeLevel);

    /**
     * Remove the n last key/value pairs
     * 
     * @param count
     *            Number of last key/value pairs to be removed
     */
    public void removeLast(int count);

    /**
     * Remove the n last key/value pairs, with the given Consistency Level for write
     * 
     * @param count
     *            Number of last key/value pairs to be removed
     * @param writeLevel
     *            Consistency Level for write
     */
    public void removeLast(int count, ConsistencyLevel writeLevel);
}
