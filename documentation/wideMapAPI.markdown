## WideMap API


 In addition to the JPA mapping and operations, **Achilles** introduces a new **WideMap** API that reflects the way **Cassandra** 
 handles wide rows.

##### The API
 
 A **WideMap** is a public interface that exposes the following methods:
 
	// Insert operations
	public void insert(K key, V value, int ttl);

	public void insert(K key, V value);
	
	
	// Find operations
	public V get(K key);

	// Find KeyValue
	public List<KeyValue<K, V>> find(K start, K end, int count);

	public List<KeyValue<K, V>> findBoundsExclusive(K start, K end, int count);

	public List<KeyValue<K, V>> findReverse(K start, K end, int count);

	public List<KeyValue<K, V>> findReverseBoundsExclusive(K start, K end, int count);

	public List<KeyValue<K, V>> find(K start, boolean inclusiveStart, K end, boolean inclusiveEnd,
			boolean reverse, int count);

	public KeyValue<K, V> findFirst();

	public List<KeyValue<K, V>> findFirst(int count);

	public KeyValue<K, V> findLast();

	public List<KeyValue<K, V>> findLast(int count);

	// Find Value
	public List<V> findValues(K start, K end, int count);

	public List<V> findValuesBoundsExclusive(K start, K end, int count);

	public List<V> findValuesReverse(K start, K end, int count);

	public List<V> findValuesReverseBoundsExclusive(K start, K end, int count);

	public List<V> findValues(K start, boolean inclusiveStart, K end, boolean inclusiveEnd,
			boolean reverse, int count);

	public V findValuesFirst();

	public List<V> findValuesFirst(int count);

	public V findValuesLast();

	public List<V> findValuesLast(int count);

	// Find Key
	public List<K> findKeys(K start, K end, int count);

	public List<K> findKeysBoundsExclusive(K start, K end, int count);

	public List<K> findKeysReverse(K start, K end, int count);

	public List<K> findKeysReverseBoundsExclusive(K start, K end, int count);

	public List<K> findKeys(K start, boolean inclusiveStart, K end, boolean inclusiveEnd,
			boolean reverse, int count);

	public K findKeysFirst();

	public List<K> findKeysFirst(int count);

	public K findKeysLast();

	public List<K> findKeysLast(int count);
	
	// Iterator
	public KeyValueIterator<K, V> iterator(K start, K end, int count);

	public KeyValueIterator<K, V> iteratorBoundsExclusive(K start, K end, int count);

	public KeyValueIterator<K, V> iteratorReverse(K start, K end, int count);

	public KeyValueIterator<K, V> iteratorReverseBoundsExclusive(K start, K end, int count);

	public KeyValueIterator<K, V> iterator(K start, boolean inclusiveStart, K end,
			boolean inclusiveEnd, boolean reverse, int count);

			
	// Remove operations		
	public void remove(K key);

	public void remove(K start, K end);

	public void removeBoundsExclusive(K start, K end);

	public void remove(K start, boolean inclusiveStart, K end, boolean inclusiveEnd);

	public void removeFirst();

	public void removeFirst(int count);
	
	public void removeLast();

	public void removeLast(int count);	

 The **WideMap** API allows you to insert, retrieve and remove value, by range or not. Unlike **Cassandra** Slice Range
 API, you can define **inclusive** or **exclusive bounds** for range queries and range deletions. The API also offers
 some convenient methods like `findFirst()` or `findLast()` to save you the hassle of defining a slice range query with count
 =1.
 
 The API also provides `findValuesXXX()` and `findKeysXXX()` methods to get only values or keys. The performance is identical
 to the `find()` methods since **Achilles** still fetch all the data in the background but only return keys or values.

 The **KeyValue** type returned by some methods is just a POJO to hold the key/value pair.

 The **KeyValueIterator** is a custom Iterator that returns a **KeyValue** instance upon call to *next()*. This iterator
 is a fork of the original **ColumnSliceIterator** from Hector source code. Basically, the iterator loads batches of *count*
 elements in memory and return then upon call to *next()*. When the last element in memory is returned, the iterator will
 fetch another batch of *count* elements from **Cassandra** and so on until the last element.
 
 Behind the scene, **Achilles** relies on (Dynamic) Composite slice range queries to do the job.
 
##### Usage 

 Let's check the example with an **User** entity
 
    @Entity 
	public class User implements Serializable
	{

		private static final long serialVersionUID = 1L;

		@Id
		private Long id;

		@Column
		private String firstname;

		@Column
		private String lastname; 
		
		@Column
		WideMap<UUID,Tweet> tweets;
	}		

 The **WideMap** interface is parameterized by a key(here UUID) and value(Tweet). **Tweet** here is a simple POJO. A 
 **WideMap** is the exact image of a wide row in **Cassandra**, the key representing the column comparator of the row.
 
 In the above example, the *tweets* field is defined as a **WideMap**. It is merely a proxy that lets you interact
 with the exposed API. To use a **WideMap** you must first retrieve a *managed* entity (either with `em.find()` or 
 `em.merge()`) then invoke the getter on the **WideMap** field:
 
	User user = em.find(User.class,1L);
	
	//Get handle on the WideMap proxy
	WideMap<UUID,Tweet> tweets = user.getTweets();
	
 Then you can use any of the methods exposed by the API to insert, search by range or remove tweets. Under the hood
 **Achilles** relies on slice queries to fetch or insert data into **Cassandra** 
 

	// Find the last 5 tweets
	List<KeyValue<UUID,Tweet>> recentTweets = tweets.findLast(5);
 
##### Dirty check, laziness and flush  
 
 Unlike normal fields, **WideMap** fields are not subjects to dirty check by **Achilless**. Similarly, those fields are
 lazy by nature since they act only as a proxy to the slice queries.
 
>	Any operation done with a **WideMap** translates to a direct access to **Cassandra**. A call on `insert()` will persist
	effectively and immediatly the value into **Cassandra**, there is no need to call `em.merge(foundUser)`.Similarly all 
	the `find()` methods fetch the data directly from **Cassandra**, there is nothing such as first level caching done by 
	the **EntityManager**


##### Performance considerations
	
 For massive insertions, to reduce the number of calls to **Cassandra**, you can use the Batch Mode, check [Performance][perf]
 for more details.
 
 On remove range operations, **Achilles** first fetch data in memory with a slice query before removing them in a batch. This
 is an inherent limitation of **Cassandra** which cannot perform remove_slice operations (https://issues.apache.org/jira/browse/CASSANDRA-494)
 
 Consequently, giving a too wide range for deletion will deplete quickly your memory. For wide range deletion, use iterator 
 and batch mode to flush deletions by batch.
 
[perf]:  /documentation/performance.markdown