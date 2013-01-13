## Simple wide row Entity

 **Achilles** can map an entity to a plain **Cassandra** column family (named as **wide row** in this documentation).
 
 The idea is to have an entity with a primary key that maps to **Cassandra** row key (partition key) and a **WideMap**
 structure that micmic **Cassandra** columns name/value.

 Below is the operations exposed by the **WideMap** interface:
 
 
	public interface WideMap<K, V>
	{
		public V get(K key);

		public void insert(K key, V value, int ttl);

		public void insert(K key, V value);

		public List<KeyValue<K, V>> findRange(K start, K end, boolean reverse, int count);

		public List<KeyValue<K, V>> findRange(K start, K end, boolean inclusiveBounds, boolean reverse,
				int count);

		public List<KeyValue<K, V>> findRange(K start, boolean inclusiveStart, K end,
				boolean inclusiveEnd, boolean reverse, int count);

		public KeyValueIterator<K, V> iterator(K start, K end, boolean reverse, int count);

		public KeyValueIterator<K, V> iterator(K start, K end, boolean inclusiveBounds,
				boolean reverse, int count);

		public KeyValueIterator<K, V> iterator(K start, boolean inclusiveStart, K end,
				boolean inclusiveEnd, boolean reverse, int count);

		public void remove(K key);

		public void removeRange(K start, K end);

		public void removeRange(K start, K end, boolean inclusiveBounds);

		public void removeRange(K start, boolean inclusiveStart, K end, boolean inclusiveEnd);
	}
 
 The **WideMap** API allow you to insert, retrieve and remove value, by range or not. Contrary to **Cassandra** Slice Range
 API, you can define **inclusive** or **exclusive bounds** for range queries and range delete.
 
 Behind the scene, **Achilles** relies on (Dynamic) Composite slice range queries to do the job.
 
 To examplify this:
 
	@WideRow
	@Table("good_old_column_family")
	public class WideRowEntity implements Serializable
	{
		private static final long serialVersionUID = 1L;

		@Id
		private Long id;

		@Column
		private WideMap<Integer, String> wideMap;
	} 
	
 In the above example, **Achilles** will create the *good\_old\_column\_family* with:
 
 1. Key validation class of type **java.lang.Long**, the class of *@Id* field
 2. Comparator of type *'Composite(Integer)'* which represents the key type of *wideMap* field
 3. Validation class of type **java.lang.String**, the value type of *wideMap* field

   
 An entity is a valid Wide Row for **Achilles** if:

 - It has an annotated *@Id* field
 - It has **one and only one** *@Column* of type **WideMap** 
 
 If any of these criteria is not match, **Achilles** will raise a **BeanMappingException** at runtime.