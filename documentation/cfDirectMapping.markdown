## Direct Column Family Mapping

 **Achilles** can map an entity to a plain **Cassandra** column family. The idea is to have an entity with a primary 
 key that maps to **Cassandra** row key (partition key) and a **WideMap** structure that micmics **Cassandra** 
 columns name/value.

 An entity is a a good candidate for direct mapping if:

 - It has an annotated *@Id* field
 - It has **one and only one** *@Column* of type **WideMap** 

<br/>    
 If any of these criteria is not match, **Achilles** will raise a **BeanMappingException** at runtime.
 
 To make it clearer, let's create a sample entity:
 
	@Entity
	@WideRow
	@Table("good_old_column_family")
	public class ColumnFamilyEntity implements Serializable
	{
		private static final long serialVersionUID = 1L;

		@Id
		private Long id;

		@Column
		private WideMap<Integer, String> wideMap;
	} 

 In the above example, **Achilles** will create the *good\_old\_column\_family* with:
 
 1. Key validation class of type **java.lang.Long**, the class of *@Id* field
 2. Comparator of type *'Composite(IntegerType)'* which represents the key type of *wideMap* field
 3. Validation class of type **java.lang.String**, the value type of *wideMap* field

<br/>
##### Usage
 
 Since the entity has only a primary key and a **WideMap** field which is a proxy to perform slice range queries, **it 
 does not make sense to persist the bean it because there is no eager fields to be saved**.
 
	
	ColumnFamilyEntity entity = new ColumnFamilyEntity();
	entity.setId(1L);
		
	em.persist(entity);  // Useless, nothing will be persisted so far
	
	
 To get the **WideMap** proxy, you must first obtain an entity from the EntityManager (by *find()* or *merge()*).
 

	ColumnFamilyEntity entity = new ColumnFamilyEntity();
	entity.setId(1L);
	
	entity = em.merge(entity); 	// Nothing is persisted so far
	
	// Or

	ColumnFamilyEntity entity = em.find(ColumnFamilyEntity.class,1L);

<br/>	

> 	**Please note that calling `entityManager.find(ColumnFamilyEntity.class,primaryKey)` will always return an object 
	whatever the primaryKey passed as argument. The entity only acts as wrapper to have access to the underlying 
	Cassandra column family structure**
 
 
<br/>

 Once you get a *managed* entity, you can use the **WideMap** field to insert, find or remove values to the column family. 
 
	// Get the WideMap proxy
	WideMap wideMap = entity.getWideMap(); 

	wideMap.insert(1,"value1");
	wideMap.insert(2,"value2");
	wideMap.insert(3,"value3");
	wideMap.insert(4,"value4");
	
<br/>

>	**All operations on a WideMap proxy are flushed immediatly to Cassandra. There is no need to call `em.merge(entity)`**

<br/>

 Now let's find some values by range:
	
	// Get the WideMap proxy
	WideMap wideMap = entity.getWideMap(); 
	
	// Find all values 
	// starting at 2 inclusive 
	// ending at 4 exclusive
	// in ascending order
	// limit result by 10 items
	List<KeyValue<Integer,String>> foundValues = wideMap.find(2, true, 4, false, false, 10);
	
	assertEquals(2,foudValues.size());
	assertEquals("value2",foundValues.get(0).getValue());
	assertEquals("value3",foundValues.get(1).getValue());

 The result should contains only "value2" & "value3" since **ending bound 4** was excluded from the result.
 
 Delete operations are also easy:
 
	// Get the WideMap proxy
	WideMap wideMap = entity.getWideMap(); 
	
	
	// Remove all values
	// starting at 1 exclusive
	// ending at 5 exclusive
	wideMap.remove(1, false, 5, false);
	

 The result is "value2", "value3" & "value4" being removed from the column family since bounds are exclusive. Please note that for
 deletion, it is not possible to define an ordering because you don't care whether values are removed in ascending or descending
 order.


<br/>

 Last but not least, if you want to access a very large serie of value, it is recommended to rely on *iterator()* instead of *find()*
 to avoid loading everything at once in memory.
 
 
	// Get the WideMap proxy
	WideMap wideMap = entity.getWideMap(); 

	// Find values
	// starting at 99999999 inclusive (by default)
	// ending at 100 inclusive (by default)
	// by batch of 100 elements
	KeyValueIterator<Integer,String> wideIterator = wideMap.iterator(99999999, 100, true, 100);
		

 In the above example, **Achilles** will load the found values by batch of 100 values. Once the last value of the batch is consumed
 upon call to *next()*, **Achilles** will load another batch of 100 elements and so on until the last value found. Internally
 **Achilles** relies on *ColumnSliceIterator* of **Hector** to do the job.
