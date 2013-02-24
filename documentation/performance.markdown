## Performance

### Join columns

 **Achilles** always fetches join columns lazily because there is nothing such join query supported
 by **Cassandra** so eager and lazy fetching do have exactly the same cost.
 
 When a collection of join entities is accessed, **Achilles** gets the join ids and performs a *multi
 get slice* query to load all the join entities data in one request.
 
 **Cassandra** will then hit different rows to fetch necessary data, especially when you use a *Random
 Order Partioner*.
 
 When using join columns, you must bear in mind this extra cost on performance. It is recommended to
 load join entities by small batches with **WideMap**' iterators. If you use plain collections or maps,
 try to keep them as small as possible
 
### Batch mode

 To insert lots of data at once, you can use **Achilles** batch mode. The ThriftEntityManager provides
 two custom methods:
 

	public void startBatch(Object entity)
	
	public void endBatch(Object entity)   

On `startBatch()` call, **Achilles** will create a batch mutator in the entity proxy. Every insertion
 operation done with the **WideMap** API will be batched in this mutator. Upon invocation of 
 `endBatch()`, all insertions will be flushed to **Cassandra** in one request.
 
 Example
 
	WideMap<UUID,Tweet> tweets = user.getTweets();
	
	// Create new mutator for batch
	em.startBatch(user);
	
	for(Tweet newTweet: tweetList)
	{
		tweets.insert(newTweet.getUuid(),newTweet);
	}
	
	// Execute mutator to flush
	em.endBatch(user);

 
> Please notice that the batch mode only works for **WideMap** fields. 

For normal fields (simple, collection or map) **Achilles** also relies on an internal mutator to flush
 data when the entity is persisted or merged. 
 
 Join entities on non-WideMap fields are saved at flush time using internal mutators. There is one mutator
 per type of join entity. Indeed batch mutation is based on row key type so different join entity types
 require distinct batch mutators.
 
  
 a mutator.
 
   