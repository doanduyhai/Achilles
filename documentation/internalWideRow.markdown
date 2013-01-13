## Internal Wide Row

 An internal wide row is simply a wide row structure stored along with other entity values:
 
	@Table(name="users_column_family")
	public class User implements Serializable
	{

		private static final long serialVersionUID = 1L;

		@Id
		private Long id;

		@Column
		private String firstname;

		@Column
		private String lastname;
		
		@Column(name="age_in_year")
		private Integer age;
		
		@Column
		private Set<String> addresses;
		
		@Lazy
		@Column
		private List<String> favoriteTags;
		
		@Column
		private Map<Integer,String> preferences;
		
		@Column
		private WideMap<UUID, Tweet> tweets; 
		
		...
	}
	
 In the above example, the *tweets* field is a **WideMap** proxy to allow inserting, finding and removing tweets POJO inside 
 the **User** like a wide row.

 For more details about **wide rows**, check [Simple wide row entity][simpleWideRow].

 Internally, **Achilles** saves all users data ( _firstname_, *lastname*, *age*, *addresses*, *favoriteTags*, *preferences* and all
 *tweets* values) in a **same physical row** in **Cassandra** storage engine. It has some benefits to doing so, you can benefit a lot
 from **Cassandra** [row caching][rowCaching].
 
 Even though internal wide rows are very similar to there simple counterparts, there is still a subtle but important difference. Since 
 internal wide rows data are persisted in the same physical row as other field values, the entity must exist if you want to access them.
 
 
 Simple example:
 
	User user = new User();
	user.setId(10L);
	user.setFirstname("DuyHai");
	
	// This will persist the user
	user = em.merge(user);
 
	// Get the WideMap proxy
	WideMap<UUID,Tweet> tweets = user.getTweets();
 
 
  The above example works because we persist the **User** entity first. In the below example, it will not work:

	// Will return NULL
	User foundUser = em.find(User.class,10L);
 
  
##### Some limitations
  
 Since the maximum number of columns per row in **Cassandra** is  2 billions (2.10^9), mixing wide row values with simple entity 
 values can be tricky when there is a large amount of data. If we take the above **User** entity, there will be:
 
 * 1 column used for *firstname*
 * 1 column used for *lastname*
 * 1 column used for *age*
 * M columns used for *addresses*
 * N columns used for *favoriteTags*
 * O columns used for *preferences*
 <br/>

So at most, the physical row can records up to `2.10^9 - (N+M+0+3)` values for the *tweets* internal wide row.

 Of course if we define other internal wide rows for the same entity, the available remaining space for each of them will be lesser
 than `2.10^9 - (N+M+0+3)`.
 
 The second limitation is the danger to have a row which is **too wide**. Yes, it seems paradoxical to write it but some guys at Ebay 
 have played with wide rows and recommended not to have too wide rows. (link [here][eBayBlog]). The main reasons for performance 
 issues is that too large rows 


 - create hotspots in the cluster
 - may not fit entirely in memory, limiting or worse, cancelling the benefit of **row caching** when the row size is very big to be
 of the same order of magnitude than the row cache size

<br/> 
 Long story short, it is a good idea to have wide rows but just do not make them too wide.
 
 If you want to overcome these limits or simply consider it's a bad practice to mix wide row values with entity values, just use
 [External wide row][externalWideRow]

 
 
 
 
 
 
 
 
[annotations]: /doanduyhai/achilles/tree/master/documentation/annotations.markdown
[emOperations]: /doanduyhai/achilles/tree/master/documentation/emOperations.markdown
[collectionsAndMaps]: /doanduyhai/achilles/tree/master/documentation/collectionsAndMaps.markdown
[dirtyCheck]: /doanduyhai/achilles/tree/master/documentation/dirtyCheck.markdown
[simpleWideRow]: /doanduyhai/achilles/tree/master/documentation/simpleWideRow.markdown
[internalWideRow]: /doanduyhai/achilles/tree/master/documentation/internalWideRow.markdown
[externalWideRow]: /doanduyhai/achilles/tree/master/documentation/externalWideRow.markdown
[multiComponentKey]: /doanduyhai/achilles/tree/master/documentation/multiComponentKey.markdown
[joinColumns]: /doanduyhai/achilles/tree/master/documentation/joinColumns.markdown
[rowCaching]: http://www.datastax.com/dev/blog/maximizing-cache-benefit-with-cassandra
[eBayBlog]: http://www.ebaytechblog.com/2012/08/14/cassandra-data-modeling-best-practices-part-2/