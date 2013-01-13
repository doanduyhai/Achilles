## External Wide Row

 Unlike [Internal wide row][internalWideRow], external wide rows have their values saved in a dedicated column family.
 
 	@Table(name="users_column_family")
	public class User implements Serializable
	{

		private static final long serialVersionUID = 1L;

		@Id
		private Long id;

		@Column
		private String firstname;

		...
		
		@Column
		private WideMap<UUID, Tweet> tweets; 

		@Column(table="timeline_column_family")
		private WideMap<UUID, Tweet> timeline; 		
		...
	}
 
 In the above example, the *table* attribute of the *@Column* annotation indicates the column family to be used for 
 storage of *timeline* tweets. Internally, **Achilles** will create the *timeline\_column\_family* with:
 
 1. Key validation class of type **java.lang.Long**, the class of *@Id* field
 2. Comparator of type *'Composite(UUID)'* which represents the key type of *timeline* field
 3. Validation class of type **java.lang.Object** since the value type of *timeline* field is not a standard
	**Cassandra** type. All **Tweet** objects will be serialized to bytes using Java object serialization
<br/>

Like [Internal wide row][internalWideRow], external wide row values cannot exist independently from the enclosing
 entity. You cannot insert values directly into the *timeline\_column\_family* without going through the **User** entity.
 
 Example:
 
	User user = new User();
	user.setId(10L);
	user.setFirstname("DuyHai");
	
	// This will persist the user
	user = em.merge(user);
 
	// Get the WideMap proxy
	WideMap<UUID,Tweet> timeline = user.getTimeline();
	
	UUID currentTime = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
	Tweet tweet = new Tweet();
	tweet.setContent("Cassandra is cool");
	
	timeline.insert(currentTime,tweet);
 

 Upon call to `timeline.insert(currentTime,tweet)`, **Achilles** will insert into the *timeline\_column\_family* a new column
 with row key = 10L, column name = *currentTime* and column value = *tweet* converted to byte[].
 
 When the entity **User** is removed, the whole row with row key = 10L will be removed from *timeline\_column\_family*.
 
 The advantage of external wide rows is to separate entity data from wide row data. 

 An useful pattern is to have an entity with a few properties (name, age ...) and a lot of external wide row to manage data related
 to this user:

 	@Table(name="users_column_family")
	public class User implements Serializable
	{

		private static final long serialVersionUID = 1L;

		@Id
		private Long id;

		@Column
		private String firstname;

		...
			
		@Column(table="userline_column_family")
		private WideMap<UUID, Tweet> tweets; 	

		@Column(table="timeline_column_family")
		private WideMap<UUID, Tweet> timeline; 			
		
		@Column(table="friends_column_family")
		private WideMap<String, Long> friends; 			

		@Column(table="followers_column_family")
		private WideMap<String, Long> followers;		

	}
	
 Above, we have a typical modeling example for an User in Tweeter. The entity **User** defines some property for the user ( *firstname*,
 *age*...). Then there is a list of external wide rows:
 
 - *tweets* to store user' tweets
 - *timeline* to store tweets from user and his friends
 - *friends* to store user' friends by id
 - *followers* to store user' followers by id

<br/>
With this design, sending a new Tweet and spreading it to all followers is a piece of cake:
 
	UUID currentTime = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
	
	Tweet tweet = new Tweet();
	tweet.setContent("Achilles is awesome");
	tweet.setId(currentTime);
	
	// Put the tweet in this user tweetline
	user.getTweets().insert(currentTime,tweet);
	
	// Put the tweet in this user timeline
	user.getTimeline().insert(currentTime,tweet);
	
	// Get an iterator on all followers
	KeyValueIterator<String, Long> followersIterator = user.getFollowers().iterator(null,null,false,100);
	
	// Spread the tweet to each follower timeline
	while(followersIterator.hasNext())
	{
		Long followerId = followersIterator.next().getValue();
		User follower = em.find(User.class,followerId);
		
		follower.getTimeline().insert(currentTime,tweet);
	}
	
 That's as easy as it !	
	
[annotations]: /doanduyhai/achilles/tree/master/documentation/annotations.markdown
[emOperations]: /doanduyhai/achilles/tree/master/documentation/emOperations.markdown
[collectionsAndMaps]: /doanduyhai/achilles/tree/master/documentation/collectionsAndMaps.markdown
[dirtyCheck]: /doanduyhai/achilles/tree/master/documentation/dirtyCheck.markdown
[simpleWideRow]: /doanduyhai/achilles/tree/master/documentation/simpleWideRow.markdown
[internalWideRow]: /doanduyhai/achilles/tree/master/documentation/internalWideRow.markdown
[externalWideRow]: /doanduyhai/achilles/tree/master/documentation/externalWideRow.markdown
[multiComponentKey]: /doanduyhai/achilles/tree/master/documentation/multiComponentKey.markdown
[joinColumns]: /doanduyhai/achilles/tree/master/documentation/joinColumns.markdown 