# Achilles #

# Presentation #

 Achilles is an open source Entity Manager for Apache Cassandra. Among all the features:
 
 * dirty check
 * lazy loading
 * collections and map support (they can be defined lazy or eager) 
 * support for Cassandra wide rows inside entities
 * support for multi components column name with wide row
 * join columns (restricted to *@ManyToOne* and *@ManyToMany*, check documentation for more details)
 * native wide row mapping with dedicated annotation

# Installation #

 For the moment the framework is not available yet as a maven repo (it will be soon) so just clone or fork the 
 project:
 
>	git clone https://github.com/doanduyhai/Achilles.git

 For now, **Achilles** depends on the following libraries:
 
 1. Cassandra 1.1.6 (will be upgraded soon to 1.2)
 2. Hector-core 1.0-5 (<strong>Achilles</strong> is built upon Hector API) 

 Install **Achilles** jar into your local Maven repository:
 
>	mvn clean install 

 Then add the following dependency in your **pom.xml**:
 
	<dependency>	
		<groupId>fr.doan</groupId>
		<artifactId>achilles</artifactId>
		<version>1.0-beta</version>
	</dependency>  

# Documentation #

1. [Available JPA annotations and their semantics in **Achilles**](/doanduyhai/achilles/tree/master/documentation/annotations.markdown)
2. [Supported operations for EntityManager](/doanduyhai/achilles/tree/master/documentation/emOperations.markdown)
3. [Collections and Map with **Achilles**](/doanduyhai/achilles/tree/master/documentation/collectionsAndMaps.markdown)
4. [Dirty check](/doanduyhai/achilles/tree/master/documentation/dirtyCheck.markdown)
5. [Internal wide row](/doanduyhai/achilles/tree/master/documentation/internalWideRow.markdown)
6. [External wide row](/doanduyhai/achilles/tree/master/documentation/externalWideRow.markdown)
7. [Multi components for wide row](/doanduyhai/achilles/tree/master/documentation/multiComponentKey.markdown)
8. [Join columns](/doanduyhai/achilles/tree/master/documentation/joinColumns.markdown)

 
# 5 minutes tutorial #

### Bootstrap #

 First of all you need to initialize the **EntityManagerFactory**.

	Cluster cluster = ...
	
	Keyspace keyspace = ...
	
	EntityManagerFactory entityManagerFactory = new ThriftEntityManagerFactoryImpl(
			cluster, keyspace, "my.package1,my.package2", true);


 You need to provide a `me.prettyprint.hector.api.Cluster` and 	`me.prettyprint.hector.api.Keyspace` 
 for the initialization of the **EntityManagerFactory**. 
 
 The third argument is a list of packages (coma separated) for entities scanning.
 
 The *last boolean flag* tells **Achilles** whether to force the column families creation if they do not 
 exist or not. This flag should be set to **true** most of the time because **Achilles** creates its own 
 column family structure for persistence and it is very unlikely that your existing column families 
 are compatible with **Achilles** structure (but it can be in some cases, check [Mapping with existing 
 Column Families][])

 For Spring users:

	<bean id="entityManagerFactory" class="fr.doan.achilles.entity.factory.ThriftEntityManagerFactoryImpl">
		<constructor-arg index="0" value="cluster"/>
		<constructor-arg index="1" value="keyspace"/>
		<constructor-arg index="2" value=""my.package1,my.package2""/>
		<constructor-arg index="3" value="true"/>		
	</bean>

### Bean Mapping #

 Let's create an **User** bean in JPA style

	import javax.persistence.Column;
	import javax.persistence.Id;
	import javax.persistence.Table;
	import org.achilles.annotations.Lazy;
 
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
		
		// Getters and setters ...
	}	

 All fields are eagerly fetched except the *favoriteTags* list annotated by **@Lazy**. 
 The list will be loaded **entirely**	when calling *getFavoriteTags()*.
	
### Usage #


 First we create an **User** and persist it:
 
	EntityManager em = entityManagerFactory.createEntityManager();

	User user = new User();
	user.setId(1L);
	user.setFirstname("DuyHai");
	user.setLastname("DOAN");
	user.setAge(30);

	// Favorite Tags
	Set<String> tags = new HashSet<String>();
	tags.add("computing");
	tags.add("java");
	tags.add("cassandra");

	user.setFavoriteTags(tags);
	
	// Addresses
	user.setAddresses(Arrays.asList("1 rue de la paix Paris","15 rue Vaugirard Paris"));
	
	// Preferences
	Map<Integer,String> preferences = new HashMap<Integer,String>();
	preferences.put(1,"FR");
	preferences.put(2,"French");
	preferences.put(3,"Paris");
	
	user.setPreferences(preferences);
	
	// Save user
	em.persist(user);

 Then we find it by id:
	
	User foundUser = em.find(User.class,1L);
	
	// Now add some new tags
	foundUser.getFavoriteTags().add("achilles"); 
	
	// Save it
	foundUser = em.merge(foundUser);
	
	assertEquals(foundUser.getFavoriteTags().get(3),"achilles");



### Wide Row


 To use **Cassandra** wide rows inside entities, add the following property to the **User** entity:
 
	@Column
	private WideMap<UUID, Tweet> tweets;

 **Tweet** here is a simple POJO.

 The declaration for the field *tweets* is only a place holder. **WideMap** is an interface exposing some useful operations for wide row.
 These operations mimic Cassandra *slice range* API. Check [Internal wide row](/doanduyhai/achilles/tree/master/documentation/annotations.markdown) for more details.
 
 To initialize the *tweets* field, you must get an **User** entity from the EntityManager (call *find()* or *merge()*):
 
	User foundUser = em.find(User.class,1L);
	
 Then calling *getTweets()* on *foundUser* will return a proxy object on which you can invoke all methods:
 
	UUID uuid1 = ...
	UUID uuid2 = ...
	UUID uuid3 = ...
	UUID uuid4 = ...
	
	Tweet tweet1 = new Tweet(), tweet2 = new Tweet(), 
	      tweet3 = new Tweet(), tweet4 = new Tweet();
	
	// Get a WideMap proxy on "tweets" to perform slice range operations
	WideMap<Integer,Tweet> myTweets = foundUser.getTweets();
	
	// Insert tweets
	myTweets.insert(uuid1,tweet1);
	myTweets.insert(uuid2,tweet2);
	myTweets.insert(uuid3,tweet3);
	
	// Insert with TTL
	myTweets.insert(uuid4,tweet4,150);
	
	...
	...
	
 Later, you can retrieved the saved tweets with *findRange()* methods:
 
	// Should return tweet2 & tweet3
	List<KeyValue<UUID,Tweet>> foundRange = myTweets.findRange(
			uuid2,  // Start key
			true,	// Inclusive start key
			uuid4,	// End key
			false,	// Exclusive end key
			false,	// Reverse order = false
			10		// Limit by 10 results	
	);
	
	assertEquals(foundRange.size(),2);	
	
	// Assuming that you implemented equals() & hashCode() for Tweet class
	assertEquals(foundRange.get(0).getValue(),tweet2); 
	assertEquals(foundRange.get(1).getValue(),tweet3);
	
 The **KeyValue** type is just a holder structure to store the keys and values.		
 It is also possible to remove tweets by range using *removeRange()* methods:

	// Remove tweet2, tweet3 & tweet4
	myTweets.removeRange(
		uuid2,	// Start key
		true,	// Inclusive start key
		uuid4,	// End key
		true	// Inclusive end key
	);

	// Return 10 first tweets in ascending order
	List<KeyValue<UUID,Tweet>> remainingTweets = myTweets.findRange(null,null,false,10);	
	
	// Check that tweet2, tweet3 & tweet4 have been removed indeed
	assertEquals(remainingTweets.size(),2);	
	assertEquals(foundRange.get(0).getValue(),tweet1); 
	assertEquals(foundRange.get(1).getValue(),tweet5);



 And that's it. To have more details on the advanced features, please check the Documentation links above.	
	
>	**Important note**: All operations done with **WideMap** properties are **flushed immediatly to Cassandra**.
	There is no need to call `em.merge(foundUser)`. **WideMap** interface and API is not part of JPA but a proprietary
	extension of **Achilles** to support Cassandra wide row and slice ranges.	
	
	
 

	
	

