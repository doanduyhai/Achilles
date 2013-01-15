## Join columns

### Rationale 

 Most of NoSQL technologies recommend denormalizing (duplicating) data to improve performances but there are use cases where 
 you cannot duplicate data, for example with *shared* entities.

 When an entity is *shared*, it can be accessed by many other entities and its state modified. If this *shared* entity content
 is duplicated, you must manage the state by tracking all copies of the entity to apply changes, a real nightmare.

 For such use cases, it is totally relevant to keep just one copy of the entity and only share its *id*. That's how join columns
 work in SQL.

 Only entities annotated with *@Table* are candidates for join columns. Obviously you cannot join on plain POJOs which are not
 managed by **Achilles**.

 In the background, **Achilles** persists only the primary key of the join entity and will use it to fetch the entity when the
 getter of the field is invoked, it's a pretty standard design.

### Supported cascading styles

 There are 5 types of cascading styles defined by JPA:

 - PERSIST
 - MERGE
 - REFRESH
 - REMOVE
 - ALL, which is just a shortcut for all the 4 types
<br/>

Out of the above 4 types, **Achilles** only support the first 3 {PERSIST, MERGE, REFRESH} and the ALL type. The reason for not
supporting cascade REMOVE is that unlike relational databases, there is no integrity check in **Cassandra** for join values so
removing an entity can produce unexpected behaviors if it is referenced somewhere else. Consequently, the **orphanRemoval** 
attribute on *@OneToMany* and *@OneToOne* annotations is ignored by **Achilles**.

 This restriction may be lift in future if a consistent behavior can be found when dealing with references to *non-existing* 
 entities.

 CascadeType.REFRESH is supported but is optional because internally **Achilles** always reloads (lazily) the join entity when 
 the enclosing entity is refreshed.

### Laziness

 All join entities are fetched lazily, upon invocation of the getter method. Join entities are never fetched eagerly because 
 the performance cost incurred by the extra request to load the join entity.
 

### Usage

 Let's consider the Twitter example. We define below **Tweet** and **User** entities:

	@Table
	public class Tweet implements Serializable
	{

		private static final long serialVersionUID = 1L;

		@Id
		private UUID id;

		@ManyToOne
		@JoinColumn
		private User creator;

		@Column
		private String content;
	}


	@Table
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
		private Long tweetCount;

		@Column
		private Long friendsCount;

		@Column
		private Long followersCount;

		@OneToMany(cascade = CascadeType.ALL)
		@JoinColumn
		private WideMap<UUID, Tweet> tweetline;

		@ManyToMany
		@JoinColumn(table = "timeline_column_family")
		private WideMap<UUID, Tweet> timeline;

		@ManyToMany(cascade = CascadeType.REFRESH)
		@JoinColumn
		private WideMap<Long, User> friends;

		@ManyToMany(cascade = CascadeType.REFRESH)
		@JoinColumn
		private WideMap<Long, User> followers;
	}


 The **Tweet** class becomes an entity managed by **Achilles** instead of POJO. It has a join column *author* pointing to
 the **User** entity. There is no cascading set on this relationship because persisting or merging a **Tweet** should not
 modify the author **User** instance.

 The **User** entity has 
 
 1. an internal join wide *tweets*. The internal wide row only keeps the  primary key of each **Tweet**. **Achilles** will 
    load the whole entity when the **WideMap** element is accessed (invocation of *findRange()*, *get()* or *iterator()* ).
	CascadeType.ALL has been set on this field because we want  **Achilles** to persist effectively the **Tweet** when adding
	it to the user tweetline.
<br/>   
 2. an external join wide row *timeline* which keeps track of all tweets in the user timeline. The wide row is external 
    because the amount of tweets can be huge for a timeline. There is no need to cascade persist on this field  since the 
	tweet in an user timeline has been persisted already when the author saved it in its own *tweetline* (see point 1. above)
<br/>   
 3. an internal join wide row called *friends* which indexes all the friends of current user by their id. CascadeType.REFRESH
    is set but not mandatory because join entity will be loaded from **Cassandra** anyway when accessed.
<br/>   
 4. similarly, an internal join wide row for all user' *followers*
<br/>
 
In this example we use join columns for *friends* and *followers* fields because we want to load the up-to-date **User** 
 entity. Persisting a copy of each user as plain POJO is not an option since the **User** entity state (firstname/lastname)
 can change over time.

##### Cascading with simple join entity

 1. On `entityManager.persist()` invocation:
	
	If the CascadeType.PERSIST or CascadeType.ALL has been activated, **Achilles** will save the join entity, overriding
	the existing copy in **Cassandra** with the new value
	
    Else **Achilles** will check in **Cassandra** whether the entity already exists with the given primary key. If not an
	exception is raised.
<br/>   
 2. On `entityManager.merge()` invocation:

	If the CascadeType.MERGE or CascadeType.ALL has been activated
	
	>	If the join entity is not *managed*, **Achilless** will simply persist it and save its primary key in the 
	enclosing entity
		
	>	Else just invoke `entityManager.merge()` on the join entity. All the dirty check mechanism will be applied as
		usual
<br/>   
 3. On `entityManager.refresh()` invocation: nothing happens, the join entity will be reloaded automatically on the next 
    invocation of getter method. Indeed the CascadeType.REFRESH is not really useful 
<br/>   
 4. On `entityManager.remove()` action: nothing happens because CascadeType.REMOVE is not supported.
<br/>   
 5. On getter invocation, if **Achilles** cannot find any joined entity with the primary key, a **null** value is returned  
 
##### Cascadint with WideMap join entity 
 
 1. On *wideMap.put()* method invocation:
	
	If the CascadeType.PERSIST or CascadeType.ALL has been activated, **Achilles** will save the join entity, overriding
	the existing copy in **Cassandra** with the new value
	
    Else **Achilles** will check in **Cassandra** whether the entity already exists with the given primary key. If not an
	exception is raised.
<br/>   
 2. On *wideMap.findRange()*, *wideMap.get()* or *iwideMap.terator()*  methods invocation:

	**Achilles** is just loading the joined entity using its primary key. If the entity does not exists, **Achilles** 
	return **null**

 
