## Join columns

### Rationale 

 Most of NoSQL technologies recommend denormalizing (duplicating) data to improve performances but there are use cases where 
 you cannot duplicate data, for example with *shared* entities.

 When an entity is *shared*, it can be accessed by many other entities and its state modified. If this *shared* entity content
 is duplicated, you must manage the state by tracking all copies of the entity to apply changes, a real nightmare.

 For such use cases, it is totally relevant to keep just one copy of the entity and only share its *id*. That's how join columns
 work in SQL.


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
attribute on *@OneToMany* and *@OneToOne* annotations is ignored by **Achilles**

 This restriction may be lift in future if a consistent behavior can be found when dealing with references to *non-existing* 
 entities.


### Laziness

 All join entities are fetched lazily, upon invocation of the getter method. Join entities are never fetched eagerly because 
 the performance cost incurred by the request to load then entity.
 

### Usage

 Let's consider the Twitter example. We define below Tweet and User entities:

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

		@OneToMany(cascade = CascadeType.ALL)
		@JoinColumn
		private WideMap<UUID, Tweet> tweets;

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
 
 1. an internal join wide row to **Tweet** entity through *tweets* **WideMap**. The internal wide row
    only keeps the primary key of each **Tweet**. **Achilles** will load the whole entity when the **WideMap** element is accessed
    (invocation of *findRange()*, *get()* or *iterator()* ).
 2. 
 

 
### Internals

 Only entities annotated with *@Table* are candidates for join columns. Obviously you cannot join on plain POJOs which are not
managed by **Achilles**.

 In the background, **Achilles** persists only the primary key of the join entity and will use it to fetch the entity when the
getter of the field is invoked, it's a pretty standard design.


##### Simple join entity cascading

 1. On persist action:
	
	If the CascadeType.PERSIST or CascadeType.ALL has been activated, **Achilles** will save the join entity, overriding
	the existing copy in **Cassandra** with the new value
	
	Else **Achilles** will check in **Cassandra** whether the entity exists with the given primary key. If yes, the 
	primary key is saved, otherwise an exception is raised

 2. On merge action:

	If the CascadeType.MERGE or CascadeType.ALL has been activated
		If the join entity is not *managed*, **Achilless** will simply persist it and save its primary key in the 
		enclosing entity
		
		Else just invoke `entityManager.merge()` on the join entity. All the dirty check mechanism will be applied as
		usual

	Else, do nothing

 3. On refresh action:

	Nothing happens


 

 

 
