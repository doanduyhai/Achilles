### Compatible JPA Annotations

 For bean mapping, only **field-based access type** is supported by **Achilles**. Furthermore, there is no default mapping 
 for fields. If you want a field to be mapped, you must annotate it with *@Column* or *@JoinColumn*. All fields that are not
 annotated by either annotationsis considered transient by **Achilles**
 
 Below is a list of all JPA annotations supported by **Achilles**. 

====  
##### @Table

 Indicates that an entity is candidate for persistence. When then *name* attribute is filled, it indicates the name
 of the column family used by by the **Cassandra** engine to store this entity.
 
 Example:
 
	@Table(name = "users_column_family")
	public class User implements Serializable
	{
		private static final long serialVersionUID = 1L;
		...
		...
	}

>	Please note that all entities must implement the `java.io.Serializable`	interface and provide a **serialVersionUID**.
	Failing to meet this requirement will trigger a **BeanMappingException**.


====	
##### @Id

 Marks a field as the primary key for the entity. The primary key can be of any type, even a plain POJO. However it must 
 implement the `java.io.Serializable` interface otherwise an **AchillesException** will be raised.

 Under the hood, the primary key will be serialized to bytes array and  used as row key (partition key) by the **Cassandra**
 engine.


====
##### @Column

 Marks a field to be mapped by **Achilles**. When the *name* attribute of *@Column* is given, the field
 will be mapped to this name. Otherwise the field name will be used.

 Example:

	@Column(name = "age_in_years")
	private Long age; 

 When put on a **WideMap** field, the *table* attribute of *@Column* annotation indicates the external column family to
 be used for the wide map. For more details, check [External wide row](/doanduyhai/achilles/tree/master/documentation/externalWideRow.markdown) 

 Example:

	@Column(table="my_tweets_column_family")
	private WideMap<UUID,Tweet> tweets;
	

====	
##### @ManyToOne, @ManyToMany

 These annotations should be used only along with *@JoinColumn*.
 
 *@ManyToOne* should only be used with *simple* join columns. 

 Example:
 
	@Table
	public class Tweet implements Serializable
	{

		private static final long serialVersionUID = 1L;

		@Id
		private UUID id;

		@ManyToOne
		@JoinColumn
		private User creator;
		
		...
		...
	}	

	
 *@ManyToMany* should only be used wide **WideMap** join columns. Suppported JPA cascade type are:

 Example: 
 
	@Table
	public class User implements Serializable
	{

		private static final long serialVersionUID = 1L;

		@Id
		private Long id;

		...

		@ManyToMany(cascade = CascadeType.ALL)
		@JoinColumn
		private WideMap<Integer, Tweet> timeline;

		...
	}	

 Suppported JPA cascade for *@ManyToOne* and *@ManyToMany* annotations are:
 
 * ALL
 * PERSIST
 * MERGE
 * REFRESH 

 *CascadeType.REMOVE* is not supported by **Achilles** as per design (check [Join columns](/doanduyhai/achilles/tree/master/documentation/joinColumns.markdown) for more details)
 *CascadeType.RESFRESH* is implicit for join columns.
 *CascadeType.ALL* in **Achilles** is just a shortcut for `{CascadeType.PERSIST,CascadeType.MERGE}`
 

====	
##### @JoinColumn	

 Marks a field to be mapped by **Achilles**. When the *name* attribute of *@JoinColumn* is given, the field
 will be mapped to this name. Otherwise the field name will be used.
 
 For a join column, only the primary key of the join entity is persisted by the **Cassandra** storage engine. 
 **Achilles** will take care of loading the join entity at runtime in the background. For more details, check
 [Join columns](/doanduyhai/achilles/tree/master/documentation/joinColumns.markdown)
 
 Example:

	@ManyToMany(cascade = CascadeType.ALL)
	@JoinColumn(table="my_tweets_join_column_family")
	private WideMap<UUID,Tweet> tweets;

	