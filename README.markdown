# Achilles #

### Presentation #

 Achilles is an open source Entity Manager for Apache Cassandra. Among all the features:
 
 * dirty check
 * lazy loading
 * collections and map support (they can be defined lazy or eager) 
 * support for Cassandra wide rows inside entities
 * support for multi components column name with wide row
 * join columns (restricted to @ManyToOne and @ManyToMany, check documentation for more details)
 * native wide row mapping with dedicated annotation

### Installation #

 For the moment the framework is not available yet as a maven repo (it will be soon) so just clone or fork the 
 project:
 
>	git clone https://github.com/doanduyhai/Achilles.git

 For now, **Achilles** depends on the following libraries:
 
 1. Cassandra 1.1.6 (will be upgraded soon to 1.2)
 2. Hector-core 1.0-5 (**Achilles** is built upon Hector API) 

### Documentation #

>	1. [Available JPA annotations and their semantics in **Achilles**](/documentation/annotations.markdown)
>	2. [Supported operations for EntityManager](/documentation/annotations.markdown)
>	3. [Collections and Map with **Achilles**](/documentation/annotations.markdown)
>	4. [Dirty check](/documentation/annotations.markdown)
>	5. [Internal wide row](/documentation/annotations.markdown)
>	6. [External wide row](/documentation/annotations.markdown)
>	7. [Multi components for wide row](/documentation/annotations.markdown)
>	8. [Join columns](/documentation/annotations.markdown)

 
### 5 minutes tutorial #

##### Bootstrap #

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

##### Bean Mapping #

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
 
##### Usage #####	


 First we create an **User** and persist it:
 
	EntityManager em = entityManagerFactory.createEntityManager();

	User user = new User();
	user.setId(1L);
	user.setFirstname("DuyHai");
	user.setLastname("DOAN");
	user.setAge(30);

	Set<String> tags = new HashSet<String>();
	tags.add("computing");
	tags.add("java");
	tags.add("cassandra");

	user.setFavoriteTags(tags);

	em.persist(user);

 Then we find it by id:
	
	User foundUser = em.find(User.class,1L);
	
	// Now add some new tags
	foundUser.getFavoriteTags().add("achilles"); 
	
	// Save it
	em.merge(foundUser);
	
	

