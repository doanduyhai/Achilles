### Compatible JPA Annotations

 For bean mapping, only field-based access type is supported by **Achilles**. Furthermore, there is no default mapping 
 for fields. If you want a field to be mapped, you must annotate it with @Column or @JoinColumn. All fields that are not
 annotated by either annotations is considered transient by **Achilles**
 
 Below is a list of all JPA annotations supported by **Achilles**. 
 
##### @Table

 Indicates that an entity is candidate for persistence. When then *name* attribute is filled, it indicates the name
 of the column family used by this entity.
 
 Example:
 
	@Table(name = "users_column_family")
	public class User implements Serializable
	{
		private static final long serialVersionUID = 1L;
		...
		...
	}

>	Please note that all entities must implement the `java.io.Serializable`	interface and provide a serialVersionUID.
	Failing to meet this requirement will trigger a BeanMappingException.

	
##### @Id

 Marks a field as the primary key for the entity. The primary key can be of any type, even a plain POJO. However it must 
 implement the `java.io.Serializable` interface otherwise a AchillesException will be raised.

 Under the hood, the primary key will be serialized to bytes array and  used as row key (partition key) by **Cassandra** engine.

 
 