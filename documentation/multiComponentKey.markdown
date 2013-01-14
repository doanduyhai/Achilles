## Multi Component Key for Wide Row

 We have seen so far that the **WideMap** interface allows us to access easily to Slice Range API without headache. However an important
 feature is missing: composite column key.
 
 Indeed it is possible to index your columns in **Cassandra** with **Composite** keys. Composite key are column key with several 
 components of different type (Integer,String,Long ...)
 
 **Achilles** implements this feature by providing a marker interface **MultiKey** as well as a custom annotation *@Key*.
 
 To make a POJO becomes a multi component column key, all you need is:
 
 - make it implement the **MultiKey**
 - annotate some of its fields with *@key*, not forgetting to specify the *order* attribute
<br/>

Example:

	public class UserIndex implements MultiKey
	{
		@Key(order=1)
		private String login;
		
		@Key(order=2)
		private Long id;
	
		// Getters and setters
	}
	
	@Table(name="users_column_family")
	public class User implements Serializable
	{

		private static final long serialVersionUID = 1L;

		@Id
		private Long id;

		@Column
		private String firstname;
		
		...
		
		@Column(table="friends_column_family")
		private WideMap<UserIndex, String> friends; 

		@Column(table="followers_column_family")
		private WideMap<UserIndex, String> followers;
	
	}

 Above we create an UserIndex POJO with *login* as first component and *id* as last component. The couple (login,id) will identify 
 uniquely an user. In fact the *id* itself is sufficient but we'll see that shortly why having the *login* as first component is 
 better.


 Internally,  **Achilles** will create 2 column families *friends\_column\_family* & *friends\_column\_family* with:
 
 1. Key validation class of type **java.lang.Long**, the class of *@Id* field
 2. Comparator of type *'Composite(UTF8Type,LongType)'* which represents the **UserIndex** key type for *friends* and *followers* fields
 3. Validation class of type **java.lang.String**  which represents the value type for *friends* and *followers* fields
<br/>
 
Indeed, the value of *friends* and *followers* external wide rows can be set to empty string since the **UserIndex** key contains
 all the necessary information to find an user.
 
 To insert values into these wide rows, you must build an **UserIndex** instance for each value:
 
	User user = em.find(User.class,10L);
	
	UserIndex foo = new UserIndex();
	foo.setLogin("foo");
	foo.setId(150L);
	
	UserIndex bar = new UserIndex();
	bar.setLogin("bar");
	bar.setId(123L);
	
	// Add "foo" as friend
	user.getFriends().insert(foo,"");
	
	// Add "bar" as follower
	user.getFollowers().insert("bar","");
	

 Now if we want to search for friends whose login start with **"john"** :

	UserIndex friendStart = new UserIndex();
	friendStart.setLogin("john");
	
	UserIndex friendEnd = new UserIndex();
	friendEnd.setLogin("johm");
	
	
	List<KeyValue<UserIndex,String>> foundFriends = user.getFriends().findRange(friendStart,true,friendEnd,false,false,100);
	
 Above, we are doing a slice range query with composite value. The *friendStart* and *friendEnd* keys define the bounds for the
 search. Please note that the second component of **UserIndex**, the *id*, is let to **null**. It does not matter as long as the first
 component *login* is filled.
 
 The above query will return all friends:
 
 - whose login starts with **"john"**
 - whose id can be of any value
<br/>

We set *friendEnd* login to **"johm"**, **"m"** being the letter immediately after **"n"** and we exclude this from the search. So:
 
 - "john" will match of course (because the *friendStart* bound is inclusive)
 - "johnny" will match
 - "johnxxxx" will match
 - "johm" will not match because *friendEnd* bound is exclusive
<br/>

#### Multi Key component validation rules
 
 Generally, a multikey object can be used with **WideMap** field as any normal types supported by **Cassandra** for insert,
search and deletion operations provided that:
 
 - for insert operations, all the component (fields annotated with *@Key*) must be filled (not null)
 - for find and delete operations, the multikey should meet the following requirements
 
	>	1. be null or	
	> 	2. has some null components but without "hole" ( **null**) between components
<br/>

Let's define a fake multi key class to illustrate this:

	public class MyKey implements MultiKey
	{	
		@Key(order=2)
		private String name;
	
		@Key(order=3)
		private UUID time;
		
		@Key(order=1)
		private Long id;
	}
	
 As you can see the component declaration lexical order does not matter, only the *order* attribute is taken into account.
 
 The following multikeys are valid:
 
 - null
 - {id=1L,null,null}
 - {id=1L,name="test",null}
 - {id=1L,name="test",time='e39707f0-5a6c-11e2-ab85-685d43d1d7d3'}
<br/>
 
The following are not valid:

 - {null,name="test",null}
 - {null,name="test",time='e39707f0-5a6c-11e2-ab85-685d43d1d7d3'}
 - {id=1L,null,time='e39707f0-5a6c-11e2-ab85-685d43d1d7d3'}
 
 
#### Multi Key ordering rules 

 When doing slice queries with starting and ending bounds in **Cassandra**, you must ensure that the starting bound is less
 than ending bound, with respect to the natural ordering of the type.

 For simple types like Integer or String, the natural ordering is obvious. For multi components keys, the ordering is obtained
 by comparing component by component with regard to their order in the key:

 - first component of starting bound with the one of the ending bound
 - second component of starting bound with the one of the ending bound
 - etc..
<br/>

For example:

 1. {id=10L,name="test",null} > {id=1L,name="test",null} because 10L > 1L
 2. {id=10L,null,null} > {id=1L,name="test",null} still because 10L > 1L. The *name* components do not matter as long as the *id* components
    are	not equal
 3. {id=10L,name="test",null} = {id=10L,name="test",null} 
 4. {id=10L,name="test",null} > {id=10L,name="xxx",time='e39707f0-5a6c-11e2-ab85-685d43d1d7d3'}  because _"test"_ > _"xxx"_



 
 	
	
