## EntityManager Operations

 Below is a list of operations supported by **Achilles** EntityManager


 
---------------------------------------  
##### find(Class<T> clazz,Object primaryKey)

 Find an entity by its primary key. Quite straightforward. The returned entity is in *managed* state and has all the proxies 
 of **WideMap** fields initialized.

---------------------------------------  
##### T getReference(Class<T> entityClass, Object primaryKey) 

 Same behavior as find(Class<T> clazz,Object primaryKey) above

 
 
---------------------------------------  
##### T merge(T entity)

 Merge the state of a *managed* entity, flush the changes to **Cassandra** and return a new *managed* entity. The 
 returned entity is in *managed* state and has all the proxies its **WideMap** fields initialized.
 
> **Achilles** implementation of *merge()* deviates a little bit from the JPA specs. The specs says that the entity 
> passed as argument of *merge()* becomes detached. With **Achilles**, the entity passed as argument remains in
> *managed* state and is returned by the *merge()* operation.
 
 Example:
 
	User user = em.find(User.class,1L);
	user.setFirstname("DuyHai");

	User mergedUser = em.merge(user);

	// mergedUser and user are the same object
	assertTrue(mergedUser.equals(user));

---------------------------------------  
##### remove(T entity)

 Remove a *managed* entity. 

 If the entity is *transient*, the methods will raise an **IllegalStateException**.

 If the entity has external **WideMap** fields, all the related rows from external column family will be removed.
 Check [External wide row](/doanduyhai/achilles/tree/master/documentation/externalWideRow.markdown) for more details

---------------------------------------  
##### refresh(Object entity)  
 
 Refresh a *managed* etity. 
 
 If the entity is *transient*, the methods will raise an **IllegalStateException**.
 
 Behind the scene, **Achilles** will load the entity from **Cassandra**. All *lazy* fields that have been previously loaded
 are cleared and will be re-loaded again upon getter invocation.

---------------------------------------  
##### Object getDelegate()

 Simply return the current EntityManager instance.
 
---------------------------------------  
##### flush() 

 Not supported, will throw an **UnsupportedOperationException**.
	 

---------------------------------------  
##### setFlushMode(FlushModeType flushMode) 

 Not supported, will throw an **UnsupportedOperationException**.


---------------------------------------  
##### FlushModeType getFlushMode() 

 Not supported, will throw an **UnsupportedOperationException**.


---------------------------------------  
##### lock(Object entity, LockModeType lockMode) 

 Not supported, will throw an **UnsupportedOperationException**.


---------------------------------------  
##### clear() 

 Not supported, will throw an **UnsupportedOperationException**.


---------------------------------------  
##### contains() 

 Not supported, will throw an **UnsupportedOperationException**. 


---------------------------------------  
##### Query createQuery(String qlString)

 Not supported **yet**, will throw an **UnsupportedOperationException**. 

---------------------------------------  
##### Query createNamedQuery(String name)() 

 Not supported **yet**, will throw an **UnsupportedOperationException**. 

---------------------------------------  
##### Query createNativeQuery(String sqlString) 

 Not supported, will throw an **UnsupportedOperationException**. 

---------------------------------------  
##### Query createNativeQuery(String sqlString, Class resultClass) 

 Not supported, will throw an **UnsupportedOperationException**.  
 
---------------------------------------  
##### Query createNativeQuery(String sqlString, String resultSetMapping) 

 Not supported, will throw an **UnsupportedOperationException**.  

---------------------------------------  
##### joinTransaction()

 Not supported, will throw an **UnsupportedOperationException**.  

---------------------------------------  
##### close()

 Not supported, will throw an **UnsupportedOperationException**.   

---------------------------------------  
##### isOpen()

 Not supported, will throw an **UnsupportedOperationException**.    
 
---------------------------------------  
##### EntityTransaction getTransaction()

 Not supported, will throw an **UnsupportedOperationException**.