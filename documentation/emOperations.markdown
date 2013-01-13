## EntityManager Operations

 Below is a list of operations supported by **Achilles** EntityManager

<br/>
---------------------------------------  
##### find(Class<T> clazz,Object primaryKey)

 Find an entity by its primary key. Quite straightforward. The returned entity is in *managed* state and has all the proxies 
 of **WideMap** fields initialized.

 <br/>
---------------------------------------  
##### T getReference(Class<T> entityClass, Object primaryKey) 

 Same behavior as find(Class<T> clazz,Object primaryKey) above

<br/> 
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

<br/>	
---------------------------------------  
##### remove(T entity)

 Remove a *managed* entity. 

 If the entity is *transient*, the methods will raise an **IllegalStateException**.

 If the entity has external **WideMap** fields, all the related rows from external column family will be removed.
 Check [External wide row][externalWideRow] for more details

<br/> 
---------------------------------------  
##### refresh(Object entity)  
 
 Refresh a *managed* etity. 
 
 If the entity is *transient*, the methods will raise an **IllegalStateException**.
 
 Behind the scene, **Achilles** will load the entity from **Cassandra**. All *lazy* fields that have been previously loaded
 are cleared and will be re-loaded again upon getter invocation.

<br/> 
---------------------------------------  
##### Object getDelegate()

 Simply return the current EntityManager instance.

<br/> 
---------------------------------------  
##### flush() 

 Not supported, will throw an **UnsupportedOperationException**.
	 
<br/>
---------------------------------------  
##### setFlushMode(FlushModeType flushMode) 

 Not supported, will throw an **UnsupportedOperationException**.

<br/>
---------------------------------------  
##### FlushModeType getFlushMode() 

 Not supported, will throw an **UnsupportedOperationException**.

<br/>
---------------------------------------  
##### lock(Object entity, LockModeType lockMode) 

 Not supported, will throw an **UnsupportedOperationException**.

<br/>
---------------------------------------  
##### clear() 

 Not supported, will throw an **UnsupportedOperationException**.

<br/>
---------------------------------------  
##### contains() 

 Not supported, will throw an **UnsupportedOperationException**. 

<br/>
---------------------------------------  
##### Query createQuery(String qlString)

 Not supported **yet**, will throw an **UnsupportedOperationException**. 

<br/> 
---------------------------------------  
##### Query createNamedQuery(String name)() 

 Not supported **yet**, will throw an **UnsupportedOperationException**. 

<br/> 
---------------------------------------  
##### Query createNativeQuery(String sqlString) 

 Not supported, will throw an **UnsupportedOperationException**. 

<br/> 
---------------------------------------  
##### Query createNativeQuery(String sqlString, Class resultClass) 

 Not supported, will throw an **UnsupportedOperationException**.  

<br/> 
---------------------------------------  
##### Query createNativeQuery(String sqlString, String resultSetMapping) 

 Not supported, will throw an **UnsupportedOperationException**.  

<br/> 
---------------------------------------  
##### joinTransaction()

 Not supported, will throw an **UnsupportedOperationException**.  

<br/> 
---------------------------------------  
##### close()

 Not supported, will throw an **UnsupportedOperationException**.   

<br/> 
---------------------------------------  
##### isOpen()

 Not supported, will throw an **UnsupportedOperationException**.    

<br/> 
---------------------------------------  
##### EntityTransaction getTransaction()

 Not supported, will throw an **UnsupportedOperationException**.
 

[annotations]: /doanduyhai/achilles/tree/master/documentation/annotations.markdown
[emOperations]: /doanduyhai/achilles/tree/master/documentation/emOperations.markdown
[collectionsAndMaps]: /doanduyhai/achilles/tree/master/documentation/collectionsAndMaps.markdown
[dirtyCheck]: /doanduyhai/achilles/tree/master/documentation/dirtyCheck.markdown
[simpleWideRow]: /doanduyhai/achilles/tree/master/documentation/simpleWideRow.markdown
[internalWideRow]: /doanduyhai/achilles/tree/master/documentation/internalWideRow.markdown
[externalWideRow]: /doanduyhai/achilles/tree/master/documentation/externalWideRow.markdown
[multiComponentKey]: /doanduyhai/achilles/tree/master/documentation/multiComponentKey.markdown
[joinColumns]: /doanduyhai/achilles/tree/master/documentation/joinColumns.markdown