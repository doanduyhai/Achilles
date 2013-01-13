## EntityManager Operations

 Below is a list of operations supported by **Achilles** EntityManager
 
---------------------------------------  
##### find(Class<T> clazz,Object id);

 Find an entity by its id. Quite straightforward. The returned entity is in *managed* state and has all the proxies 
 of **WideMap** fields initialized.
 
---------------------------------------  
##### T merge(T entity);

 Merge the state of a *managed* entity, flush the changes to **Cassandra** and return a new *managed* entity. The 
 returned entity is in *managed* state and has all the proxies its **WideMap** fields initialized.
 
 **Achilles** implementation of *merge()* deviates a little bit from the JPA specs. The specs says that the entity 
 passed as argument of *merge()* becomes detached. With **Achilles**, the entity passed as argument remains in
 *managed* state and is returned by the *merge()* operation.
 
 Example:
 
	User user = em.find(User.class,1L);
	user.setFirstname("DuyHai");

	User mergedUser = em.merge(user);

	// mergedUser and user are the same object
	assertTrue(mergedUser.equals(user));

---------------------------------------  
##### remove(T entity);	

 Remove a *managed* entity. 

 If the entity is *transient*, the methods will raise an **IllegalStateException**.

 If the entity has external WideMap fields, all the related rows from external column family will be removed.
 Check [External wide row](/doanduyhai/achilles/tree/master/documentation/externalWideRow.markdown) for more details

 