## Join columns

##### Rationale 

 Most of NoSQL technologies recommend denormalizing (duplicating) data to improve performances but there are use cases where 
 you cannot duplicate data, for example with *shared* entities.

 When an entity is *shared*, it can be accessed by many other entities and its state modified. If this *shared* entity content
 is duplicated, you must manage the state by tracking all copies of the entity to apply changes, a nightmare.

 For such use cases, it is totally relevant to keep just one copy of the entity and dispatch only share its *id*. That's how
 join columns work in the SQL.


 

 

 
