## FAQs

1. How to bootstrap **Achilles** with **Spring** ?

	There is a Gist showing how to create a simple FactoryBean for **Achilles**
	ThriftEntityManager here: https://gist.github.com/doanduyhai/5008510
	
2. Can I use **Achilles** and **Hibernate** (or any JPA provider) at the same time ?

	Yes, but you need to remove all *@Entity* annotation on **Achilles** entities
	and use the *@Table* annotation instead. Indeed all **Achilles** entities
	having the *@Entity* annotation will be parsed by **Hibernate** and it will complain
	since the **WideMap** type is unknown and furthermore it cannot find any SQL table
	that map to your entities.
	
	Alternatively if you're using **Spring**, you can give a list of explicit entities
	to be scanned by **Hibernate** so in this case no issue
	
	If you're using **Hibernate** HBM files for mapping instead of annotations, it's also
	fine
	
3. Will **Achilles** support CQL3 ?

	Yes, in a very near future, as soon as the Java driver from Datastax will be available
	on a public Maven repo.
	
	There will be a CQL3EntityManager, with most of the features of ThriftEntityManager. 
	There will still be some features mismatching because of the difference between the 
	Thrift and CQL3 protocole but most of the core features will be there

4. How can I set custom consistency level for each entity/property

	The feature is the next to be implemented
	
5. Will there be any support for secondary index

	The feature is in the pipe, though having less priority than the CQL3EntityManager 		 	  	

6.  Any support for property indexing & text search ?

	Technically possible by putting Lucence in the loop but lots of work to do. You're 
	welcomed to help. 
	