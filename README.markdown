![Achilles logo](assets/Achilles_New_Logo.png)

<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[![Build Status](https://travis-ci.org/doanduyhai/Achilles.png?branch=master)](https://travis-ci.org/doanduyhai/Achilles)

# Presentation #

 Achilles is an open source Entity Manager for Apache Cassandra. Among all the features:
 
 - Dirty check for simple and collection/map type properties
 - Lazy loading 
 - Collections and map support
 - Support for Cassandra clustered entities with compound primary key
 - Join columns with cascading support
 - Support for counters
 - Support for custom consistency levels
 - Batch mode for atomic commits (atomicity only available for **CQL** impl)

# Installation #

 To use **Achilles**, just add the following dependency in your **pom.xml**:
 
 For **CQL** version:
 
	<dependency>	
		<groupId>info.archinnov</groupId>
		<artifactId>achilles-cql</artifactId>
		<version>2.0.5</version>
	</dependency>  
 
  For **Thrift** version:
 
	<dependency>	
		<groupId>info.archinnov</groupId>
		<artifactId>achilles-thrift</artifactId>
		<version>2.0.5</version>
	</dependency> 

 
 
 For now, **Achilles** depends on the following libraries:
 
 1. cassandra 1.2.8
 2. cassandra-driver-core 1.0.2 for the **CQL** version
 3. hector-core 1.1-2 for the **Thrift** version
 3. CGLIB nodep 2.2.2 for proxy building
 4. hibernate-jpa-2.0-api 1.0.1.Final (no reference jar for JPA 2, only vendor specific ones are available)
 5. Jackson asl, mapper & xc 1.9.3 
   
  
# 5 minutes tutorial

 To boostrap quickly with **Achilles**, you can check the **[5 minutes tutorial]**

# Quick Reference

 To be productive quickly with **Achilles**. Most of useful examples are given in the **[Quick Reference]**
 
# Advanced tutorial

 To get a deeper look on how you can use **Achilles Thrift version**, check out the **[Twitter Demo]** application and read the **[Advanced Tutorial]** section
 
# Documentation

 All the documentation and tutorial is available in the **[Wiki]**

# License
Copyright 2012 DuyHai DOAN

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this application except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

# Changes log

* **2.0.5**:
    * Minor refactor for embedded Cassandra server and test resources
    * Display bound values in DML debug messages for bound statements
    * Introduce Options to simplify setting of TTL and Timestamp
    * Force initialization of counter type when calling `initialize()`
    * Bug with bi-directional relation with a cascade persist/merge and 'achilles.consistency.join.check' option set to true 
* **2.0.4**:
    * Migrate to Cassandra 1.2.8
    * Fix NPE when no join entity in collection/map for CQL
    * Remove Cassandra Unit library to avoid dependency on Hector for CQL version
    * Add Cassandra Embedded server. Add JUnit rule to bootstrap Cassandra embedded server and Achilles together
* **2.0.3**:
    * Fix buggy implementation of FactoryBean for Spring integration
    * Fix bug in decoding enums for Slice Queries 
* **2.0.2**:
    * Internal refactoring of PropertyMeta
    * Support for value-less entities
    * Remove `@CompoundKey` annotation, un-used
* **2.0.1**:
    * Add Spring Integration for CQL 
    * Add new parameters for **CQLEntityManagerFactory**
    * Enhance **SliceQueryBuilder** API
    * Fix bug on Thrift clustered entity validation
    * Fix small bug on CQL RawTyped queries
* **2.0.0**:
    * **Official support for CQL3**. See docs for more details    
    * Remove custom `Pair` type, use the one provided by Cassandra
    * Fixes bug during entities parsing when List/Set/Map have parameterized typse
    * Add `removeById()` to EntityManager to avoid read-before-write pattern
    * **BREAKING CHANGE**, removal of `WideMap<K,V>` structure because its use cases are not consistent
    * Add DML logs to display CQL statements at runtime
    * Serialize **enum** types using name()
    * **BREAKING CHANGE**, removal of `@WideRow` annotation, replaced by clustered entities and slice queries for getting data (see doc for more details) . 
* **1.8.2**:
    * Support immutability for `@CompoundKey` classes (Issue #5). See https://github.com/doanduyhai/Achilles/issues/5#issuecomment-19882998 for more details on new syntax
    * **BREAKING CHANGE**, for `@CompoundKey`, enum types are serialized as String instead of Object (Issue #8)
    * **BREAKING CHANGE**, replace `@Key(order=1)` by `@Order(1)` annotation (Issue #6)
    * Throw Exception instead of having NPE when keyspace is not created in Cassandra (Issue #10)
    * **BREAKING CHANGE**, replace `MultiKey`interface by `@CompoundKey` annotation (Issue #15)
    * It is no longer required to implements `Serializable`on all entities (Issue #11)
    * Enforce consistency im ThriftImpl on `persist()` by removing all row data first (Issue #14 & Issue #7)
    * Add timestamp meta data to `KeyValye<K,V>` type (Issue #9)
    * Add initAndUnwrap() shortcut to EntityManager (Issue #2)
    * Fix bug on dirty check on join collection/map
* **1.8.1**:
    * Add TTL to persist and merge operations
    * Rework of runtime consistency level
    * Fix small bug in the Column Family comment for wide rows
    * **BREAKING CHANGES** simplify PropertyType byte flag for Thrift persistence layer. **Need data migration**
    * Rename 'unproxy' to 'unwrap'
    * Fix bug with cyclic join entities during merge and persist operations
    * Fix buggy property removal after merging
    * Ignore un-mapped properties instead of raising exception
    * Renamming in core package
    * Upgrade to Cassandra 1.2.4
    
* **1.8.0**:
    * Use the `org.reflections.reflections` package for entity parsing
    * Split the project into 3 modules: `core`, `thrift` and `cql`
    
* **1.7.3**:
    * Rework of em.getReference() to avoid hitting the database
    * Refactor generics
    * Migrate to JPA 2
   
* **1.7.2**:
    * Add commons-collections 3.2.1 to compile dependency
    * Enhance error message for entity mapping
    * Fix bug about default consistency level hard-coded to QUORUM for WideMap and Counters 
    * Add `firstFirstMatching()` and `findLastMatching()` to the WideMap API
    * Fix bug. Do not load join entity if no join primary keys
    
* **1.7.1**: fix bug because key validation class & comparator type has changed from Cassandra 1.1 to 1.

* **1.7**: stable release


[5 minutes tutorial]: https://github.com/doanduyhai/Achilles/wiki/5-minutes-Tutorial
[Quick Reference]: https://github.com/doanduyhai/Achilles/wiki/Quick-Reference
[Twitter Demo]: https://github.com/doanduyhai/Achilles-Twitter-Demo
[Advanced Tutorial]: https://github.com/doanduyhai/Achilles/wiki/Advanced-Tutorial:-Twitter-Demo
[Wiki]: https://github.com/doanduyhai/Achilles/wiki
[Datastax Java Driver]: https://github.com/datastax/java-driver
