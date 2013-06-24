![Achilles logo](assets/Achilles_Logo_Reversed.png)
# Presentation #

 Achilles is an open source Entity Manager for Apache Cassandra. Among all the features:
 
 - Dirty check for simple and collection/map type properties
 - Lazy loading 
 - Collections and map support
 - Native wide row mapping with dedicated annotations
 - Support for Cassandra wide rows mapping inside entities
 - Support for multi components column name with wide row
 - Join columns with cascading support
 - Support for counters
 - Support for custom consistency levels
 - Batch mode to boost performance

# Installation #

 To use **Achilles**, just add the following dependency in your **pom.xml**:
 
	<dependency>	
		<groupId>info.archinnov</groupId>
		<artifactId>achilles-thrift</artifactId>
		<version>1.8.1</version>
	</dependency>  
 
 Right now, only the **Thrift** implementation is available. The **CQL** version is in progress and relies on 
 **[Datastax Java Driver]** which is still in beta.
 
 The framework has been released on **Sonatype OSS** repository so make sure you have the following
 entry in your **pom.xml**:
 
 	<repository>
		<id>Sonatype</id>
		<name>oss.sonatype.org</name>
		<url>http://oss.sonatype.org</url>
	</repository>
 
 For now, **Achilles** depends on the following libraries:
 
 1. Cassandra 1.2.0
 2. Hector-core 1.1-2 ( **Achilles** ThriftEntityManager is built upon Hector API)
 3. CGLIB nodep 2.2.2 for proxy building
 4. hibernate-jpa-2.0-api 1.0.1.Final (no reference jar for JPA 2, only vendor specific ones are available)
 5. Jackson asl, mapper & xc 1.9.3 
   
  
# 5 minutes tutorial

 To boostrap quickly with **Achilles**, you can check the **[5 minutes tutorial]**

# Advanced tutorial

 To get a deeper look on how you can use **Achilles**, check out the **[Twitter Demo]** application and read the **[Advanced Tutorial]** section
 
# Documentation

 All the documentation and tutorial is available in the **[Wiki]**

# License
Copyright 2012 DuyHai DOAN

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this application except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

# Changes log

* **1.8.2**:
    * Remove
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
[Twitter Demo]: https://github.com/doanduyhai/Achilles-Twitter-Demo
[Advanced Tutorial]: https://github.com/doanduyhai/Achilles/wiki/Advanced-Tutorial:-Twitter-Demo
[Wiki]: https://github.com/doanduyhai/Achilles/wiki
[Datastax Java Driver]: https://github.com/datastax/java-driver
