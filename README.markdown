![Achilles logo](assets/Achilles_New_Logo.png)

<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[![Build Status](https://travis-ci.org/doanduyhai/Achilles.png?branch=master)](https://travis-ci.org/doanduyhai/Achilles)

# Presentation #

  Achilles is an **open source** Persistence Manager for **Apache Cassandra**. Among all the features:

- Advanced bean mapping (compound primary key, composite partition key, timeUUID...)
- Display of DML scripts & DDL statements
- JUnit @Rule with embedded **Cassandra** server for productive development
- Native collections and map support
- Advanced queries (slice, native or typed)
- Native support for counters 
- Runtime setting of consistency level, ttl and timestamp
- Batch mode for atomic commits in next major release (along side with migration to **Cassandra 2.0**)
- Dirty check and lazy loading
- And more to come ...

# Installation #

 To use **Achilles**, just add the following dependency in your **pom.xml**:
 
 For **CQL** version:
 
	<dependency>	
		<groupId>info.archinnov</groupId>
		<artifactId>achilles-cql</artifactId>
		<version>2.0.9</version>
	</dependency>  
 
> **Warning!!! The Thrift version is experimental and will be dropped in the next major release. Please migrate to CQL3**

  For **Thrift** version:
 
	<dependency>	
		<groupId>info.archinnov</groupId>
		<artifactId>achilles-thrift</artifactId>
		<version>2.0.9</version>
	</dependency> 

 
 
 For now, **Achilles** depends on the following libraries:
 
 1. cassandra 1.2.8
 2. cassandra-driver-core 1.0.4 for the **CQL** version
 3. hector-core 1.1-4 for the **Thrift** version
 4. CGLIB nodep 2.2.2 for proxy building
 5. Jackson asl, mapper & xc 1.9.3 
   
  
# 5 minutes tutorial

 To boostrap quickly with **Achilles**, you can check the **[5 minutes tutorial]**

# Quick Reference

 To be productive quickly with **Achilles**. Most of useful examples are given in the **[Quick Reference]**
 
# Advanced tutorial

 To get a deeper look on how you can use **Achilles**, check out the **[Twitter Demo]** application and read the **[Advanced Tutorial]** section
 
# Documentation

 All the documentation and tutorial is available in the **[Wiki]**

# Mailing list 

 For any question, bug encourered, you can use the **[mailing list]** 

# License
Copyright 2012 DuyHai DOAN

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this application except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

[5 minutes tutorial]: https://github.com/doanduyhai/Achilles/wiki/5-minutes-Tutorial
[Quick Reference]: https://github.com/doanduyhai/Achilles/wiki/Quick-Reference
[Twitter Demo]: https://github.com/doanduyhai/Achilles-Twitter-Demo
[Advanced Tutorial]: https://github.com/doanduyhai/Achilles/wiki/Advanced-Tutorial:-Twitter-Demo
[Wiki]: https://github.com/doanduyhai/Achilles/wiki
[Datastax Java Driver]: https://github.com/datastax/java-driver
[mailing list]: https://groups.google.com/forum/?hl=fr#!forum/cassandra-achilles
