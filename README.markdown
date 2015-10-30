![Achilles logo](assets/Achilles_New_Logo.png)

<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[![Build Status](https://travis-ci.org/doanduyhai/Achilles.png?branch=master)](https://travis-ci.org/doanduyhai/Achilles)[![Maven central](https://maven-badges.herokuapp.com/maven-central/info.archinnov/achilles-core/badge.svg)](https://maven-badges.herokuapp.com/maven-central/info.archinnov/achilles-core)

  Achilles is an **open source**  advanced object mapper for **Apache Cassandra**. Among all the features:

- Advanced bean mapping (compound primary key, composite partition key, timeUUID, counter, static column ...)
- Pluggable **codec system** to define your own types
- Life cycle **interceptors** to define custom behavior before INSERT/UPDATE/DELETE/SELECT operations
- Fluent **options system** to parameter runtime statements (consistency level, retry policy, ...)
- Powerful and **type-safe DSL** to create your own queries
- Display of DML scripts & DDL statements
- Wrapper to deploy an embedded **Cassandra** server easily
- Tight integration with JUnit for productive TDD programming
- Support for Bean Validation (JSR-303)
- Support for **Lightweight Transaction** with dedicated listener interface 
- Flexible naming strategy & insert strategy
- Runtime **Schema Name Provider** for multi-tenant environments
- Full compatibility with Java 8 **CompletableFuture**


## Installation #

Below is the compatibility matrix between **Achilles**, **Java Driver** and **Cassandra** versions

<br/>
<table border="1">
	<thead>
		<tr>
			<th>Achilles version</th>
			<th>Java Driver version</th>
			<th>Cassandra version</th>
		</tr>
	</thead>
	<tbody>
		<tr>
			<td>4.0.0</td>
			<td>2.2.0-rc3</td>
			<td>2.2.3</td>
		</tr>		
		<tr>
			<td>3.2.3 (limited to Cassandra 2.1.x features)</td>
			<td>2.1.6</td>
			<td>2.1.5</td>
		</tr>		
		<tr>
			<td>3.0.22 (limited to Cassandra 2.0.x features)</td>
			<td>2.1.6</td>
			<td>2.0.15</td>
		</tr>
    </tbody>
</table>    

 To use **Achilles**, just add the following dependency in your **pom.xml**:

```xml
	<dependency>
		<groupId>info.archinnov</groupId>
		<artifactId>achilles-core</artifactId>
		<version>${achilles.version}</version>
	</dependency>
```

 For unit-testing with embedded Cassandra, add this dependency with **test** scope:

```xml
 	<dependency>
 		<groupId>info.archinnov</groupId>
 		<artifactId>achilles-junit</artifactId>
 		<version>${achilles.version}</version>
 		<scope>test</scope>
 	</dependency>
```
 
 
 For now, **Achilles** depends on the following libraries:
 
 1. cassandra (see matrix version above)
 2. cassandra-driver-core (see matrix version above)
 3. Jackson core, annotations, databind & module jaxb annotations 2.3.3
 4. Google Auto Common 0.4
 5. Google Auto Service 1.0-rc2
 6. Java Poet 1.2.0 
 7. Guava 18.0
 8. slf4j-api 1.7.2
 9. commons-io 2.4
 10. commons-lang3 3.3.2
 11. commons-collections 3.2.1
 12. validation-api 1.1.0.Final

## Configure Your IDE

 **Achilles** is using code generation at compile time through annotation processors, you'll need to configure your IDE carefully. 
 Please follow the **[IDE Configuration]** guide

## 5 minutes tutorial

 To boostrap quickly with **Achilles**, you can check the **[5 minutes tutorial]**

## Quick Reference

 To be productive quickly with **Achilles**. Most of useful examples are given in the **[Quick Reference]**

## Advanced tutorial

 To get a deeper look on how you can use **Achilles**, check out the **[KillrChat]** application

## Documentation

 All the documentation and tutorial is available in the **[Wiki]**

 Versioned documentation is available at **[Documentation]**

## Mailing list

 For any question, bug encountered, you can use the **[mailing list]**

## License
Copyright 2012-2015 DuyHai DOAN

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this application except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

[IDE Configuration]: https://github.com/doanduyhai/Achilles/wiki/IDE-configuration
[5 minutes tutorial]: https://github.com/doanduyhai/Achilles/wiki/5-minutes-Tutorial
[Quick Reference]: https://github.com/doanduyhai/Achilles/wiki/Quick-Reference
[Twitter Demo]: https://github.com/doanduyhai/Achilles-Twitter-Demo
[KillrChat]: https://github.com/doanduyhai/Achilles/wiki/Advanced-Tutorial:-KillrChat
[Wiki]: https://github.com/doanduyhai/Achilles/wiki
[Documentation]: https://github.com/doanduyhai/Achilles/tree/master/documentation/versions
[Datastax Java Driver]: https://github.com/datastax/java-driver
[mailing list]: https://groups.google.com/forum/?hl=fr#!forum/cassandra-achilles