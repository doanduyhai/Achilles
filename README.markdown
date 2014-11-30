![Achilles logo](assets/Achilles_New_Logo.png)

<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[![Build Status](https://travis-ci.org/doanduyhai/Achilles.png?branch=master)](https://travis-ci.org/doanduyhai/Achilles)[![Maven central](https://maven-badges.herokuapp.com/maven-central/info.archinnov/achilles-core/badge.svg)](https://maven-badges.herokuapp.com/maven-central/info.archinnov/achilles-core)

  Achilles is an **open source** Persistence Manager for **Apache Cassandra**. Among all the features:

- Advanced bean mapping (compound primary key, composite partition key, timeUUID...)
- Display of DML scripts & DDL statements
- JUnit @Rule with embedded **Cassandra** server for productive development
- Native collections and map support
- Advanced queries (slice, native or typed)
- Native support for counters
- Runtime setting of consistency level, ttl and timestamp
- Batch mode for atomic commits
- Dirty check
- All new **Cassandra 2.0** features
- Life cycle interceptors
- Bean Validation (JSR-303)
- Distributed CAS operation support
- Multi-keyspace
- Naming Strategy
- Type Transformer
- Asynchronous operations


## Installation #

 To use **Achilles**, just add the following dependency in your **pom.xml**:

```xml
	<dependency>
		<groupId>info.archinnov</groupId>
		<artifactId>achilles-core</artifactId>
		<version>3.0.12</version>
	</dependency>
```

 For unit-testing with embedded Cassandra, add this dependency with **test** scope:

```xml
 	<dependency>
 		<groupId>info.archinnov</groupId>
 		<artifactId>achilles-junit</artifactId>
 		<version>3.0.12</version>
 		<scope>test</scope>
 	</dependency>
```

 For now, **Achilles** depends on the following libraries:

 1. cassandra 2.0.11
 2. cassandra-driver-core 2.1.2
 3. CGLIB nodep 2.2.2 for proxy building
 4. Jackson core, annotations, databind & module jaxb annotations 2.3.3
 5. org.reflections 0.9.9-RC1
 6. Guava 15.0
 7. slf4j-api 1.7.2
 8. commons-io 2.4
 9. commons-lang3 3.3.2
 10. commons-collections 3.2.1
 11. validation-api 1.1.0.Final


## 5 minutes tutorial

 To boostrap quickly with **Achilles**, you can check the **[5 minutes tutorial]**

## Quick Reference

 To be productive quickly with **Achilles**. Most of useful examples are given in the **[Quick Reference]**

## Advanced tutorial

 To get a deeper look on how you can use **Achilles**, check out the **[Twitter Demo]** application and read the **[Advanced Tutorial]** section

## Documentation

 All the documentation and tutorial is available in the **[Wiki]**

 Versioned documentation is available at **[Documentation]**

## Mailing list

 For any question, bug encountered, you can use the **[mailing list]**

## License
Copyright 2012-2014 DuyHai DOAN

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this application except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

[5 minutes tutorial]: https://github.com/doanduyhai/Achilles/wiki/5-minutes-Tutorial
[Quick Reference]: https://github.com/doanduyhai/Achilles/wiki/Quick-Reference
[Twitter Demo]: https://github.com/doanduyhai/Achilles-Twitter-Demo
[Advanced Tutorial]: https://github.com/doanduyhai/Achilles/wiki/Advanced-Tutorial:-Twitter-Demo
[Wiki]: https://github.com/doanduyhai/Achilles/wiki
[Documentation]: https://github.com/doanduyhai/Achilles/tree/master/documentation/versions
[Datastax Java Driver]: https://github.com/datastax/java-driver
[mailing list]: https://groups.google.com/forum/?hl=fr#!forum/cassandra-achilles

