![Achilles](https://raw.github.com/wiki/doanduyhai/Achilles/assets/Achilles_New_Logo.png)

<br/>

[![Build Status](https://travis-ci.org/doanduyhai/Achilles.png?branch=master)](https://travis-ci.org/doanduyhai/Achilles)

## Presentation #

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
- Support for **Materialized View** 
- Support for typed-safe **Function calls**
- Support for the new **JSON** API
- Support for multi-project compilation unit
- Support for native index, **SASI** and **DSE Search**
- Support for **`GROUP BY`** since **Cassandra 3.10** and **DSE 5.1.x**
- Flexible naming strategy & insert strategy
- Runtime **Schema Name Provider** for multi-tenant environments
- Full compatibility with Java 8 **CompletableFuture**

> **Warning: Achilles versions 5.x are no longer maintained, only bug-fixes are supported, please migrate to
version 6.x and follow the [Migration From 5.x Guide]**

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
            <td>6.1.0 (all Cassandra versions up to 3.11.10, all DSE up to 5.1.10)</td>
            <td>3.11.0</td>
            <td>3.11.10</td>
        </tr>
        <tr>
            <td>5.2.1 (all Cassandra versions up to 3.7, all DSE up to 5.0.3)</td>
       	    <td>3.1.3</td>
            <td>3.7</td>
        </tr>   
        <tr>
            <td>5.0.0 (all Cassandra versions up to 3.7, all DSE up to 5.0.3)</td>
            <td>3.1.0</td>
            <td>3.7</td>
        </tr> 
	<tr>
	    <td>4.2.3 (all Cassandra versions up to 3.7, all DSE up to 5.0.3)</td>
	    <td>3.1.0</td>
	    <td>3.7</td>
	</tr>		
	<tr>
    	    <td>4.0.1 (limited to Cassandra 2.2.3 features)</td>
	    <td>3.0.0-alpha5</td>
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

> Warning: there will be no new features for branches older than **5.0.x**. Those branches are
only supported for bug fixes. New features will **not** be back-ported. Please upgrade to the
latest version of **Achilles** to benefit from new features

 To use **Achilles**, just add the following dependency in your **pom.xml**:

```xml
	<dependency>
		<groupId>info.archinnov</groupId>
		<artifactId>achilles-core</artifactId>
		<version>${achilles.version}</version>
	</dependency>
```

 Do not forget to deactivate _incremental compilation_ and use _Java 8_ in your **pom.xml** file
 
```xml
    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <configuration>
                    <source>1.8</source>
                    <target>1.8</target>
                    <useIncrementalCompilation>false</useIncrementalCompilation>
                    <annotationProcessors>
                        <annotationProcessor>info.archinnov.achilles.internals.apt.processors.meta.AchillesProcessor</annotationProcessor>
                    </annotationProcessors>
                </configuration>
            </plugin>
        </plugins>
    </build>        
```
            
> Achilles 5.x requires a JDK 8 to work. It is recommended to use JDK 8 update 45 or later

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
 3. Jackson core, annotations, databind & module jaxb annotations 2.10.0
 4. Google Auto Common 0.4
 5. Google Auto Service 1.0-rc2
 6. Java Poet 1.5.1 
 7. Guava 24.1.1-jre
 8. slf4j-api 1.7.2
 9. commons-io 2.4
 10. commons-lang3 3.3.2
 11. commons-collections 3.2.1
 12. validation-api 1.1.0.Final
 13. org.eclipse.jdt.core.compiler-ecj 4.4.2

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
Copyright 2012-2016 DuyHai DOAN

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this application except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

[Migration From 3.x Guide]: https://github.com/doanduyhai/Achilles/wiki/Migration-Guide
[IDE Configuration]: https://github.com/doanduyhai/Achilles/wiki/IDE-configuration
[5 minutes tutorial]: https://github.com/doanduyhai/Achilles/wiki/5-minutes-Tutorial
[Quick Reference]: https://github.com/doanduyhai/Achilles/wiki/Quick-Reference
[Twitter Demo]: https://github.com/doanduyhai/Achilles-Twitter-Demo
[KillrChat]: https://github.com/doanduyhai/Achilles/wiki/Advanced-Tutorial:-KillrChat
[Migration Guide]: https://github.com/doanduyhai/Achilles/wiki/Migration-Guide
[Wiki]: https://github.com/doanduyhai/Achilles/wiki
[Documentation]: https://github.com/doanduyhai/Achilles/tree/master/documentation/versions
[Datastax Java Driver]: https://github.com/datastax/java-driver
[mailing list]: https://groups.google.com/forum/?hl=fr#!forum/cassandra-achilles

