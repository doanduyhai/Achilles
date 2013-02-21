# Achilles #

# Presentation #

 Achilles is an open source Entity Manager for Apache Cassandra. Among all the features:
 
 - Dirty check for simple and collection/map type properties
 - Lazy loading 
 - Collections and map support
 - Native wide row mapping with dedicated annotations
 - Support for Cassandra wide rows mapping inside entities
 - Support for multi components column name with wide row
 - Join columns with cascading support


# Installation #

 To use **Achilles**, just add the following dependency in your **pom.xml**:
 
	<dependency>	
		<groupId>info.archinnov</groupId>
		<artifactId>achilles</artifactId>
		<version>1.1-beta</version>
	</dependency>  
 
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
 4. Persistence-api 1.0.2
 5. Jackson asl, mapper & xc 1.9.3 
   
  
 

# Documentation #

1. [5 minutes tutorial][quickTuto]
2. [JPA and custom annotations for bean mapping][annotations]
3. [Supported operations for EntityManager] [emOperations]
4. [Collections and Map][collectionsAndMaps]
5. [Dirty check][dirtyCheck]
6. [WideMap API][wideMapAPI]
7. [Internal WideMap][internalWideMap]
8. [External WideMap][externalWideMap]
9. [Direct Column Family Mapping][cfDirectMapping]
10. [Multi components for wide row][multiComponentKey]
11. [Join columns][joinColumns]
12. [Manual Column Family Creation][manualCFCreation]
13. [JSON Serialization][json]
14. [Performance][perf]

# License #
Copyright 2012 DuyHai DOAN

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this application except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
 

[quickTuto]: /documentation/quickTuto.markdown
[annotations]: /documentation/annotations.markdown
[emOperations]: /documentation/emOperations.markdown
[collectionsAndMaps]: /documentation/collectionsAndMaps.markdown
[dirtyCheck]: /documentation/dirtyCheck.markdown
[wideMapAPI]: /documentation/wideMapAPI.markdown
[internalWideMap]: /documentation/internalWideMap.markdown
[externalWideMap]: /documentation/externalWideMap.markdown
[cfDirectMapping]: /documentation/cfDirectMapping.markdown
[multiComponentKey]: /documentation/multiComponentKey.markdown
[joinColumns]: /documentation/joinColumns.markdown
[manualCFCreation]:  /documentation/manualCFCreation.markdown
[json]: /documentation/jsonSerialization.markdown
[perf]: /documentation/performance.markdown