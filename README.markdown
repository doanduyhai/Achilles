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

 For the moment the framework is not available yet as a maven repo (it will be soon) so just clone or fork the 
 project:
 
>	git clone https://github.com/doanduyhai/Achilles.git

 For now, **Achilles** depends on the following libraries:
 
 1. Cassandra 1.1.6 (will be upgraded soon to 1.2)
 2. Hector-core 1.0-5 (<strong>Achilles</strong> is built upon Hector API) 
   
  
 Install **Achilles** jar into your local Maven repository:
 
>	mvn clean install 

 Then add the following dependency in your **pom.xml**:
 
	<dependency>	
		<groupId>fr.doan</groupId>
		<artifactId>achilles</artifactId>
		<version>1.0-beta</version>
	</dependency>  

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


# License #
Copyright 2012 DuyHai DOAN

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this application except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
 

[quickTuto]: /doanduyhai/achilles/tree/master/documentation/quickTuto.markdown	
[annotations]: /doanduyhai/achilles/tree/master/documentation/annotations.markdown
[emOperations]: /doanduyhai/achilles/tree/master/documentation/emOperations.markdown
[collectionsAndMaps]: /doanduyhai/achilles/tree/master/documentation/collectionsAndMaps.markdown
[dirtyCheck]: /doanduyhai/achilles/tree/master/documentation/dirtyCheck.markdown
[wideMapAPI]: /doanduyhai/achilles/tree/master/documentation/wideMapAPI.markdown
[internalWideMap]: /doanduyhai/achilles/tree/master/documentation/internalWideMap.markdown
[externalWideMap]: /doanduyhai/achilles/tree/master/documentation/externalWideMap.markdown
[cfDirectMapping]: /doanduyhai/achilles/tree/master/documentation/cfDirectMapping.markdown
[multiComponentKey]: /doanduyhai/achilles/tree/master/documentation/multiComponentKey.markdown
[joinColumns]: /doanduyhai/achilles/tree/master/documentation/joinColumns.markdown
[manualCFCreation]:  /doanduyhai/achilles/tree/master/documentation/manualCFCreation.markdown
[perf]: /doanduyhai/achilles/tree/master/documentation/perf.markdown  	
