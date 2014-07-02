/*
 * Copyright (C) 2012-2014 DuyHai DOAN
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package info.archinnov.achilles.internal.metadata.holder;

import static info.archinnov.achilles.schemabuilder.Create.Options.ClusteringOrder;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public class EmbeddedIdPropertiesBuilder {

	private final List<Class<?>> componentClasses = new ArrayList<>();
	private final List<String> componentNames = new ArrayList<>();
	private final List<Field> componentFields = new ArrayList<>();
	private final List<Method> componentGetters = new ArrayList<>();
	private final List<Method> componentSetters = new ArrayList<>();
	private final List<String> componentsAsTimeUUID = new ArrayList<>();
	private List<ClusteringOrder> clusteringOrders;

	public void addComponentClass(Class<?> clazz) {
		componentClasses.add(clazz);
	}

	public Class<?> removeFirstComponentClass() {
		Class<?> firstComponentClass = componentClasses.get(0);
		componentClasses.remove(firstComponentClass);
		return firstComponentClass;
	}

	public void addComponentName(String name) {
		componentNames.add(name);
	}

	public String removeFirstComponentName() {
		String firstComponentName = componentNames.get(0);
		componentNames.remove(firstComponentName);
		return firstComponentName;
	}

    public void addComponentField(Field field) {
        componentFields.add(field);
    }

    public Field removeFirstComponentField() {
        Field firstComponentField = componentFields.get(0);
        componentFields.remove(firstComponentField);
        return firstComponentField  ;
    }

	public void addComponentGetter(Method getter) {
		componentGetters.add(getter);
	}

	public Method removeFirstComponentGetter() {
		Method firstComponentGetter = componentGetters.get(0);
		componentGetters.remove(firstComponentGetter);
		return firstComponentGetter;
	}

	public void addComponentSetter(Method setter) {
		componentSetters.add(setter);
	}

	public Method removeFirstComponentSetter() {
		Method firstComponentSetter = componentSetters.get(0);
		componentSetters.remove(firstComponentSetter);
		return firstComponentSetter;
	}

	public void addTimeUUIDComponent(String name) {
		this.componentsAsTimeUUID.add(name);
	}

	public void setClusteringOrders(List<ClusteringOrder> clusteringOrders) {
        this.clusteringOrders = clusteringOrders;
	}

	public PartitionComponents buildPartitionKeys() {
		return new PartitionComponents(componentClasses, componentNames,componentFields, componentGetters, componentSetters);
	}

	public ClusteringComponents buildClusteringKeys() {
		return new ClusteringComponents(componentClasses, componentNames,componentFields, componentGetters, componentSetters, clusteringOrders);
	}

	public EmbeddedIdProperties buildEmbeddedIdProperties(PartitionComponents partitionComponents, ClusteringComponents clusteringComponents) {
		return new EmbeddedIdProperties(partitionComponents, clusteringComponents, componentClasses, componentNames,
                componentFields,componentGetters, componentSetters, componentsAsTimeUUID);
	}
}
