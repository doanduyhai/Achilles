package info.archinnov.achilles.entity.metadata;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public class EmbeddedIdPropertiesBuilder {

	private final List<Class<?>> componentClasses = new ArrayList<Class<?>>();
	private final List<String> componentNames = new ArrayList<String>();
	private final List<Method> componentGetters = new ArrayList<Method>();
	private final List<Method> componentSetters = new ArrayList<Method>();
	private final List<String> componentsAsTimeUUID = new ArrayList<String>();
	private String reversedComponentName = null;

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

	public void setReversedComponentName(String reversedComponentName) {
		this.reversedComponentName = reversedComponentName;
	}

	public PartitionComponents buildPartitionKeys() {
		return new PartitionComponents(componentClasses, componentNames, componentGetters, componentSetters);
	}

	public ClusteringComponents buildClusteringKeys() {
		return new ClusteringComponents(componentClasses, componentNames, reversedComponentName, componentGetters, componentSetters);
	}

	public EmbeddedIdProperties buildEmbeddedIdProperties(PartitionComponents partitionComponents, ClusteringComponents clusteringComponents) {
		return new EmbeddedIdProperties(partitionComponents, clusteringComponents, componentClasses, componentNames,
				componentGetters, componentSetters, componentsAsTimeUUID);
	}
}
