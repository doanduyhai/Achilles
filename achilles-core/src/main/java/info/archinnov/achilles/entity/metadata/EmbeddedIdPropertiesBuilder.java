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

	public PartitionKeys buildPartitionKeys() {
		return new PartitionKeys(componentClasses, componentNames, componentGetters, componentSetters);
	}

	public ClusteringKeys buildClusteringKeys() {
		return new ClusteringKeys(componentClasses, componentNames, componentGetters, componentSetters);
	}

	public EmbeddedIdProperties buildEmbeddedIdProperties(PartitionKeys partitionKeys, ClusteringKeys clusteringKeys) {
		return new EmbeddedIdProperties(partitionKeys, clusteringKeys, componentClasses, componentNames,
				componentGetters, componentSetters, componentsAsTimeUUID);
	}
}
