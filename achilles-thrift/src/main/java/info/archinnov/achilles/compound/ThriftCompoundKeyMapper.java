package info.archinnov.achilles.compound;

import static info.archinnov.achilles.logger.ThriftLoggerHelper.format;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.EQUAL;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.validation.Validator;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.AbstractComposite.Component;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.Composite;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.FluentIterable;

public class ThriftCompoundKeyMapper
{

	private static final Logger log = LoggerFactory.getLogger(ThriftCompoundKeyMapper.class);
	private static final ClassToSerializerTransformer classToSerializer = new ClassToSerializerTransformer();

	private ReflectionInvoker invoker = new ReflectionInvoker();

	public <K, V> K fromCompositeToCompound(PropertyMeta<K, V> pm, List<Component<?>> components)
	{
		if (log.isTraceEnabled())
		{
			log.trace("Build compound key {} from composite components {}", pm.getPropertyName(),
					format(components));
		}

		K compoundKey;
		Constructor<K> constructor = pm.getCompoundKeyConstructor();
		List<Class<?>> componentClasses = pm.getComponentClasses();
		List<Method> componentSetters = pm.getComponentSetters();

		List<Serializer<Object>> serializers = FluentIterable
				.from(componentClasses)
				.transform(classToSerializer)
				.toImmutableList();

		int componentCount = components.size();

		List<Object> componentValues = new ArrayList<Object>();
		for (int i = 0; i < componentCount; i++)
		{
			Component<?> comp = components.get(i);
			componentValues.add(serializers.get(i).fromByteBuffer(comp.getBytes()));
		}
		if (pm.hasDefaultConstructorForCompoundKey())
		{
			compoundKey = injectValuesBySetter(componentValues, constructor, componentSetters);
		}
		else
		{
			compoundKey = injectValuesByConstructor(componentValues, constructor);
		}

		log.trace("Built compound key : {}", compoundKey);

		return compoundKey;
	}

	public <V> V fromCompositeToEmbeddedId(PropertyMeta<?, V> pm, List<Component<?>> components,
			Object partitionKey)
	{
		if (log.isTraceEnabled())
		{
			log.trace("Build compound key {} from composite components {}", pm.getPropertyName(),
					format(components));
		}

		V compoundKey;
		Constructor<V> constructor = pm.getCompoundKeyConstructor();
		List<Class<?>> componentClasses = pm.getComponentClasses().subList(1,
				pm.getComponentClasses().size());
		List<Method> componentSetters = pm.getComponentSetters();

		List<Serializer<Object>> serializers = FluentIterable
				.from(componentClasses)
				.transform(classToSerializer)
				.toImmutableList();

		int componentCount = components.size();

		List<Object> componentValues = new ArrayList<Object>();
		componentValues.add(partitionKey);
		for (int i = 0; i < componentCount; i++)
		{
			Component<?> comp = components.get(i);
			componentValues.add(serializers.get(i).fromByteBuffer(comp.getBytes()));
		}

		try
		{
			if (pm.hasDefaultConstructorForCompoundKey())
			{
				compoundKey = injectValuesBySetter(componentValues, constructor, componentSetters);
			}
			else
			{
				compoundKey = injectValuesByConstructor(componentValues, constructor);
			}
		}
		catch (Exception e)
		{
			throw new AchillesException(e);
		}

		log.trace("Built compound key : {}", compoundKey);

		return compoundKey;
	}

	public Composite fromCompoundToCompositeForInsertOrGet(Object compoundKey, PropertyMeta<?, ?> pm)
	{
		String propertyName = pm.getPropertyName();
		log.trace("Build composite from key {} to persist @CompoundKey {} ", compoundKey,
				propertyName);

		List<Object> components = fromCompoundToComponents(compoundKey, pm.getComponentGetters());
		return fromComponentsToCompositeForInsertOrGet(components, pm);

	}

	public Composite fromComponentsToCompositeForInsertOrGet(List<Object> components,
			PropertyMeta<?, ?> pm)
	{
		Composite composite = new Composite();
		String propertyName = pm.getPropertyName();

		List<Object> columnComponents;
		List<Class<?>> columnClasses;
		if (pm.isEmbeddedId())
		{
			columnComponents = components.subList(1, components.size());
			columnClasses = pm.getComponentClasses().subList(1, pm.getComponentClasses().size());
		}
		else
		{
			columnComponents = components;
			columnClasses = pm.getComponentClasses();
		}

		log.trace("Build composite from components {} to persist @CompoundKey {} ",
				columnComponents, propertyName);

		List<Serializer<Object>> serializers = FluentIterable
				.from(columnClasses)
				.transform(classToSerializer)
				.toImmutableList();
		int srzCount = serializers.size();

		for (Object value : columnComponents)
		{
			Validator.validateNotNull(value, "The values for the @CompoundKey '" + propertyName
					+ "' should not be null");
		}

		for (int i = 0; i < srzCount; i++)
		{
			Serializer<Object> srz = serializers.get(i);
			composite.setComponent(i, columnComponents.get(i), srz, srz
					.getComparatorType()
					.getTypeName());
		}
		return composite;
	}

	public Composite fromCompoundToCompositeForQuery(Object compoundKey, PropertyMeta<?, ?> pm,
			ComponentEquality equality)
	{
		String propertyName = pm.getPropertyName();
		log.trace("Build composite from key {} to query @CompoundKey {} ", compoundKey,
				propertyName);

		List<Object> components = fromCompoundToComponents(compoundKey, pm.getComponentGetters());
		return fromComponentsToCompositeForQuery(components, pm, equality);
	}

	public Composite fromComponentsToCompositeForQuery(List<Object> components,
			PropertyMeta<?, ?> pm,
			ComponentEquality equality)
	{
		String propertyName = pm.getPropertyName();

		List<Object> columnComponents;
		List<Class<?>> columnClasses;

		if (pm.isEmbeddedId())
		{
			columnComponents = components.subList(1, components.size());
			columnClasses = pm.getComponentClasses().subList(1, pm.getComponentClasses().size());
		}
		else
		{
			columnComponents = components;
			columnClasses = pm.getComponentClasses();
		}

		log.trace("Build composite from components {} to query @CompoundKey {} ", columnComponents,
				propertyName);

		Composite composite = new Composite();

		List<Serializer<Object>> serializers = FluentIterable
				.from(columnClasses)
				.transform(classToSerializer)
				.toImmutableList();

		int srzCount = serializers.size();

		Validator.validateTrue(srzCount >= columnComponents.size(), "There should be at most "
				+ srzCount
				+ " values for the @CompoundKey '" + propertyName + "'");

		int lastNotNullIndex = validateNoHoleAndReturnLastNonNullIndex(columnComponents);

		for (int i = 0; i <= lastNotNullIndex; i++)
		{
			Serializer<Object> srz = serializers.get(i);
			Object value = columnComponents.get(i);
			if (i < lastNotNullIndex)
			{
				composite.setComponent(i, value, srz, srz.getComparatorType().getTypeName(), EQUAL);
			}
			else
			{
				composite.setComponent(i, value, srz, srz.getComparatorType().getTypeName(),
						equality);
			}
		}
		return composite;
	}

	public List<Object> fromCompoundToComponents(Object compoundKey, List<Method> componentGetters)
	{
		log.trace("Determine components for compound key {} ", compoundKey);

		List<Object> compoundComponents = new ArrayList<Object>();

		if (compoundKey != null)
		{
			for (Method getter : componentGetters)
			{
				Object component = invoker.getValueFromField(compoundKey, getter);
				compoundComponents.add(component);
			}
		}
		log.trace("Found compound key : {}", compoundComponents);
		return compoundComponents;
	}

	public int validateNoHoleAndReturnLastNonNullIndex(List<Object> components)
	{
		boolean nullFlag = false;
		int lastNotNullIndex = 0;
		for (Object keyValue : components)
		{
			if (keyValue != null)
			{
				if (nullFlag)
				{
					throw new IllegalArgumentException(
							"There should not be any null value between two non-null components of a @CompoundKey");
				}
				lastNotNullIndex++;
			}
			else
			{
				nullFlag = true;
			}
		}
		lastNotNullIndex--;

		log.trace("Last non null index for compound components : {}", lastNotNullIndex);
		return lastNotNullIndex;
	}

	private <K> K injectValuesBySetter(List<Object> components, Constructor<K> constructor,
			List<Method> componentSetters)
	{

		K compoundKey = invoker.instanciate(constructor);
		log.trace("Built and inject value into compound key : {} by setters", compoundKey);

		for (int i = 0; i < components.size(); i++)
		{
			Object compValue = components.get(i);
			invoker.setValueToField(compoundKey, componentSetters.get(i), compValue);
		}
		return compoundKey;
	}

	private <K> K injectValuesByConstructor(List<Object> components, Constructor<K> constructor)
	{
		K compoundKey;
		int componentCount = components.size();
		Object[] constructorParams = new Object[componentCount];
		for (int i = 0; i < componentCount; i++)
		{
			constructorParams[i] = components.get(i);
		}
		compoundKey = invoker.instanciate(constructor, constructorParams);
		log.trace("Built and inject value into compound key : {} by constructor", compoundKey);
		return compoundKey;
	}
}
