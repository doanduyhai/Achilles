package info.archinnov.achilles.test.builders;

import info.archinnov.achilles.annotations.CompoundKey;
import info.archinnov.achilles.entity.metadata.CompoundKeyProperties;
import info.archinnov.achilles.entity.metadata.CounterProperties;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.JoinProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.helper.EntityIntrospector;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Counter;
import info.archinnov.achilles.type.Pair;
import info.archinnov.achilles.type.WideMap;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.persistence.CascadeType;

import org.codehaus.jackson.map.ObjectMapper;

/**
 * PropertyMetaBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class PropertyMetaTestBuilder<T, K, V>
{
	private EntityIntrospector achillesEntityIntrospector = new EntityIntrospector();

	private Class<T> clazz;
	private String field;
	private String entityClassName;
	private PropertyType type;
	private Class<K> keyClass;
	private Class<V> valueClass;
	private EntityMeta joinMeta;
	private Set<CascadeType> cascadeTypes = new HashSet<CascadeType>();

	private List<Class<?>> componentClasses;
	private List<String> componentNames;
	private List<Method> componentGetters;
	private List<Method> componentSetters;

	private String externalTableName;

	private boolean buildAccessors;
	private Class<?> idClass;
	private ObjectMapper objectMapper;
	private PropertyMeta<Void, ?> counterIdMeta;
	private String fqcn;
	private Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels;

	public static <T, K, V> PropertyMetaTestBuilder<T, K, V> of(Class<T> clazz, Class<K> keyClass,
			Class<V> valueClass)
	{
		return new PropertyMetaTestBuilder<T, K, V>(clazz, keyClass, valueClass);
	}

	public static <K, V> PropertyMetaTestBuilder<CompleteBean, K, V> completeBean(
			Class<K> keyClass, Class<V> valueClass)
	{
		return new PropertyMetaTestBuilder<CompleteBean, K, V>(CompleteBean.class, keyClass,
				valueClass);
	}

	public static <K, V> PropertyMetaTestBuilder<Void, K, V> noClass(Class<K> keyClass,
			Class<V> valueClass)
	{
		return new PropertyMetaTestBuilder<Void, K, V>(Void.class, keyClass, valueClass);
	}

	public static <V> PropertyMetaTestBuilder<Void, Void, V> valueClass(Class<V> valueClass)
	{
		return new PropertyMetaTestBuilder<Void, Void, V>(Void.class, Void.class, valueClass);
	}

	public PropertyMetaTestBuilder(Class<T> clazz, Class<K> keyClass, Class<V> valueClass) {
		this.clazz = clazz;
		this.keyClass = keyClass;
		this.valueClass = valueClass;
	}

	public PropertyMeta<K, V> build() throws Exception
	{
		PropertyMeta<K, V> pm = new PropertyMeta<K, V>();
		pm.setType(type);
		pm.setEntityClassName(entityClassName);
		pm.setPropertyName(field);
		pm.setKeyClass(keyClass);
		pm.setValueClass(valueClass);
		pm.setIdClass(idClass);
		if (buildAccessors)
		{
			Field declaredField = clazz.getDeclaredField(field);
			pm.setGetter(achillesEntityIntrospector.findGetter(clazz, declaredField));
			Class<?> fieldClass = declaredField.getType();
			if (!WideMap.class.isAssignableFrom(fieldClass)
					&& !Counter.class.isAssignableFrom(fieldClass))
			{
				pm.setSetter(achillesEntityIntrospector.findSetter(clazz, declaredField));
			}
		}

		if (joinMeta != null || !cascadeTypes.isEmpty())
		{
			JoinProperties joinProperties = new JoinProperties();
			joinProperties.setCascadeTypes(cascadeTypes);
			joinProperties.setEntityMeta(joinMeta);
			pm.setJoinProperties(joinProperties);
		}

		if (componentClasses != null || componentNames != null || componentGetters != null
				|| componentSetters != null)
		{
			CompoundKeyProperties compoundKeyProps = new CompoundKeyProperties();
			compoundKeyProps.setComponentClasses(componentClasses);
			compoundKeyProps.setComponentNames(componentNames);
			compoundKeyProps.setComponentGetters(componentGetters);
			compoundKeyProps.setComponentSetters(componentSetters);

			pm.setCompoundKeyProperties(compoundKeyProps);
		}

		if (pm.getCompoundKeyProperties() != null
				|| keyClass.getAnnotation(CompoundKey.class) != null
				|| (type != null && type.isEmbeddedId()))
		{
			pm.setSingleKey(false);
		}
		else
		{
			pm.setSingleKey(true);
		}

		if (externalTableName != null)
		{
			pm.setExternalTableName(externalTableName);
		}

		if (counterIdMeta != null || fqcn != null)
		{
			CounterProperties counterProperties = new CounterProperties(fqcn, counterIdMeta);
			pm.setCounterProperties(counterProperties);

		}
		objectMapper = objectMapper != null ? objectMapper : new ObjectMapper();
		pm.setObjectMapper(objectMapper);
		if (consistencyLevels == null)
		{
			consistencyLevels = new Pair<ConsistencyLevel, ConsistencyLevel>(ConsistencyLevel.ONE,
					ConsistencyLevel.ONE);
		}
		pm.setConsistencyLevels(consistencyLevels);
		return pm;
	}

	public PropertyMetaTestBuilder<T, K, V> field(String field)
	{
		this.field = field;
		return this;
	}

	public PropertyMetaTestBuilder<T, K, V> entityClassName(String entityClassName)
	{
		this.entityClassName = entityClassName;
		return this;
	}

	public PropertyMetaTestBuilder<T, K, V> type(PropertyType type)
	{
		this.type = type;
		return this;
	}

	public PropertyMetaTestBuilder<T, K, V> joinMeta(EntityMeta joinMeta)
	{
		this.joinMeta = joinMeta;
		return this;
	}

	public PropertyMetaTestBuilder<T, K, V> cascadeType(CascadeType cascadeType)
	{
		this.cascadeTypes.add(cascadeType);
		return this;
	}

	public PropertyMetaTestBuilder<T, K, V> cascadeTypes(CascadeType... cascadeTypes)
	{
		this.cascadeTypes.addAll(Arrays.asList(cascadeTypes));
		return this;
	}

	public PropertyMetaTestBuilder<T, K, V> externalTable(String externalTableName)
	{
		this.externalTableName = externalTableName;
		return this;
	}

	public PropertyMetaTestBuilder<T, K, V> compClasses(List<Class<?>> componentClasses)
	{
		this.componentClasses = componentClasses;
		return this;
	}

	public PropertyMetaTestBuilder<T, K, V> compClasses(Class<?>... componentClasses)
	{
		this.componentClasses = Arrays.asList(componentClasses);
		return this;
	}

	public PropertyMetaTestBuilder<T, K, V> compNames(List<String> componentNames)
	{
		this.componentNames = componentNames;
		return this;
	}

	public PropertyMetaTestBuilder<T, K, V> compNames(String... componentNames)
	{
		this.componentNames = Arrays.asList(componentNames);
		return this;
	}

	public PropertyMetaTestBuilder<T, K, V> compGetters(List<Method> componentGetters)
	{
		this.componentGetters = componentGetters;
		return this;
	}

	public PropertyMetaTestBuilder<T, K, V> compGetters(Method... componentGetters)
	{
		this.componentGetters = Arrays.asList(componentGetters);
		return this;
	}

	public PropertyMetaTestBuilder<T, K, V> compSetters(List<Method> componentSetters)
	{
		this.componentSetters = componentSetters;
		return this;
	}

	public PropertyMetaTestBuilder<T, K, V> compSetters(Method... componentSetters)
	{
		this.componentSetters = Arrays.asList(componentSetters);
		return this;
	}

	public PropertyMetaTestBuilder<T, K, V> accessors()
	{
		this.buildAccessors = true;
		return this;
	}

	public PropertyMetaTestBuilder<T, K, V> mapper(ObjectMapper mapper)
	{
		this.objectMapper = mapper;
		return this;
	}

	public PropertyMetaTestBuilder<T, K, V> counterIdMeta(PropertyMeta<Void, ?> counterIdMeta)
	{
		this.counterIdMeta = counterIdMeta;
		return this;
	}

	public PropertyMetaTestBuilder<T, K, V> fqcn(String fqcn)
	{
		this.fqcn = fqcn;
		return this;
	}

	public PropertyMetaTestBuilder<T, K, V> idClass(Class<?> idClass)
	{
		this.idClass = idClass;
		return this;
	}

	public PropertyMetaTestBuilder<T, K, V> consistencyLevels(
			Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels)
	{
		this.consistencyLevels = consistencyLevels;
		return this;
	}
}
