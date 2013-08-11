package info.archinnov.achilles.entity.metadata;

import static info.archinnov.achilles.entity.metadata.PropertyType.*;
import static info.archinnov.achilles.helper.PropertyHelper.isSupportedType;
import info.archinnov.achilles.entity.metadata.transcoding.DataTranscoder;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.KeyValue;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.persistence.CascadeType;
import org.apache.cassandra.utils.Pair;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Sets;

/**
 * PropertyMeta
 * 
 * @author DuyHai DOAN
 * 
 */
public class PropertyMeta
{
    private static final Logger log = LoggerFactory.getLogger(PropertyMeta.class);

    private ObjectMapper objectMapper;
    private PropertyType type;
    private String propertyName;
    private String entityClassName;
    private Class<?> keyClass;
    private Class<?> valueClass;
    private Method getter;
    private Method setter;
    private CounterProperties counterProperties;
    private JoinProperties joinProperties;
    private EmbeddedIdProperties embeddedIdProperties;
    private Class<?> idClass;
    private Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels;

    private DataTranscoder transcoder;

    private static final Logger logger = LoggerFactory.getLogger(PropertyMeta.class);

    public Object getValueFromString(Object stringValue)
    {
        log.trace("Getting value from string {} for property {} of entity class {}", stringValue,
                propertyName, entityClassName);
        if (stringValue != null)
        {
            try
            {
                if (valueClass == String.class)
                {
                    log.trace("Casting value straight to string");
                    return valueClass.cast(stringValue);
                }
                else if (stringValue instanceof String)
                {
                    log.trace("Deserializing value from string");
                    return this.objectMapper.readValue((String) stringValue, this.valueClass);
                }
                else
                {
                    throw new AchillesException("Error while trying to deserialize the JSON '" + stringValue);
                }
            } catch (Exception e)
            {
                throw new AchillesException("Error while trying to deserialize the JSON '" + stringValue);
            }
        }
        else {
            return null;
        }
    }

    public KeyValue<Object, Object> getKeyValueFromString(String stringKeyValue)
    {
        log.trace("Getting key/value from string {} for property {} of entity class {}",
                stringKeyValue, propertyName, entityClassName);
        try
        {
            return this.objectMapper.readValue(stringKeyValue, new TypeReference<KeyValue<Object, Object>>()
            {
            });
        } catch (Exception e)
        {
            logger.error("Error while trying to deserialize the JSON : " + stringKeyValue,
                    e);
            return null;
        }
    }

    public String writeValueToString(Object object)
    {
        log.trace("Writing value {} to string for property {} of entity class {}", object,
                propertyName, entityClassName);
        try
        {
            if (valueClass == String.class && type != MAP && type != LAZY_MAP)
            {
                log.trace("Casting value straight to string");
                return (String) object;
            }
            else
            {
                log.trace("Serializing value to string");
                return this.objectMapper.writeValueAsString(object);
            }
        } catch (Exception e)
        {
            logger.error("Error while trying to serialize to JSON the object : " + object, e);
            return null;
        }
    }

    public Object writeValueAsSupportedTypeOrString(Object value)
    {
        log.trace("Writing value {} as native type or string for property {} of entity class {}",
                value, propertyName, entityClassName);
        try
        {
            if (isSupportedType(valueClass))
            {
                log.trace("Value belongs to list of supported native types");
                return value;
            }
            else
            {
                log.trace("Serializing value to string");
                return this.objectMapper.writeValueAsString(value);
            }
        } catch (Exception e)
        {
            logger.error("Error while trying to serialize to JSON the object : " + value, e);
            return null;
        }
    }

    public Object castValue(Object object)
    {
        try
        {
            if (isSupportedType(valueClass) || type.isJoin())
                return this.valueClass.cast(object);
            else
                return objectMapper.readValue((String) object, valueClass);
        } catch (Exception e)
        {
            throw new AchillesException("Error while trying to cast the object " + object
                    + " to type '" + this.valueClass.getCanonicalName() + "'", e);
        }
    }

    public List<Method> getComponentGetters()
    {
        List<Method> compGetters = new ArrayList<Method>();
        if (embeddedIdProperties != null)
        {
            compGetters = embeddedIdProperties.getComponentGetters();
        }
        return compGetters;
    }

    public Method getPartitionKeyGetter()
    {
        Method getter = null;
        if (embeddedIdProperties != null)
        {
            getter = embeddedIdProperties.getComponentGetters().get(0);
        }
        return getter;
    }

    public Method getPartitionKeySetter()
    {
        Method getter = null;
        if (embeddedIdProperties != null)
        {
            getter = embeddedIdProperties.getComponentSetters().get(0);
        }
        return getter;
    }

    public List<Method> getComponentSetters()
    {
        List<Method> compSetters = new ArrayList<Method>();
        if (embeddedIdProperties != null)
        {
            compSetters = embeddedIdProperties.getComponentSetters();
        }
        return compSetters;
    }

    public List<Class<?>> getComponentClasses()
    {
        List<Class<?>> compClasses = new ArrayList<Class<?>>();
        if (embeddedIdProperties != null)
        {
            compClasses = embeddedIdProperties.getComponentClasses();
        }
        return compClasses;
    }

    public String getOrderingComponent()
    {
        String component = null;
        if (embeddedIdProperties != null)
        {
            return embeddedIdProperties.getOrderingComponent();
        }
        return component;
    }

    public List<String> getComponentNames()
    {
        List<String> components = new ArrayList<String>();
        if (embeddedIdProperties != null)
        {
            return embeddedIdProperties.getComponentNames();
        }
        return components;
    }

    public <T> Constructor<T> getEmbeddedIdConstructor()
    {
        return embeddedIdProperties != null ? embeddedIdProperties.<T> getConstructor() : null;
    }

    public boolean hasDefaultConstructorForEmbeddedId()
    {
        return embeddedIdProperties != null ? embeddedIdProperties
                .getConstructor()
                .getParameterTypes().length == 0 : false;
    }

    public boolean isJoin()
    {
        return type.isJoin();
    }

    public EntityMeta joinMeta()
    {
        return joinProperties != null ? joinProperties.getEntityMeta() : null;
    }

    public PropertyMeta joinIdMeta()
    {
        return joinMeta() != null ? joinMeta().getIdMeta() : null;
    }

    public PropertyMeta counterIdMeta()
    {
        return counterProperties != null ? counterProperties.getIdMeta() : null;
    }

    public String fqcn()
    {
        return counterProperties != null ? counterProperties.getFqcn() : null;
    }

    public boolean isLazy()
    {
        return this.type.isLazy();
    }

    public boolean isCounter()
    {
        return this.type.isCounter();
    }

    public boolean isEmbeddedId()
    {
        return type.isEmbeddedId();
    }

    public boolean hasCascadeType(CascadeType type)
    {
        if (joinProperties != null && joinProperties.getCascadeTypes().contains(type))
        {
            return true;
        }
        else
        {
            return false;
        }
    }

    public boolean hasAnyCascadeType(CascadeType... types)
    {
        return joinProperties != null
                && !Sets.intersection(joinProperties.getCascadeTypes(), Sets.newHashSet(types))
                        .isEmpty();
    }

    public boolean isJoinCollection()
    {
        return type == JOIN_LIST || type == JOIN_SET;
    }

    public boolean isJoinMap()
    {
        return type == JOIN_MAP;
    }

    public ConsistencyLevel getReadConsistencyLevel()
    {
        return consistencyLevels != null ? consistencyLevels.left : null;
    }

    public ConsistencyLevel getWriteConsistencyLevel()
    {
        return consistencyLevels != null ? consistencyLevels.right : null;
    }

    public Object decode(Object cassandraValue) {
        return cassandraValue == null ? null : transcoder.decode(this, cassandraValue);
    }

    public Object decodeKey(Object cassandraValue) {
        return cassandraValue == null ? null : transcoder.decodeKey(this, cassandraValue);
    }

    public List<Object> decode(List<?> cassandraValue) {
        return cassandraValue == null ? null : transcoder.decode(this, cassandraValue);
    }

    public Set<Object> decode(Set<?> cassandraValue) {
        return cassandraValue == null ? null : transcoder.decode(this, cassandraValue);
    }

    public Map<Object, Object> decode(Map<?, ?> cassandraValue) {
        return cassandraValue == null ? null : transcoder.decode(this, cassandraValue);
    }

    public Object decodeFromComponents(List<?> components) {
        return components == null ? null : transcoder.decodeFromComponents(this, components);
    }

    public Object encode(Object entityValue) {
        return entityValue == null ? null : transcoder.encode(this, entityValue);
    }

    public Object encodeKey(Object entityValue) {
        return entityValue == null ? null : transcoder.encodeKey(this, entityValue);
    }

    public List<Object> encode(List<?> entityValue) {
        return entityValue == null ? null : transcoder.encode(this, entityValue);
    }

    public Set<Object> encode(Set<?> entityValue) {
        return entityValue == null ? null : transcoder.encode(this, entityValue);
    }

    public Map<Object, Object> encode(Map<?, ?> entityValue) {
        return entityValue == null ? null : transcoder.encode(this, entityValue);
    }

    public List<Object> encodeToComponents(Object compoundKey) {
        return compoundKey == null ? null : transcoder.encodeToComponents(this, compoundKey);
    }

    public List<Object> encodeComponents(List<?> components) {
        return components == null ? null : transcoder.encodeComponents(this, components);
    }

    public String forceEncodeToJSON(Object object)
    {
        return transcoder.forceEncodeToJSON(object);
    }

    public Object forceDecodeFromJSON(String cassandraValue, Class<?> targetType)
    {
        return transcoder.forceDecodeFromJSON(cassandraValue, targetType);
    }

    public PropertyType type()
    {
        return type;
    }

    public void setType(PropertyType propertyType)
    {
        this.type = propertyType;
    }

    public String getPropertyName()
    {
        return propertyName;
    }

    public void setPropertyName(String propertyName)
    {
        this.propertyName = propertyName;
    }

    public Class<?> getKeyClass()
    {
        return keyClass;
    }

    public void setKeyClass(Class<?> keyClass)
    {
        this.keyClass = keyClass;
    }

    public Class<?> getValueClass()
    {
        return valueClass;
    }

    public void setValueClass(Class<?> valueClass)
    {
        this.valueClass = valueClass;
    }

    public Method getGetter()
    {
        return getter;
    }

    public void setGetter(Method getter)
    {
        this.getter = getter;
    }

    public Method getSetter()
    {
        return setter;
    }

    public void setSetter(Method setter)
    {
        this.setter = setter;
    }

    public EmbeddedIdProperties getEmbeddedIdProperties()
    {
        return embeddedIdProperties;
    }

    public void setEmbeddedIdProperties(EmbeddedIdProperties embeddedIdProperties)
    {
        this.embeddedIdProperties = embeddedIdProperties;
    }

    public Class<?> getIdClass()
    {
        return idClass;
    }

    public void setIdClass(Class<?> idClass)
    {
        this.idClass = idClass;
    }

    public Object getKey(Object object)
    {
        return keyClass.cast(object);
    }

    public JoinProperties getJoinProperties()
    {
        return joinProperties;
    }

    public void setJoinProperties(JoinProperties joinProperties)
    {
        this.joinProperties = joinProperties;
    }

    public void setObjectMapper(ObjectMapper objectMapper)
    {
        this.objectMapper = objectMapper;
    }

    public CounterProperties getCounterProperties()
    {
        return counterProperties;
    }

    public void setCounterProperties(CounterProperties counterProperties)
    {
        this.counterProperties = counterProperties;
    }

    public void setConsistencyLevels(Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels)
    {
        this.consistencyLevels = consistencyLevels;
    }

    public String getEntityClassName()
    {
        return entityClassName;
    }

    public void setEntityClassName(String entityClassName)
    {
        this.entityClassName = entityClassName;
    }

    public DataTranscoder getTranscoder() {
        return transcoder;
    }

    public void setTranscoder(DataTranscoder transcoder) {
        this.transcoder = transcoder;
    }

    @Override
    public String toString()
    {
        StringBuilder description = new StringBuilder();
        description.append("PropertyMeta [type=").append(type).append(", ");
        description.append("propertyName=").append(propertyName).append(", ");
        description.append("entityClassName=").append(entityClassName).append(", ");
        if (keyClass != null)
            description.append("keyClass=").append(keyClass.getCanonicalName()).append(", ");

        description.append("valueClass=").append(valueClass.getCanonicalName()).append(", ");

        if (counterProperties != null)
            description.append("counterProperties=").append(counterProperties).append(", ");

        if (joinProperties != null)
            description.append("joinProperties=").append(joinProperties).append(", ");

        if (embeddedIdProperties != null)
            description.append("multiKeyProperties=").append(embeddedIdProperties).append(", ");

        if (consistencyLevels != null)
        {
            description
                    .append("consistencyLevels=[")
                    .append(consistencyLevels.left.name())
                    .append(",");
            description.append(consistencyLevels.right.name()).append("], ");
        }

        return description.toString();
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((entityClassName == null) ? 0 : entityClassName.hashCode());
        result = prime * result + ((propertyName == null) ? 0 : propertyName.hashCode());
        result = prime * result + ((type == null) ? 0 : type.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        PropertyMeta other = (PropertyMeta) obj;
        if (entityClassName == null)
        {
            if (other.entityClassName != null)
                return false;
        }
        else if (!entityClassName.equals(other.entityClassName))
            return false;
        if (propertyName == null)
        {
            if (other.propertyName != null)
                return false;
        }
        else if (!propertyName.equals(other.propertyName))
            return false;
        if (type != other.type)
            return false;
        return true;
    }
}
