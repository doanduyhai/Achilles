package info.archinnov.achilles.entity.metadata.builder;

import static info.archinnov.achilles.entity.PropertyHelper.isSupportedType;
import static info.archinnov.achilles.entity.manager.ThriftEntityManagerFactoryImpl.configurableCLPolicyTL;
import static info.archinnov.achilles.serializer.SerializerUtils.STRING_SRZ;
import info.archinnov.achilles.dao.CounterDao;
import info.archinnov.achilles.dao.GenericCompositeDao;
import info.archinnov.achilles.dao.GenericDynamicCompositeDao;
import info.archinnov.achilles.dao.Pair;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.type.ConsistencyLevel;
import info.archinnov.achilles.validation.Validator;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import me.prettyprint.cassandra.serializers.SerializerTypeInferer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;

/**
 * EntityMetaBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntityMetaBuilder<ID>
{

	private PropertyMeta<Void, ID> idMeta;
	private String className;
	private String columnFamilyName;
	private Long serialVersionUID;
	private Map<String, PropertyMeta<?, ?>> propertyMetas;
	private Keyspace keyspace;
	private boolean columnFamilyDirectMapping = false;
	private boolean hasCounter = false;
	private CounterDao counterDao;
	private Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels;

	public static <ID> EntityMetaBuilder<ID> entityMetaBuilder(PropertyMeta<Void, ID> idMeta)
	{
		return new EntityMetaBuilder<ID>(idMeta);
	}

	public EntityMetaBuilder(PropertyMeta<Void, ID> idMeta) {
		this.idMeta = idMeta;
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	public EntityMeta build()
	{

		Validator.validateNotNull(keyspace, "keyspace should not be null");
		Validator.validateNotNull(idMeta, "idMeta should not be null");
		Validator.validateNotNull(serialVersionUID, "serialVersionUID should not be null");
		Validator.validateNotEmpty(propertyMetas, "propertyMetas map should not be empty");
		Validator.validateRegExp(columnFamilyName, EntityMeta.COLUMN_FAMILY_PATTERN,
				"columnFamilyName");

		EntityMeta meta = new EntityMeta();

		meta.setIdMeta(idMeta);
		Serializer<?> idSerializer = SerializerTypeInferer.getSerializer(idMeta.getValueClass());
		meta.setIdSerializer(idSerializer);
		meta.setClassName(className);
		meta.setColumnFamilyName(columnFamilyName);
		meta.setSerialVersionUID(serialVersionUID);
		meta.setPropertyMetas(Collections.unmodifiableMap(propertyMetas));
		meta.setGetterMetas(Collections.unmodifiableMap(this.extractGetterMetas(propertyMetas)));
		meta.setSetterMetas(Collections.unmodifiableMap(this.extractSetterMetas(propertyMetas)));
		meta.setColumnFamilyDirectMapping(columnFamilyDirectMapping);
		meta.setHasCounter(hasCounter);
		meta.setCounterDao(counterDao);
		meta.setConsistencyLevels(consistencyLevels);

		if (columnFamilyDirectMapping)
		{
			PropertyMeta<?, ?> wideMapMeta = propertyMetas.entrySet().iterator().next().getValue();
			Serializer<?> valueSerializer = wideMapMeta.getValueSerializer();
			GenericCompositeDao<ID, ?> dao;

			if (isSupportedType(wideMapMeta.getValueClass()))
			{
				dao = new GenericCompositeDao(keyspace, idSerializer, valueSerializer,
						this.columnFamilyName, configurableCLPolicyTL.get());
			}
			else
			{
				dao = new GenericCompositeDao(keyspace, idSerializer, STRING_SRZ,
						this.columnFamilyName, configurableCLPolicyTL.get());
			}

			meta.setColumnFamilyDao(dao);
		}
		else
		{
			meta.setEntityDao(new GenericDynamicCompositeDao(keyspace, idSerializer,
					this.columnFamilyName, configurableCLPolicyTL.get()));
		}
		return meta;
	}

	private Map<Method, PropertyMeta<?, ?>> extractGetterMetas(
			Map<String, PropertyMeta<?, ?>> propertyMetas)
	{
		Map<Method, PropertyMeta<?, ?>> getterMetas = new HashMap<Method, PropertyMeta<?, ?>>();
		for (PropertyMeta<?, ?> propertyMeta : propertyMetas.values())
		{
			getterMetas.put(propertyMeta.getGetter(), propertyMeta);
		}
		return getterMetas;
	}

	private Map<Method, PropertyMeta<?, ?>> extractSetterMetas(
			Map<String, PropertyMeta<?, ?>> propertyMetas)
	{
		Map<Method, PropertyMeta<?, ?>> setterMetas = new HashMap<Method, PropertyMeta<?, ?>>();
		for (PropertyMeta<?, ?> propertyMeta : propertyMetas.values())
		{
			setterMetas.put(propertyMeta.getSetter(), propertyMeta);
		}
		return setterMetas;
	}

	public EntityMetaBuilder<ID> className(String className)
	{
		this.className = className;
		return this;
	}

	public EntityMetaBuilder<ID> columnFamilyName(String columnFamilyName)
	{
		this.columnFamilyName = columnFamilyName;
		return this;
	}

	public EntityMetaBuilder<ID> serialVersionUID(long serialVersionUID)
	{
		this.serialVersionUID = serialVersionUID;
		return this;
	}

	public EntityMetaBuilder<ID> propertyMetas(Map<String, PropertyMeta<?, ?>> propertyMetas)
	{
		this.propertyMetas = propertyMetas;
		return this;
	}

	public EntityMetaBuilder<ID> keyspace(Keyspace keyspace)
	{
		this.keyspace = keyspace;
		return this;
	}

	public EntityMetaBuilder<ID> columnFamilyDirectMapping(boolean columnFamilyDirectMapping)
	{
		this.columnFamilyDirectMapping = columnFamilyDirectMapping;
		return this;
	}

	public EntityMetaBuilder<ID> hasCounter(boolean hasCounter)
	{
		this.hasCounter = hasCounter;
		return this;
	}

	public EntityMetaBuilder<ID> counterDao(CounterDao counterDao)
	{
		this.counterDao = counterDao;
		return this;
	}

	public EntityMetaBuilder<ID> consistencyLevels(
			Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels)
	{
		this.consistencyLevels = consistencyLevels;
		return this;
	}
}
