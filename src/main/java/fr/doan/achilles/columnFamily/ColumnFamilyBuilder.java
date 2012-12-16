package fr.doan.achilles.columnFamily;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import me.prettyprint.cassandra.serializers.SerializerTypeInferer;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.factory.HFactory;

import org.apache.commons.lang.StringUtils;

import fr.doan.achilles.entity.PropertyHelper;
import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.entity.type.MultiKey;

public class ColumnFamilyBuilder
{

	private static final String DYNAMIC_TYPE_ALIASES = "(a=>AsciiType,b=>BytesType,c=>BooleanType,d=>DateType,e=>DecimalType,z=>DoubleType,f=>FloatType,i=>IntegerType,j=>Int32Type,x=>LexicalUUIDType,l=>LongType,t=>TimeUUIDType,s=>UTF8Type,u=>UUIDType)";

	private PropertyHelper propertyHelper = new PropertyHelper();

	public <ID> ColumnFamilyDefinition build(EntityMeta<ID> entityMeta, String keyspaceName)
	{

		ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(keyspaceName,
				entityMeta.getColumnFamilyName(), ComparatorType.DYNAMICCOMPOSITETYPE);

		cfDef.setKeyValidationClass(entityMeta.getIdSerializer().getComparatorType().getTypeName());
		cfDef.setComparatorTypeAlias(DYNAMIC_TYPE_ALIASES);
		cfDef.setComment("Column family for entity '" + entityMeta.getCanonicalClassName() + "'");

		return cfDef;
	}

	public <K, N, V> ColumnFamilyDefinition buildWideRow(String keyspaceName,
			String columnFamilyName, Class<K> keyClass, Class<N> nameClass, Class<V> valueClass)
	{

		Serializer<?> keySerializer = SerializerTypeInferer.getSerializer(keyClass);
		ComparatorType comparatorType;
		List<String> comparatorTypes = new ArrayList<String>();
		String comparatorTypesAlias = null;

		if (MultiKey.class.isAssignableFrom(nameClass))
		{
			comparatorType = ComparatorType.COMPOSITETYPE;

			List<Class<?>> componentClasses = new ArrayList<Class<?>>();
			List<Method> componentGetters = new ArrayList<Method>();
			List<Method> componentSetters = new ArrayList<Method>();

			propertyHelper.parseMultiKey(componentClasses, componentGetters, componentSetters,
					nameClass);

			for (Class<?> clazz : componentClasses)
			{
				Serializer<?> srz = SerializerTypeInferer.getSerializer(clazz);
				comparatorTypes.add(srz.getComparatorType().getTypeName());
			}

			comparatorTypesAlias = "ComparatorType(" + StringUtils.join(comparatorTypes, ',') + ")";
		}
		else
		{
			Serializer<?> nameSerializer = SerializerTypeInferer.getSerializer(nameClass);
			comparatorType = nameSerializer.getComparatorType();
		}

		ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(keyspaceName,
				columnFamilyName, comparatorType);

		cfDef.setKeyValidationClass(keySerializer.getComparatorType().getTypeName());
		if (comparatorTypesAlias != null)
		{
			cfDef.setComparatorTypeAlias(comparatorTypesAlias);
		}

		Serializer<?> valueSerializer = SerializerTypeInferer.getSerializer(valueClass);

		cfDef.setDefaultValidationClass(valueSerializer.getComparatorType().getTypeName());
		cfDef.setComment("Column family for entity '" + columnFamilyName + "'");

		return cfDef;
	}
}
