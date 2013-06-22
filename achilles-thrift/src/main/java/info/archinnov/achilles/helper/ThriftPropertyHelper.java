package info.archinnov.achilles.helper;

import static info.archinnov.achilles.helper.ThriftLoggerHelper.*;
import static info.archinnov.achilles.serializer.ThriftSerializerUtils.*;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.*;
import info.archinnov.achilles.annotations.MultiKey;
import info.archinnov.achilles.entity.metadata.CompoundKeyProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.proxy.MethodInvoker;
import info.archinnov.achilles.serializer.ThriftSerializerTypeInferer;
import info.archinnov.achilles.type.WideMap;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.AbstractComposite.Component;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ThriftPropertyHelper
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftPropertyHelper extends PropertyHelper
{
    private static final Logger log = LoggerFactory.getLogger(ThriftPropertyHelper.class);

    private MethodInvoker invoker = new MethodInvoker();

    public <ID> String determineCompatatorTypeAliasForCompositeCF(PropertyMeta<?, ?> propertyMeta,
            boolean forCreation)
    {

        log
                .debug("Determine the Comparator type alias for composite-based column family using propertyMeta of field {} ",
                        propertyMeta.getPropertyName());

        Class<?> nameClass = propertyMeta.getKeyClass();
        List<String> comparatorTypes = new ArrayList<String>();
        String comparatorTypesAlias;

        if (nameClass.getAnnotation(MultiKey.class) != null)
        {
            CompoundKeyProperties multiKeyProperties = this.parseCompoundKey(nameClass);

            for (Class<?> clazz : multiKeyProperties.getComponentClasses())
            {
                Serializer<?> srz = ThriftSerializerTypeInferer.getSerializer(clazz);
                if (forCreation)
                {
                    comparatorTypes.add(srz.getComparatorType().getTypeName());
                }
                else
                {
                    comparatorTypes.add("org.apache.cassandra.db.marshal."
                            + srz.getComparatorType().getTypeName());
                }
            }
            if (forCreation)
            {
                comparatorTypesAlias = "(" + StringUtils.join(comparatorTypes, ',') + ")";
            }
            else
            {
                comparatorTypesAlias = "CompositeType(" + StringUtils.join(comparatorTypes, ',')
                        + ")";
            }
        }
        else
        {
            String typeAlias = ThriftSerializerTypeInferer
                    .getSerializer(nameClass)
                    .getComparatorType()
                    .getTypeName();
            if (forCreation)
            {
                comparatorTypesAlias = "(" + typeAlias + ")";
            }
            else
            {
                comparatorTypesAlias = "CompositeType(org.apache.cassandra.db.marshal." + typeAlias
                        + ")";
            }
        }

        log.trace("Comparator type alias : {}", comparatorTypesAlias);

        return comparatorTypesAlias;
    }

    public <K, V> K buildComponentsFromComposite(PropertyMeta<K, V> propertyMeta,
            List<Component<?>> components)
    {
        if (log.isTraceEnabled())
        {
            log.trace("Build multi-key instance of field {} from composite components {}",
                    propertyMeta.getPropertyName(), format(components));
        }

        K key;
        Class<K> compoundKeyClass = propertyMeta.getKeyClass();
        List<Method> componentSetters = propertyMeta.getComponentSetters();
        List<Class<?>> componentClasses = propertyMeta.getComponentClasses();
        List<Serializer<?>> serializers = new ArrayList<Serializer<?>>();
        Set<Integer> enumTypeIndex = new HashSet<Integer>();
        int j = 0;
        for (Class<?> clazz : componentClasses)
        {
            if (clazz.isEnum()) {
                serializers.add(STRING_SRZ);
                enumTypeIndex.add(j);
            }
            else {
                serializers.add(ThriftSerializerTypeInferer.getSerializer(clazz));
            }
            j++;
        }

        try
        {
            key = compoundKeyClass.newInstance();

            for (int i = 0; i < components.size(); i++)
            {
                Component<?> comp = components.get(i);
                Object compValue;
                if (enumTypeIndex.contains(i)) {
                    String enumName = STRING_SRZ.fromByteBuffer(comp.getBytes());
                    compValue = Enum.valueOf((Class<Enum>) componentClasses.get(i), enumName);
                }
                else {
                    compValue = serializers.get(i).fromByteBuffer(comp.getBytes());
                }
                invoker.setValueToField(key, componentSetters.get(i), compValue);
            }

        } catch (Exception e)
        {
            throw new AchillesException(e);
        }

        log.trace("Built compound key : {}", key);

        return key;
    }

    public ComponentEquality[] determineEquality(WideMap.BoundingMode bounds,
            WideMap.OrderingMode ordering)
    {
        log
                .trace("Determine component equality with respect to bounding mode {} and ordering mode {}",
                        bounds.name(), ordering.name());
        ComponentEquality[] result = new ComponentEquality[2];
        if (WideMap.OrderingMode.DESCENDING.equals(ordering))
        {
            if (WideMap.BoundingMode.INCLUSIVE_BOUNDS.equals(bounds))
            {
                result[0] = GREATER_THAN_EQUAL;
                result[1] = EQUAL;
            }
            else if (WideMap.BoundingMode.EXCLUSIVE_BOUNDS.equals(bounds))
            {
                result[0] = LESS_THAN_EQUAL;
                result[1] = GREATER_THAN_EQUAL;
            }
            else if (WideMap.BoundingMode.INCLUSIVE_START_BOUND_ONLY.equals(bounds))
            {
                result[0] = GREATER_THAN_EQUAL;
                result[1] = GREATER_THAN_EQUAL;
            }
            else if (WideMap.BoundingMode.INCLUSIVE_END_BOUND_ONLY.equals(bounds))
            {
                result[0] = LESS_THAN_EQUAL;
                result[1] = EQUAL;
            }
        }
        else
        {
            if (WideMap.BoundingMode.INCLUSIVE_BOUNDS.equals(bounds))
            {
                result[0] = EQUAL;
                result[1] = GREATER_THAN_EQUAL;
            }
            else if (WideMap.BoundingMode.EXCLUSIVE_BOUNDS.equals(bounds))
            {
                result[0] = GREATER_THAN_EQUAL;
                result[1] = LESS_THAN_EQUAL;
            }
            else if (WideMap.BoundingMode.INCLUSIVE_START_BOUND_ONLY.equals(bounds))
            {
                result[0] = EQUAL;
                result[1] = LESS_THAN_EQUAL;
            }
            else if (WideMap.BoundingMode.INCLUSIVE_END_BOUND_ONLY.equals(bounds))
            {
                result[0] = GREATER_THAN_EQUAL;
                result[1] = GREATER_THAN_EQUAL;
            }
        }

        log
                .trace("For the to bounding mode {} and ordering mode {}, the component equalities should be : {} - {}",
                        bounds.name(), ordering.name(), result[0].name(), result[1].name());
        return result;
    }
}
