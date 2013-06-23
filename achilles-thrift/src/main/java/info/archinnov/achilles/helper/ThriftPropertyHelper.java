package info.archinnov.achilles.helper;

import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.*;
import info.archinnov.achilles.annotations.CompoundKey;
import info.archinnov.achilles.entity.metadata.CompoundKeyProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.parsing.CompoundKeyParser;
import info.archinnov.achilles.proxy.MethodInvoker;
import info.archinnov.achilles.serializer.ThriftSerializerTypeInferer;
import info.archinnov.achilles.type.WideMap;
import info.archinnov.achilles.validation.Validator;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import me.prettyprint.hector.api.Serializer;
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

    private CompoundKeyParser parser = new CompoundKeyParser();
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

        if (nameClass.getAnnotation(CompoundKey.class) != null)
        {
            CompoundKeyProperties multiKeyProperties = parser.parseCompoundKey(nameClass);

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

    public <K> void checkBounds(PropertyMeta<?, ?> propertyMeta, K start, K end,
            WideMap.OrderingMode ordering,
            boolean isCompoundKey)
    {
        log.trace("Check composites {} / {} with respect to ordering mode {}", start, end,
                ordering.name());
        if (start != null && end != null)
        {
            if (propertyMeta.isSingleKey())
            {
                @SuppressWarnings("unchecked")
                Comparable<K> startComp = (Comparable<K>) start;

                if (WideMap.OrderingMode.DESCENDING.equals(ordering))
                {
                    Validator
                            .validateTrue(startComp.compareTo(end) >= 0,
                                    "For reverse range query, start value should be greater or equal to end value");
                }
                else
                {
                    Validator.validateTrue(startComp.compareTo(end) <= 0,
                            "For range query, start value should be lesser or equal to end value");
                }
            }
            else
            {
                List<Method> componentGetters = propertyMeta
                        .getComponentGetters();
                String propertyName = propertyMeta.getPropertyName();

                List<Object> startComponentValues = invoker.extractCompoundKeyComponents(start,
                        componentGetters);
                List<Object> endComponentValues = invoker.extractCompoundKeyComponents(end,
                        componentGetters);

                if (isCompoundKey)
                {
                    Validator.validateNotNull(startComponentValues.get(0),
                            "Partition key should not be null for start clustering key : "
                                    + startComponentValues);
                    Validator.validateNotNull(endComponentValues.get(0),
                            "Partition key should not be null for end clustering key : "
                                    + endComponentValues);
                    Validator.validateTrue(
                            startComponentValues.get(0).equals(endComponentValues.get(0)),
                            "Partition key should be equals for start and end clustering keys : ["
                                    + startComponentValues + "," + endComponentValues + "]");
                }

                findLastNonNullIndexForComponents(propertyName, startComponentValues);
                findLastNonNullIndexForComponents(propertyName, endComponentValues);

                for (int i = 0; i < startComponentValues.size(); i++)
                {

                    @SuppressWarnings("unchecked")
                    Comparable<Object> startValue = (Comparable<Object>) startComponentValues
                            .get(i);
                    Object endValue = endComponentValues.get(i);

                    if (WideMap.OrderingMode.DESCENDING.equals(ordering))
                    {
                        if (startValue != null && endValue != null)
                        {
                            Validator
                                    .validateTrue(startValue.compareTo(endValue) >= 0,
                                            "For multiKey descending range query, startKey value should be greater or equal to end endKey");
                        }

                    }
                    else
                    {
                        if (startValue != null && endValue != null)
                        {
                            Validator
                                    .validateTrue(startValue.compareTo(endValue) <= 0,
                                            "For multiKey ascending range query, startKey value should be lesser or equal to end endKey");
                        }
                    }
                }
            }
        }
    }

    public int findLastNonNullIndexForComponents(String propertyName, List<Object> keyValues)
    {
        boolean nullFlag = false;
        int lastNotNullIndex = 0;
        for (Object keyValue : keyValues)
        {
            if (keyValue != null)
            {
                if (nullFlag)
                {
                    throw new IllegalArgumentException(
                            "There should not be any null value between two non-null keys of WideMap '"
                                    + propertyName + "'");
                }
                lastNotNullIndex++;
            }
            else
            {
                nullFlag = true;
            }
        }
        lastNotNullIndex--;

        log.trace("Last non null index for components of property {} : {}", propertyName,
                lastNotNullIndex);
        return lastNotNullIndex;
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
