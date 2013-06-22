package info.archinnov.achilles.composite;

import static info.archinnov.achilles.serializer.ThriftSerializerUtils.*;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.*;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.helper.ThriftPropertyHelper;
import info.archinnov.achilles.proxy.MethodInvoker;
import info.archinnov.achilles.serializer.ThriftSerializerTypeInferer;
import info.archinnov.achilles.type.WideMap;
import info.archinnov.achilles.validation.Validator;
import java.util.ArrayList;
import java.util.List;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.Composite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ThriftCompositeFactory
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftCompositeFactory
{
    private static final Logger log = LoggerFactory.getLogger(ThriftCompositeFactory.class);

    private ThriftPropertyHelper helper = new ThriftPropertyHelper();
    private MethodInvoker invoker = new MethodInvoker();

    public <K, V, T> Composite createBaseComposite(PropertyMeta<K, V> propertyMeta, T key)
    {
        log.trace("Creating base composite for propertyMeta {}", propertyMeta.getPropertyName());

        Composite composite = new Composite();
        String propertyName = propertyMeta.getPropertyName();

        if (propertyMeta.isSingleKey())
        {
            log.trace("PropertyMeta {} is single key", propertyMeta.getPropertyName());
            Validator.validateNotNull(key, "The values for the for the key of WideMap '"
                    + propertyName + "' should not be null");

            Serializer<T> keySerializer = ThriftSerializerTypeInferer.getSerializer(propertyMeta
                    .getKeyClass());
            composite.setComponent(0, key, keySerializer, keySerializer
                    .getComparatorType()
                    .getTypeName());
        }
        else
        {
            log.trace("PropertyMeta {} is multi key", propertyMeta.getPropertyName());
            List<Serializer<Object>> componentSerializers = getComponentSerializers(propertyMeta
                    .getComponentClasses());
            List<Object> keyComponents = invoker.extractCompoundKeyComponents(key,
                    propertyMeta.getComponentGetters());
            int srzCount = componentSerializers.size();
            int valueCount = keyComponents.size();

            Validator.validateTrue(srzCount == valueCount, "There should be " + srzCount
                    + " values for the key of WideMap '" + propertyName + "'");

            for (Object value : keyComponents)
            {
                Validator.validateNotNull(value, "The values for the for the key of WideMap '"
                        + propertyName + "' should not be null");
            }

            for (int i = 0; i < srzCount; i++)
            {
                Serializer<Object> srz = componentSerializers.get(i);
                composite.setComponent(i, keyComponents.get(i), srz, srz
                        .getComparatorType()
                        .getTypeName());
            }
        }
        return composite;
    }

    public <K, V, T> Composite createForQuery(PropertyMeta<K, V> propertyMeta, T key,
            ComponentEquality equality)
    {
        log.trace("Creating query composite for propertyMeta {}", propertyMeta.getPropertyName());

        Composite composite = new Composite();
        String propertyName = propertyMeta.getPropertyName();

        if (propertyMeta.isSingleKey())
        {
            log.trace("PropertyMeta {} is single key", propertyMeta.getPropertyName());

            if (key == null)
            {
                composite = null;
            }
            else
            {
                composite.addComponent(0, key, equality);
            }
        }
        else
        {
            log.trace("PropertyMeta {} is multi key", propertyMeta.getPropertyName());

            List<Serializer<Object>> componentSerializers = getComponentSerializers(propertyMeta
                    .getComponentClasses());

            List<Object> keyComponents = invoker.extractCompoundKeyComponents(key,
                    propertyMeta.getComponentGetters());
            int srzCount = componentSerializers.size();
            int valueCount = keyComponents.size();

            Validator.validateTrue(srzCount >= valueCount, "There should be at most" + srzCount
                    + " values for the key of WideMap '" + propertyName + "'");

            int lastNotNullIndex = helper
                    .findLastNonNullIndexForComponents(propertyName, keyComponents);

            for (int i = 0; i <= lastNotNullIndex; i++)
            {
                Serializer<Object> srz = componentSerializers.get(i);
                Object value = keyComponents.get(i);
                if (i < lastNotNullIndex)
                {
                    composite.setComponent(i, value, srz, srz.getComparatorType().getTypeName(),
                            EQUAL);
                }
                else
                {
                    composite.setComponent(i, value, srz, srz.getComparatorType().getTypeName(),
                            equality);
                }
            }
        }
        return composite;
    }

    private List<Serializer<Object>> getComponentSerializers(List<Class<?>> componentClasses)
    {
        List<Serializer<Object>> componentSerializers = new ArrayList<Serializer<Object>>();
        for (Class<?> clazz : componentClasses)
        {
            componentSerializers.add(ThriftSerializerTypeInferer.getSerializer(clazz));
        }
        return componentSerializers;
    }

    public <K, V> Composite[] createForQuery(PropertyMeta<K, V> propertyMeta, K start, K end,
            WideMap.BoundingMode bounds, WideMap.OrderingMode ordering)
    {
        log
                .trace("Creating query composite for propertyMeta {} with start {}, end {}, bounding mode {} and orderging {}",
                        propertyMeta.getPropertyName(), start, end, bounds.name(), ordering.name());

        Composite[] queryComp = new Composite[2];

        ComponentEquality[] equalities = helper.determineEquality(bounds, ordering);

        Composite startComp = createForQuery(propertyMeta, start, equalities[0]);
        Composite endComp = createForQuery(propertyMeta, end, equalities[1]);

        queryComp[0] = startComp;
        queryComp[1] = endComp;

        return queryComp;
    }

    public Composite createKeyForCounter(String fqcn, Object key, PropertyMeta<?, ?> idMeta)
    {
        log.trace("Creating composite counter row key for entity class {} and primary key {}",
                fqcn, key);

        Composite comp = new Composite();
        comp.setComponent(0, fqcn, STRING_SRZ);
        comp.setComponent(1, idMeta.writeValueToString(key), STRING_SRZ);
        return comp;
    }

    public <K, V> Composite createBaseForGet(PropertyMeta<K, V> propertyMeta)
    {
        log
                .trace("Creating base composite for propertyMeta {} get",
                        propertyMeta.getPropertyName());

        Composite composite = new Composite();
        composite.addComponent(0, propertyMeta.type().flag(), ComponentEquality.EQUAL);
        composite.addComponent(1, propertyMeta.getPropertyName(), ComponentEquality.EQUAL);
        composite.addComponent(2, 0, ComponentEquality.EQUAL);
        return composite;
    }

    public <K, V> Composite createBaseForCounterGet(PropertyMeta<K, V> propertyMeta)
    {
        log
                .trace("Creating base composite for propertyMeta {} get",
                        propertyMeta.getPropertyName());

        Composite composite = new Composite();
        composite.addComponent(0, propertyMeta.getPropertyName(), ComponentEquality.EQUAL);
        return composite;
    }

    public <K, V> Composite createBaseForQuery(PropertyMeta<K, V> propertyMeta,
            ComponentEquality equality)
    {
        log.trace("Creating base composite for propertyMeta {} query and equality {}",
                propertyMeta.getPropertyName(), equality.name());

        Composite composite = new Composite();
        composite.addComponent(0, propertyMeta.type().flag(), ComponentEquality.EQUAL);
        composite.addComponent(1, propertyMeta.getPropertyName(), equality);
        return composite;
    }

    public <K, V> Composite createForBatchInsertSingleValue(PropertyMeta<K, V> propertyMeta)
    {
        log.trace("Creating base composite for propertyMeta {} for single value batch insert",
                propertyMeta.getPropertyName());

        Composite composite = new Composite();
        composite.setComponent(0, propertyMeta.type().flag(), BYTE_SRZ, BYTE_SRZ
                .getComparatorType()
                .getTypeName());
        composite.setComponent(1, propertyMeta.getPropertyName(), STRING_SRZ, STRING_SRZ
                .getComparatorType()
                .getTypeName());
        composite.setComponent(2, 0, INT_SRZ, INT_SRZ.getComparatorType().getTypeName());
        return composite;
    }

    public <K, V> Composite createForBatchInsertSingleCounter(PropertyMeta<K, V> propertyMeta)
    {
        log
                .trace("Creating base composite for propertyMeta {} for single counter value batch insert",
                        propertyMeta.getPropertyName());

        Composite composite = new Composite();
        composite.setComponent(0, propertyMeta.getPropertyName(), STRING_SRZ, STRING_SRZ
                .getComparatorType()
                .getTypeName());
        return composite;
    }

    public <K, V> Composite createForBatchInsertMultiValue(PropertyMeta<K, V> propertyMeta,
            int hashOrPosition)
    {
        log
                .trace("Creating base composite for propertyMeta {} for multi value batch insert with hash or position {}",
                        propertyMeta.getPropertyName(), hashOrPosition);

        Composite composite = new Composite();
        composite.setComponent(0, propertyMeta.type().flag(), BYTE_SRZ, BYTE_SRZ
                .getComparatorType()
                .getTypeName());
        composite.setComponent(1, propertyMeta.getPropertyName(), STRING_SRZ, STRING_SRZ
                .getComparatorType()
                .getTypeName());
        composite.setComponent(2, hashOrPosition, INT_SRZ, INT_SRZ
                .getComparatorType()
                .getTypeName());
        return composite;
    }

}
