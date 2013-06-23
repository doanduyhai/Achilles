package info.archinnov.achilles.compound;

import static info.archinnov.achilles.helper.ThriftLoggerHelper.*;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.*;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.helper.ThriftPropertyHelper;
import info.archinnov.achilles.proxy.MethodInvoker;
import info.archinnov.achilles.validation.Validator;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.AbstractComposite.Component;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.Composite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.FluentIterable;

public class ThriftCompoundKeyMapper {

    private static final Logger log = LoggerFactory.getLogger(ThriftCompoundKeyMapper.class);
    private static final ClassToSerializerTransformer classToSerializer = new ClassToSerializerTransformer();

    private ThriftPropertyHelper helper = new ThriftPropertyHelper();
    private MethodInvoker invoker = new MethodInvoker();

    public <K, V> K readFromComposite(PropertyMeta<K, V> propertyMeta,
            List<Component<?>> components)
    {
        if (log.isTraceEnabled())
        {
            log.trace("Build compound key {} from composite components {}",
                    propertyMeta.getPropertyName(), format(components));
        }

        K compoundKey;
        Constructor<K> constructor = propertyMeta.getCompoundKeyConstructor();
        List<Method> componentSetters = propertyMeta.getComponentSetters();

        List<Serializer<Object>> serializers = FluentIterable
                .from(propertyMeta.getComponentClasses())
                .transform(classToSerializer)
                .toImmutableList();

        int componentCount = components.size();

        try {
            if (propertyMeta.hasDefaultConstructorForCompoundKey())
            {
                compoundKey = injectValuesBySetter(components, constructor, componentSetters, serializers,
                        componentCount);
            }
            else {
                compoundKey = injectValuesByConstructor(components, constructor, serializers, componentCount);
            }
        } catch (Exception e) {
            throw new AchillesException(e);
        }

        log.trace("Built compound key : {}", compoundKey);

        return compoundKey;
    }

    public Composite writeToComposite(Object compoundKey, PropertyMeta<?, ?> pm)
    {
        String propertyName = pm.getPropertyName();
        log.trace("Build composite to persist @CompoundKey : ", propertyName);

        Composite composite = new Composite();

        List<Serializer<Object>> serializers = FluentIterable
                .from(pm.getComponentClasses())
                .transform(classToSerializer)
                .toImmutableList();

        List<Object> componentValues = invoker.extractCompoundKeyComponents(compoundKey,
                pm.getComponentGetters());
        int srzCount = serializers.size();

        for (Object value : componentValues)
        {
            Validator.validateNotNull(value, "The values for the @CompoundKey '"
                    + propertyName + "' should not be null");
        }

        for (int i = 0; i < srzCount; i++)
        {
            Serializer<Object> srz = serializers.get(i);
            composite.setComponent(i, componentValues.get(i), srz, srz
                    .getComparatorType()
                    .getTypeName());
        }

        return composite;
    }

    public Composite buildCompositeForQuery(Object compoundKey, PropertyMeta<?, ?> pm, ComponentEquality equality)
    {
        String propertyName = pm.getPropertyName();
        log.trace("Build composite to query @CompoundKey : ", propertyName);

        Composite composite = new Composite();

        List<Serializer<Object>> serializers = FluentIterable
                .from(pm.getComponentClasses())
                .transform(classToSerializer)
                .toImmutableList();

        List<Object> componentValues = invoker.extractCompoundKeyComponents(compoundKey,
                pm.getComponentGetters());

        int srzCount = serializers.size();

        Validator.validateTrue(srzCount >= componentValues.size(), "There should be at most " + srzCount
                + " values for the @CompoundKey '" + propertyName + "'");

        int lastNotNullIndex = helper
                .findLastNonNullIndexForComponents(propertyName, componentValues);

        for (int i = 0; i <= lastNotNullIndex; i++)
        {
            Serializer<Object> srz = serializers.get(i);
            Object value = componentValues.get(i);
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

        return composite;
    }

    private <K> K injectValuesBySetter(List<Component<?>> components, Constructor<K> constructor,
            List<Method> componentSetters, List<Serializer<Object>> serializers, int componentCount)
            throws InstantiationException, IllegalAccessException, InvocationTargetException {

        K compoundKey = constructor.newInstance();
        log.trace("Built and inject value into compound key : {} by setters", compoundKey);

        for (int i = 0; i < componentCount; i++)
        {
            Component<?> comp = components.get(i);
            Object compValue = serializers.get(i).fromByteBuffer(comp.getBytes());
            invoker.setValueToField(compoundKey, componentSetters.get(i), compValue);
        }
        return compoundKey;
    }

    private <K> K injectValuesByConstructor(List<Component<?>> components, Constructor<K> constructor,
            List<Serializer<Object>> serializers, int componentCount) throws InstantiationException,
            IllegalAccessException, InvocationTargetException {
        K compoundKey;
        Object[] constructorParams = new Object[componentCount];
        for (int i = 0; i < componentCount; i++)
        {
            Component<?> comp = components.get(i);
            constructorParams[i] = serializers.get(i).fromByteBuffer(comp.getBytes());
        }
        compoundKey = constructor.newInstance(constructorParams);
        log.trace("Built and inject value into compound key : {} by constructor", compoundKey);
        return compoundKey;
    }
}
