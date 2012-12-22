package fr.doan.achilles.composite.factory;

import static fr.doan.achilles.serializer.Utils.BYTE_SRZ;
import static fr.doan.achilles.serializer.Utils.INT_SRZ;
import static fr.doan.achilles.serializer.Utils.STRING_SRZ;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.EQUAL;
import java.lang.reflect.Method;
import java.util.List;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.DynamicComposite;
import fr.doan.achilles.entity.metadata.PropertyType;
import fr.doan.achilles.helper.CompositeHelper;
import fr.doan.achilles.proxy.EntityWrapperUtil;
import fr.doan.achilles.validation.Validator;

/**
 * DynamicCompositeKeyFactory
 * 
 * @author DuyHai DOAN
 * 
 */
public class DynamicCompositeKeyFactory {

    private CompositeHelper helper = new CompositeHelper();
    private EntityWrapperUtil util = new EntityWrapperUtil();

    public DynamicComposite createForInsert(String propertyName, PropertyType type, int hashOrPosition) {
        DynamicComposite composite = new DynamicComposite();
        composite.setComponent(0, type.flag(), BYTE_SRZ, BYTE_SRZ.getComparatorType().getTypeName());
        composite.setComponent(1, propertyName, STRING_SRZ, STRING_SRZ.getComparatorType().getTypeName());
        composite.setComponent(2, hashOrPosition, INT_SRZ, INT_SRZ.getComparatorType().getTypeName());
        return composite;
    }

    public <T> DynamicComposite createForInsert(String propertyName, PropertyType type, T value,
            Serializer<T> valueSerializer) {
        DynamicComposite composite = new DynamicComposite();
        composite.setComponent(0, type.flag(), BYTE_SRZ, BYTE_SRZ.getComparatorType().getTypeName());
        composite.setComponent(1, propertyName, STRING_SRZ, STRING_SRZ.getComparatorType().getTypeName());
        composite.setComponent(2, value, valueSerializer, valueSerializer.getComparatorType().getTypeName());
        return composite;
    }

    public DynamicComposite createBaseForQuery(String propertyName, PropertyType type, ComponentEquality equality) {
        DynamicComposite composite = new DynamicComposite();
        composite.addComponent(0, type.flag(), ComponentEquality.EQUAL);
        composite.addComponent(1, propertyName, equality);

        return composite;
    }

    public DynamicComposite createForQuery(String propertyName, PropertyType type, Object value,
            ComponentEquality equality) {
        DynamicComposite composite = new DynamicComposite();
        composite.addComponent(0, type.flag(), ComponentEquality.EQUAL);

        if (value != null) {
            composite.addComponent(1, propertyName, ComponentEquality.EQUAL);
            composite.addComponent(2, value, equality);
        } else {
            composite.addComponent(1, propertyName, equality);
        }

        return composite;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public DynamicComposite createForInsertMultiKey(String propertyName, PropertyType type, List<Object> keyValues,
            List<Serializer<?>> serializers) {
        int srzCount = serializers.size();
        int valueCount = keyValues.size();

        Validator.validateTrue(srzCount == valueCount, "There should be " + srzCount
                + " values for the key of WideMap '" + propertyName + "'");

        for (Object keyValue : keyValues) {
            Validator.validateNotNull(keyValue, "The values for the for the key of WideMap '" + propertyName
                    + "' should not be null");
        }

        DynamicComposite composite = new DynamicComposite();
        composite.setComponent(0, type.flag(), BYTE_SRZ, BYTE_SRZ.getComparatorType().getTypeName());
        composite.setComponent(1, propertyName, STRING_SRZ, STRING_SRZ.getComparatorType().getTypeName());

        for (int i = 0; i < srzCount; i++) {
            Serializer srz = serializers.get(i);
            composite.setComponent(i + 2, keyValues.get(i), srz, srz.getComparatorType().getTypeName());
        }

        return composite;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public DynamicComposite createForQueryMultiKey(String propertyName, PropertyType type, List<Object> keyValues,
            List<Serializer<?>> serializers, ComponentEquality equality) {
        int srzCount = serializers.size();
        int valueCount = keyValues.size();

        Validator.validateTrue(srzCount >= valueCount, "There should be at most" + srzCount
                + " values for the key of WideMap '" + propertyName + "'");

        int lastNotNullIndex = helper.findLastNonNullIndexForComponents(propertyName, keyValues);

        DynamicComposite composite = new DynamicComposite();
        composite.setComponent(0, type.flag(), BYTE_SRZ, BYTE_SRZ.getComparatorType().getTypeName(), EQUAL);
        composite.setComponent(1, propertyName, STRING_SRZ, STRING_SRZ.getComparatorType().getTypeName(), EQUAL);

        for (int i = 0; i <= lastNotNullIndex; i++) {
            Serializer srz = serializers.get(i);
            Object keyValue = keyValues.get(i);
            if (i < lastNotNullIndex) {
                composite.setComponent(i + 2, keyValue, srz, srz.getComparatorType().getTypeName(), EQUAL);
            } else {
                composite.setComponent(i + 2, keyValue, srz, srz.getComparatorType().getTypeName(), equality);
            }
        }

        return composite;
    }

    public <K> DynamicComposite[] createForQuery(String propertyName, PropertyType propertyType, K start,
            boolean inclusiveStart, K end, boolean inclusiveEnd, boolean reverse) {
        DynamicComposite[] queryComp = new DynamicComposite[2];

        ComponentEquality[] equalities = helper.determineEquality(inclusiveStart, inclusiveEnd, reverse);

        DynamicComposite startComp = this.createForQuery(propertyName, propertyType, start, equalities[0]);
        DynamicComposite endComp = this.createForQuery(propertyName, propertyType, end, equalities[1]);

        queryComp[0] = startComp;
        queryComp[1] = endComp;

        return queryComp;
    }

    public <K> DynamicComposite[] createForMultiKeyQuery(String propertyName, PropertyType propertyType,
            List<Serializer<?>> componentSerializers, List<Method> componentGetters, K start, boolean inclusiveStart,
            K end, boolean inclusiveEnd, boolean reverse) {
        DynamicComposite[] queryComp = new DynamicComposite[2];
        ComponentEquality[] equalities = helper.determineEquality(inclusiveStart, inclusiveEnd, reverse);

        List<Object> startComponentValues = util.determineMultiKey(start, componentGetters);
        List<Object> endComponentValues = util.determineMultiKey(end, componentGetters);

        DynamicComposite startComp = this.createForQueryMultiKey(propertyName, propertyType, startComponentValues,
                componentSerializers, equalities[0]);
        DynamicComposite endComp = this.createForQueryMultiKey(propertyName, propertyType, endComponentValues,
                componentSerializers, equalities[1]);

        queryComp[0] = startComp;
        queryComp[1] = endComp;

        return queryComp;
    }

}
