package info.archinnov.achilles.internal.metadata.parsing;

import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import info.archinnov.achilles.annotations.Order;
import info.archinnov.achilles.annotations.PartitionKey;
import info.archinnov.achilles.internal.metadata.parsing.context.PropertyParsingContext;
import info.archinnov.achilles.internal.validation.Validator;
import info.archinnov.achilles.schemabuilder.Create;
import org.reflections.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.*;

import static org.reflections.ReflectionUtils.getAllFields;

public class LegacyComponentOrderingParser extends ComponentOrderingParser {

    private static final Logger log = LoggerFactory.getLogger(LegacyComponentOrderingParser.class);

    private PropertyFilter filter = PropertyFilter.Singleton.INSTANCE.get();

    public LegacyComponentOrderingParser(PropertyParsingContext context) {
        super(context);
    }

    @Override
    Map<Integer, Field> extractComponentsOrdering(Class<?> compoundPKClass) {
        log.trace("Extract components ordering from compound primary key class {} ", compoundPKClass.getCanonicalName());

        String compoundPKClassName = compoundPKClass.getCanonicalName();
        Map<Integer, Field> components = new TreeMap<>();

        @SuppressWarnings("unchecked")
        Set<Field> candidateFields = getAllFields(compoundPKClass, ReflectionUtils.<Field>withAnnotation(Order.class));

        Set<Integer> orders = new HashSet<>();
        int orderSum = 0;
        int componentCount = candidateFields.size();

        for (Field candidateField : candidateFields) {
            Order orderAnnotation = candidateField.getAnnotation(Order.class);
            int order = orderAnnotation.value();
            orderSum = validateNoDuplicateOrderAndType(compoundPKClassName, orders, orderSum, order);
            components.put(order, candidateField);
        }

        validateConsistentPartitionKeys(components, compoundPKClassName);
        validateConsistentOrdering(compoundPKClassName, orderSum, componentCount);
        Validator.validateBeanMappingTrue(componentCount > 1,"There should be at least 2 fields annotated with @Order for the @CompoundPrimaryKey class '%s'",
                compoundPKClass.getCanonicalName());
        return components;
    }

    @Override
    List<Create.Options.ClusteringOrder> extractClusteringOrder(Class<?> compoundPKClass) {
        log.trace("Extract clustering component order from compound primary key class {} ", compoundPKClass.getCanonicalName());

        List<Create.Options.ClusteringOrder> sortOrders = new ArrayList<>();

        @SuppressWarnings("unchecked")
        Set<Field> candidateFields = getAllFields(compoundPKClass, ReflectionUtils.withAnnotation(Order.class));
        final List<Field> clusteringFields = FluentIterable.from(candidateFields).filter(new Predicate<Field>() {
            @Override
            public boolean apply(Field field) {
                Order orderAnnotation = field.getAnnotation(Order.class);
                return !filter.hasAnnotation(field, PartitionKey.class) && orderAnnotation.value() > 1;
            }
        }).toSortedList(new Comparator<Field>() {
            @Override
            public int compare(Field o1, Field o2) {
                Order order1 = o1.getAnnotation(Order.class);
                Order order2 = o2.getAnnotation(Order.class);
                return new Integer(order1.value()).compareTo(new Integer(order2.value()));
            }
        });

        for (Field clusteringField : clusteringFields) {
            final Order order = clusteringField.getAnnotation(Order.class);
            final String cqlColumnName = introspector.inferCQLColumnName(clusteringField, context.getClassNamingStrategy());
            validateNotStaticColumn(clusteringField);
            sortOrders.add(new Create.Options.ClusteringOrder(cqlColumnName, order.reversed() ? Create.Options.ClusteringOrder.Sorting.DESC : Create.Options.ClusteringOrder.Sorting.ASC));
        }

        return sortOrders;
    }


    private int validateNoDuplicateOrderAndType(String compoundPKClassName, Set<Integer> orders, int orderSum,int order) {
        log.debug("Validate type and component ordering for compound primary key class {} ", compoundPKClassName);
        Validator.validateBeanMappingTrue(orders.add(order), "The order '%s' is duplicated in @CompoundPrimaryKey class '%s'", order, compoundPKClassName);

        orderSum += order;

        return orderSum;
    }

    private void validateConsistentOrdering(String compoundPKClassName, int orderSum, int componentCount) {
        int check = (componentCount * (componentCount + 1)) / 2;

        log.debug("Validate component ordering for @CompoundPrimaryKey class {} ", compoundPKClassName);

        Validator.validateBeanMappingTrue(orderSum == check, "The component ordering is wrong for @CompoundPrimaryKey class '%s'", compoundPKClassName);
    }

    private void validateConsistentPartitionKeys(Map<Integer, Field> componentsOrdering, String compoundPKClassName) {
        log.debug("Validate composite partition key component ordering for @CompoundPrimaryKey class {} ", compoundPKClassName);
        int orderSum = 0;
        int orderCount = 0;
        for (Integer order : componentsOrdering.keySet()) {
            Field componentField = componentsOrdering.get(order);
            if (filter.hasAnnotation(componentField, PartitionKey.class)) {
                orderSum = orderSum + order;
                orderCount++;
            }
        }

        /**
         * Math formula : sum of N consecutive integers = N * (N+1)/2
         */
        int check = (orderCount * (orderCount + 1)) / 2;
        Validator.validateBeanMappingTrue(orderSum == check, "The composite partition key ordering is wrong for @CompoundPrimaryKey class '%s'", compoundPKClassName);
    }
}
