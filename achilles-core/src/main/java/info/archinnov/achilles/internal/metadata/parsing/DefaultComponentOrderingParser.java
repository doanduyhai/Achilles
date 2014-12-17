package info.archinnov.achilles.internal.metadata.parsing;

import com.google.common.collect.FluentIterable;
import info.archinnov.achilles.annotations.ClusteringColumn;
import info.archinnov.achilles.annotations.PartitionKey;
import info.archinnov.achilles.internal.metadata.parsing.context.PropertyParsingContext;
import info.archinnov.achilles.internal.validation.Validator;
import org.reflections.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.*;

import static info.archinnov.achilles.schemabuilder.Create.Options.ClusteringOrder;
import static info.archinnov.achilles.schemabuilder.Create.Options.ClusteringOrder.Sorting;
import static java.lang.String.format;

public class DefaultComponentOrderingParser extends ComponentOrderingParser {

    private static final Logger log = LoggerFactory.getLogger(DefaultComponentOrderingParser.class);

    public DefaultComponentOrderingParser(PropertyParsingContext context) {
        super(context);
    }

    @Override
    Map<Integer, Field> extractComponentsOrdering(Class<?> embeddedIdClass) {

        log.trace("Extract components ordering from embedded id class {} ", embeddedIdClass.getCanonicalName());

        String embeddedIdClassName = embeddedIdClass.getCanonicalName();


        final List<Field> partitionComponents =
                FluentIterable.from(ReflectionUtils.getAllFields(embeddedIdClass, ReflectionUtils.<Field>withAnnotation(PartitionKey.class)))
                .toSortedList(new Comparator<Field>() {
                    @Override
                    public int compare(Field o1, Field o2) {
                        PartitionKey order1 = o1.getAnnotation(PartitionKey.class);
                        PartitionKey order2 = o2.getAnnotation(PartitionKey.class);
                        return new Integer(order1.value()).compareTo(new Integer(order2.value()));
                    }
                });

        final List<Field> clusteringColumns =
                FluentIterable.from(ReflectionUtils.getAllFields(embeddedIdClass, ReflectionUtils.<Field>withAnnotation(ClusteringColumn.class)))
                .toSortedList(new Comparator<Field>() {
                    @Override
                    public int compare(Field o1, Field o2) {
                        ClusteringColumn order1 = o1.getAnnotation(ClusteringColumn.class);
                        ClusteringColumn order2 = o2.getAnnotation(ClusteringColumn.class);
                        return new Integer(order1.value()).compareTo(new Integer(order2.value()));
                    }
                });


        validatePartitionComponentsOrder(partitionComponents, format("The partition components ordering is wrong for @EmbeddedId class '%s'", embeddedIdClassName));
        validateClusteringColumnsOrder(clusteringColumns, format("The clustering keys ordering is wrong for @EmbeddedId class '%s'", embeddedIdClassName));


        Map<Integer, Field> result = new HashMap<>();
        for (int i = 1; i <= partitionComponents.size() + clusteringColumns.size(); i++) {
            if (i <= partitionComponents.size()) {
                result.put(i, partitionComponents.get(i - 1));
            } else {
                result.put(i, clusteringColumns.get(i - 1 - partitionComponents.size()));
            }
        }

        Validator.validateBeanMappingTrue(result.size()>1, format("There should be at least 2 fields annotated with @PartitionKey or @ClusteringColumn for the @EmbeddedId class '%s'",embeddedIdClassName));
        return result;
    }

    @Override
    List<ClusteringOrder> extractClusteringOrder(Class<?> embeddedIdClass) {

        log.trace("Extract clustering component order from embedded id class {} ",embeddedIdClass.getCanonicalName());

        final List<Field> clusteringColumns =
                FluentIterable.from(ReflectionUtils.getAllFields(embeddedIdClass, ReflectionUtils.<Field>withAnnotation(ClusteringColumn.class)))
                        .toSortedList(new Comparator<Field>() {
                            @Override
                            public int compare(Field o1, Field o2) {
                                ClusteringColumn order1 = o1.getAnnotation(ClusteringColumn.class);
                                ClusteringColumn order2 = o2.getAnnotation(ClusteringColumn.class);
                                return new Integer(order1.value()).compareTo(new Integer(order2.value()));
                            }
                        });

        List<ClusteringOrder> sortOrders = new ArrayList<>();
        for (Field clusteringColumn : clusteringColumns) {
            final ClusteringColumn annotation = clusteringColumn.getAnnotation(ClusteringColumn.class);
            final String cqlColumnName = introspector.inferCQLColumnName(clusteringColumn, context.getClassNamingStrategy());
            validateNotStaticColumn(clusteringColumn);
            sortOrders.add(new ClusteringOrder(cqlColumnName, annotation.reversed() ? Sorting.DESC : Sorting.ASC));

        }
        return sortOrders;
    }

    private void validatePartitionComponentsOrder(List<Field> partitionComponents, String errorMessage) {
        int clusteringOrderSum = 0;
        for (Field partitionComponent : partitionComponents) {
            final PartitionKey partitionKey = partitionComponent.getAnnotation(PartitionKey.class);
            clusteringOrderSum += partitionKey.value();
        }

        /**
         * Math formula : sum of N consecutive integers = N * (N+1)/2
         */
        int checkForPartitionKey = partitionComponents.size() * (partitionComponents.size() + 1) / 2;
        Validator.validateBeanMappingTrue(checkForPartitionKey == clusteringOrderSum, errorMessage);
    }

    private void validateClusteringColumnsOrder(List<Field> clusteringColumns, String errorMessage) {
        int clusteringOrderSum = 0;
        for (Field clusteringColumn : clusteringColumns) {
            final ClusteringColumn clusteringColumnAnnotation = clusteringColumn.getAnnotation(ClusteringColumn.class);
            clusteringOrderSum += clusteringColumnAnnotation.value();
        }

        /**
         * Math formula : sum of N consecutive integers = N * (N+1)/2
         */
        int checkForClusteringColumns = clusteringColumns.size() * (clusteringColumns.size() + 1) / 2;
        Validator.validateBeanMappingTrue(checkForClusteringColumns == clusteringOrderSum, errorMessage);
    }
}
