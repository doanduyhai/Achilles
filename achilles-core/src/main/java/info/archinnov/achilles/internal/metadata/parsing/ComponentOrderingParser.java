package info.archinnov.achilles.internal.metadata.parsing;

import info.archinnov.achilles.annotations.ClusteringColumn;
import info.archinnov.achilles.annotations.Column;
import info.archinnov.achilles.annotations.Order;
import info.archinnov.achilles.annotations.PartitionKey;
import info.archinnov.achilles.exception.AchillesBeanMappingException;
import info.archinnov.achilles.internal.metadata.parsing.context.PropertyParsingContext;
import info.archinnov.achilles.schemabuilder.Create;
import org.reflections.ReflectionUtils;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.lang.String.format;

public abstract class ComponentOrderingParser {

    protected EntityIntrospector introspector = EntityIntrospector.Singleton.INSTANCE.get();

    protected PropertyParsingContext context;

    public static ComponentOrderingParser determineAppropriateParser(Class<?> embeddedIdClass, PropertyParsingContext context) {
        final Set<Field> partitionKeyAnnotations = ReflectionUtils.getAllFields(embeddedIdClass, ReflectionUtils.<Field>withAnnotation(PartitionKey.class));
        final Set<Field> clusteringColumnAnnotations = ReflectionUtils.getAllFields(embeddedIdClass, ReflectionUtils.<Field>withAnnotation(ClusteringColumn.class));
        final Set<Field> orderAnnotations = ReflectionUtils.getAllFields(embeddedIdClass, ReflectionUtils.<Field>withAnnotation(Order.class));

        if (clusteringColumnAnnotations.size() > 0 && orderAnnotations.size() > 0) {
            throw new AchillesBeanMappingException(format("You should stop using the deprecated @Order annotation in favor of @PartitionKey and @ClusteringColumn for the @EmbeddedId class '%s'", embeddedIdClass.getCanonicalName()));
        }

        if (clusteringColumnAnnotations.size() + orderAnnotations.size() + partitionKeyAnnotations.size() == 0) {
            throw new AchillesBeanMappingException(format("Please use @PartitionKey and @ClusteringColumn annotations for the @EmbeddedId class '%s'", embeddedIdClass.getCanonicalName()));
        }

        if (orderAnnotations.size() > 0) {
            return new LegacyComponentOrderingParser(context);
        } else {
            return new DefaultComponentOrderingParser(context);
        }
    }

    public ComponentOrderingParser(PropertyParsingContext context) {
        this.context = context;
    }

    abstract Map<Integer, Field> extractComponentsOrdering(Class<?> embeddedIdClass);

    abstract List<Create.Options.ClusteringOrder> extractClusteringOrder(Class<?> embeddedIdClass);

    protected void validateNotStaticColumn(Field field) {
        Column column = field.getAnnotation(Column.class);
        if (column != null && column.staticColumn()) {
            throw new AchillesBeanMappingException(format("The property '%s' of class '%s' cannot be a static column because it belongs to the primary key", field.getName(), field.getDeclaringClass().getCanonicalName()));
        }
    }
}
