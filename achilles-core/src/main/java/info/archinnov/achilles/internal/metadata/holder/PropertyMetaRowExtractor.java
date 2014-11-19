package info.archinnov.achilles.internal.metadata.holder;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.Row;
import com.google.common.collect.FluentIterable;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta.EntityState;
import info.archinnov.achilles.internal.metadata.parsing.NamingHelper;
import info.archinnov.achilles.internal.validation.Validator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static info.archinnov.achilles.internal.cql.TypeMapper.getRowMethod;
import static info.archinnov.achilles.internal.cql.TypeMapper.toCompatibleJavaType;
import static info.archinnov.achilles.internal.metadata.parsing.NamingHelper.TO_LOWER_CASE;
import static java.lang.String.format;

public class PropertyMetaRowExtractor extends PropertyMetaView{

    private static final Logger log = LoggerFactory.getLogger(PropertyMetaRowExtractor.class);

    protected PropertyMetaRowExtractor(PropertyMeta meta) {
        super(meta);
    }

    public List<Object> extractRawCompoundPrimaryComponentsFromRow(Row row) {
        log.trace("Extract raw compound primary components from CQL3 row {} for id meta {}", row, meta);
        Validator.validateNotNull(meta.getEmbeddedIdProperties(), "Cannot extract raw compound primary keys from CQL3 row because entity '%s' does not have a compound primary key", meta.getEntityClassName());
        final List<Class<?>> componentClasses = meta.getEmbeddedIdProperties().getCQL3ComponentClasses();
        final List<String> cql3ComponentNames = meta.getEmbeddedIdProperties().getCQL3ComponentNames();
        List<Object> rawValues = new ArrayList<>(Collections.nCopies(cql3ComponentNames.size(), null));
        try {
            for (int index=0; index<cql3ComponentNames.size(); index++) {
                Object rawValue;
                Class<?> componentClass = componentClasses.get(index);
                rawValue = getRowMethod(componentClass).invoke(row, cql3ComponentNames.get(index));
                rawValues.set(index, rawValue);
            }
        } catch (Exception e) {
            throw new AchillesException(format("Cannot retrieve compound primary key for entity class '%s' from CQL Row", meta.getEntityClassName()), e);
        }
        return rawValues;
    }

    public Object invokeOnRowForFields(Row row) {
        if(log.isTraceEnabled()) {
            log.trace("Extract column {} from CQL3 row {} for id meta {}", meta.getCQL3ColumnName(), row, meta);
        }

        Object value = null;
        if (!row.isNull(meta.getCQL3ColumnName())) {
            switch (meta.type()) {
                case LIST:
                    value = invokeOnRowForList(row);
                    break;
                case SET:
                    value = invokeOnRowForSet(row);
                    break;
                case MAP:
                    value = invokeOnRowForMap(row);
                    break;
                case ID:
                case SIMPLE:
                    value = invokeOnRowForProperty(row);
                    break;
                default:
                    break;
            }
        } else {
            switch (meta.type()) {
                case LIST:
                case SET:
                case MAP:
                    value = meta.forValues().nullValueForCollectionAndMap();
                    break;
                default:
                    break;
            }
        }
        return value;
    }

    public Object extractCompoundPrimaryKeyFromRow(Row row, EntityMeta entityMeta, EntityState entityState) {
        log.trace("Extract compound primary key {} from CQL3 row for entity class {}", meta.getPropertyName(),meta.getEntityClassName());
        final List<Object> rawComponents = extractRawCompoundPrimaryComponentsFromRow(row);
        if (entityState.isManaged() && !entityMeta.structure().hasOnlyStaticColumns()) {
           validateExtractedCompoundPrimaryComponents(rawComponents);
        }
        return meta.forTranscoding().decodeFromComponents(rawComponents);

    }

    public void validateExtractedCompoundPrimaryComponents(List<Object> rawComponents) {
        log.trace("Validate raw compound primary components {} for id meta {}", rawComponents, meta);

        Validator.validateNotNull(meta.getEmbeddedIdProperties(), "Cannot validate raw compound primary keys from CQL3 row because entity '%s' does not have a compound primary key",meta.getEntityClassName());
        final List<String> cql3ComponentNames = meta.getEmbeddedIdProperties().getCQL3ComponentNames();
        for (int i = 0; i < cql3ComponentNames.size(); i++) {
            Validator.validateNotNull(rawComponents.get(i), "Error, the component '%s' from @EmbeddedId class '%s' is null in Cassandra", cql3ComponentNames.get(i), meta.getValueClass());
        }
    }

    private Object invokeOnRowForProperty(Row row) {
        final String cql3ColumnName = meta.getCQL3ColumnName();
        final String entityClassName = meta.getEntityClassName();
        log.trace("Extract property {} from CQL3 row for entity class {}", cql3ColumnName, entityClassName);
        try {
            Object rawValue = getRowMethod(meta.getCql3ValueClass()).invoke(row, cql3ColumnName);
            return meta.forTranscoding().decodeFromCassandra(rawValue);
        } catch (Exception e) {
            throw new AchillesException(String.format("Cannot retrieve property '%s' for entity class '%S' from CQL Row", cql3ColumnName, entityClassName), e);
        }
    }

    private Object invokeOnRowForList(Row row) {
        final String cql3ColumnName = meta.getCQL3ColumnName();
        final String entityClassName = meta.getEntityClassName();
        log.trace("Extract list property {} from CQL3 row for entity class {}", cql3ColumnName, entityClassName);
        try {
            List<?> rawValues = row.getList(cql3ColumnName, toCompatibleJavaType(meta.getCql3ValueClass()));
            return meta.forTranscoding().decodeFromCassandra(rawValues);

        } catch (Exception e) {
            throw new AchillesException(String.format("Cannot retrieve list property '%s' for entity class '%S' from CQL Row", cql3ColumnName, entityClassName), e);

        }
    }

    private Object invokeOnRowForSet(Row row) {
        final String cql3ColumnName = meta.getCQL3ColumnName();
        final String entityClassName = meta.getEntityClassName();
        log.trace("Extract set property {} from CQL3 row for entity class {}", cql3ColumnName, entityClassName);
        try {
            Set<?> rawValues = row.getSet(cql3ColumnName, toCompatibleJavaType(meta.getCql3ValueClass()));
            return meta.forTranscoding().decodeFromCassandra(rawValues);

        } catch (Exception e) {
            throw new AchillesException(String.format("Cannot retrieve set property '%s' for entity class '%S' from CQL Row", cql3ColumnName, entityClassName), e);
        }
    }

    private Object invokeOnRowForMap(Row row) {
        final String cql3ColumnName = meta.getCQL3ColumnName();
        final String entityClassName = meta.getEntityClassName();
        log.trace("Extract map property {} from CQL3 row for entity class {}", cql3ColumnName, entityClassName);
        try {
            Map<?, ?> rawValues = row.getMap(cql3ColumnName, toCompatibleJavaType(meta.getCql3KeyClass()),toCompatibleJavaType(meta.getCql3ValueClass()));
            return meta.forTranscoding().decodeFromCassandra(rawValues);

        } catch (Exception e) {
            throw new AchillesException(String.format("Cannot retrieve map property '%s' for entity class '%S' from CQL Row", cql3ColumnName, entityClassName), e);
        }
    }

}
