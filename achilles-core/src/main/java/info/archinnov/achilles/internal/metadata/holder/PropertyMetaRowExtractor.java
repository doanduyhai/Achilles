package info.archinnov.achilles.internal.metadata.holder;

import com.datastax.driver.core.Row;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta.EntityState;
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
import static java.lang.String.format;

public class PropertyMetaRowExtractor extends PropertyMetaView{

    private static final Logger log = LoggerFactory.getLogger(PropertyMetaRowExtractor.class);

    protected PropertyMetaRowExtractor(PropertyMeta meta) {
        super(meta);
    }

    public List<Object> extractRawCompoundPrimaryComponentsFromRow(Row row, boolean hasStaticColumns) {
        log.trace("Extract raw compound primary components from CQL row {} for id meta {}", row, meta);
        Validator.validateNotNull(meta.getCompoundPKProperties(), "Cannot extract raw compound primary keys from CQL row because entity '%s' does not have a compound primary key", meta.getEntityClassName());
        final List<Class<?>> componentClasses = meta.getCompoundPKProperties().getCQLComponentClasses();
        final List<String> cqlComponentNames = meta.getCompoundPKProperties().getCQLComponentNames();
        final int partitionComponentsCount = meta.getCompoundPKProperties().getPartitionComponents().getCQLComponentNames().size();
        List<Object> rawValues = new ArrayList<>(Collections.nCopies(cqlComponentNames.size(), null));

        for (int index=0; index<partitionComponentsCount; index++) {
            Object rawValue;
            Class<?> componentClass = componentClasses.get(index);
            try {
                rawValue = getRowMethod(componentClass).invoke(row, cqlComponentNames.get(index));
            } catch (Exception e) {
                throw new AchillesException(format("Cannot retrieve partition key(s) for entity class '%s' from CQL Row", meta.getEntityClassName()), e);
            }
            rawValues.set(index, rawValue);
        }

        for (int index=partitionComponentsCount; index<cqlComponentNames.size(); index++) {
            Object rawValue;
            Class<?> componentClass = componentClasses.get(index);
            try {
                rawValue = getRowMethod(componentClass).invoke(row, cqlComponentNames.get(index));
            } catch (Exception e) {
                if (hasStaticColumns) {
                    rawValue = null;
                } else {
                    throw new AchillesException(format("Cannot retrieve compound primary key for entity class '%s' from CQL Row", meta.getEntityClassName()), e);
                }
            }
            rawValues.set(index, rawValue);
        }

        return rawValues;
    }

    public Object invokeOnRowForFields(Row row) {
        if(log.isTraceEnabled()) {
            log.trace("Extract column {} from CQL row {} for id meta {}", meta.getCQLColumnName(), row, meta);
        }

        Object value = null;
        if (!row.isNull(meta.getCQLColumnName())) {
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
                case PARTITION_KEY:
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
        log.trace("Extract compound primary key {} from CQL row for entity class {}", meta.getPropertyName(),meta.getEntityClassName());
        boolean hasSomeStaticValue = rowHasSomeStaticValue(row, entityMeta);
        final List<Object> rawComponents = extractRawCompoundPrimaryComponentsFromRow(row, hasSomeStaticValue);
        if (entityState.isManaged() ) {


            if (entityMeta.structure().hasOnlyStaticColumns() || hasSomeStaticValue) {
                // Do nothing
            } else {
                validateExtractedCompoundPrimaryComponents(rawComponents);
            }
        }
        return meta.forTranscoding().decodeFromComponents(rawComponents);

    }

    protected boolean rowHasSomeStaticValue(Row row, EntityMeta entityMeta) {
        boolean hasSomeStaticValue=false;
        final List<PropertyMeta> allStaticMetas = entityMeta.getAllStaticMetas();
        for (PropertyMeta staticMeta : allStaticMetas) {
            switch (staticMeta.type()) {
                case SIMPLE:
                    try {
                        hasSomeStaticValue = staticMeta.forRowExtraction().invokeOnRowForProperty(row) != null;
                    } catch (AchillesException e) {

                    }
                    break;
                case LIST:
                    try {
                        hasSomeStaticValue = staticMeta.forRowExtraction().invokeOnRowForList(row) != null;
                    } catch (AchillesException e) {

                    }
                    break;
                case SET:
                    try {
                        hasSomeStaticValue = staticMeta.forRowExtraction().invokeOnRowForSet(row) != null;
                    } catch (AchillesException e) {

                    }
                    break;
                case MAP:
                    try {
                        hasSomeStaticValue = staticMeta.forRowExtraction().invokeOnRowForMap(row) != null;
                    } catch (AchillesException e) {

                    }
                    break;
                default:
                    break;
            }
            if(hasSomeStaticValue) break;
        }
        return hasSomeStaticValue;
    }

    public void validateExtractedCompoundPrimaryComponents(List<Object> rawComponents) {
        log.trace("Validate raw compound primary components {} for id meta {}", rawComponents, meta);

        Validator.validateNotNull(meta.getCompoundPKProperties(), "Cannot validate raw compound primary keys from CQL row because entity '%s' does not have a compound primary key",meta.getEntityClassName());
        final List<String> cqlComponentNames = meta.getCompoundPKProperties().getCQLComponentNames();
        for (int i = 0; i < cqlComponentNames.size(); i++) {
            Validator.validateNotNull(rawComponents.get(i), "Error, the component '%s' from @CompoundPrimaryKey class '%s' is null in Cassandra", cqlComponentNames.get(i), meta.getValueClass());
        }
    }

    protected Object invokeOnRowForProperty(Row row) {
        final String cqlColumnName = meta.getCQLColumnName();
        final String entityClassName = meta.getEntityClassName();
        log.trace("Extract property {} from CQL row for entity class {}", cqlColumnName, entityClassName);
        try {
            Object rawValue = getRowMethod(meta.getCqlValueClass()).invoke(row, cqlColumnName);
            return meta.forTranscoding().decodeFromCassandra(rawValue);
        } catch (Exception e) {
            throw new AchillesException(String.format("Cannot retrieve property '%s' for entity class '%S' from CQL Row", cqlColumnName, entityClassName), e);
        }
    }

    protected Object invokeOnRowForList(Row row) {
        final String cqlColumnName = meta.getCQLColumnName();
        final String entityClassName = meta.getEntityClassName();
        log.trace("Extract list property {} from CQL row for entity class {}", cqlColumnName, entityClassName);
        try {
            List<?> rawValues = row.getList(cqlColumnName, toCompatibleJavaType(meta.getCqlValueClass()));
            return meta.forTranscoding().decodeFromCassandra(rawValues);

        } catch (Exception e) {
            throw new AchillesException(String.format("Cannot retrieve list property '%s' for entity class '%S' from CQL Row", cqlColumnName, entityClassName), e);

        }
    }

    protected Object invokeOnRowForSet(Row row) {
        final String cqlColumnName = meta.getCQLColumnName();
        final String entityClassName = meta.getEntityClassName();
        log.trace("Extract set property {} from CQL row for entity class {}", cqlColumnName, entityClassName);
        try {
            Set<?> rawValues = row.getSet(cqlColumnName, toCompatibleJavaType(meta.getCqlValueClass()));
            return meta.forTranscoding().decodeFromCassandra(rawValues);

        } catch (Exception e) {
            throw new AchillesException(String.format("Cannot retrieve set property '%s' for entity class '%S' from CQL Row", cqlColumnName, entityClassName), e);
        }
    }

    protected Object invokeOnRowForMap(Row row) {
        final String cqlColumnName = meta.getCQLColumnName();
        final String entityClassName = meta.getEntityClassName();
        log.trace("Extract map property {} from CQL row for entity class {}", cqlColumnName, entityClassName);
        try {
            Map<?, ?> rawValues = row.getMap(cqlColumnName, toCompatibleJavaType(meta.getCQLKeyClass()),toCompatibleJavaType(meta.getCqlValueClass()));
            return meta.forTranscoding().decodeFromCassandra(rawValues);

        } catch (Exception e) {
            throw new AchillesException(String.format("Cannot retrieve map property '%s' for entity class '%S' from CQL Row", cqlColumnName, entityClassName), e);
        }
    }

}
