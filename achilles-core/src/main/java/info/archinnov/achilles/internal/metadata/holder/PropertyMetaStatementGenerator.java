package info.archinnov.achilles.internal.metadata.holder;

import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;
import com.google.common.base.Optional;
import info.archinnov.achilles.type.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

public class PropertyMetaStatementGenerator extends PropertyMetaView{

    private static final Logger log = LoggerFactory.getLogger(PropertyMetaStatementGenerator.class);

    PropertyMetaStatementGenerator(PropertyMeta meta) {
        super(meta);
    }


    public Insert generateInsertPrimaryKey(Insert insert, boolean onlyStaticColumns) {
        log.debug("Generate INSERT primary key for id meta {}", meta);
        if (meta.structure().isEmbeddedId()) {
            if (onlyStaticColumns) {
                for (PropertyMeta partitionComponents : meta.getEmbeddedIdProperties().getPartitionComponents().propertyMetas) {
                    final String cql3ColumnName = partitionComponents.getCQL3ColumnName();
                    insert.value(cql3ColumnName, bindMarker(cql3ColumnName));
                }
            } else {
                for (String component : meta.getEmbeddedIdProperties().getCQL3ComponentNames()) {
                    insert.value(component, bindMarker(component));
                }
            }
            return insert;
        } else {
            return insert.value(meta.getCQL3ColumnName(), bindMarker(meta.getCQL3ColumnName()));
        }
    }

    public RegularStatement generateWhereClauseForSelect(Optional<PropertyMeta> pmO, Select from) {
        log.debug("Generate SELECT WHERE clause for property meta {} with static column ? {}", meta, pmO.isPresent());

        if (meta.structure().isEmbeddedId()) {
            return generateWhereClauseForSelectWithCompound(pmO, from);
        } else {
            return from.where(eq(meta.getCQL3ColumnName(), bindMarker(meta.getCQL3ColumnName())));
        }
    }

    private Select.Where generateWhereClauseForSelectWithCompound(Optional<PropertyMeta> pmO, Select from) {
        log.debug("Generate SELECT WHERE clause with compound primary key for property meta {} with static column ? {}", meta, pmO.isPresent());

        Select.Where where = null;
        int i = 0;
        List<String> componentNames;
        if (pmO.isPresent() && pmO.get().structure().isStaticColumn()) {
            componentNames =  meta.getEmbeddedIdProperties().getPartitionComponents().getCQL3ComponentNames();
        } else {
            componentNames = meta.getEmbeddedIdProperties().getCQL3ComponentNames();
        }
        for (String partitionKey : componentNames) {
            if (i++ == 0) {
                where = from.where(eq(partitionKey, bindMarker(partitionKey)));
            } else {
                where.and(eq(partitionKey, bindMarker(partitionKey)));
            }
        }
        return where;
    }

    public RegularStatement generateWhereClauseForDelete(boolean onlyStaticColumns, Delete mainFrom) {
        log.debug("Generate DELETE WHERE clause for property meta {} with static column ? {}", meta, onlyStaticColumns);
        if (meta.structure().isEmbeddedId()) {
            return generateWhereClauseForDeleteWithCompound(onlyStaticColumns, mainFrom);
        } else {
            return mainFrom.where(eq(meta.getCQL3ColumnName(), bindMarker(meta.getCQL3ColumnName())));
        }
    }

    private Delete.Where generateWhereClauseForDeleteWithCompound(boolean onlyStaticColumns, Delete mainFrom) {
        log.debug("Generate DELETE WHERE clause with compound primary key for property meta {} with static column ? {}", meta, onlyStaticColumns);
        Delete.Where where = null;
        List<String> componentNames;
        if (onlyStaticColumns) {
            componentNames = meta.getEmbeddedIdProperties().getPartitionComponents().getCQL3ComponentNames();
        } else {
            componentNames = meta.getEmbeddedIdProperties().getCQL3ComponentNames();
        }

        int i = 0;
        for (String clusteredId : componentNames) {
            if (i ++== 0) {
                where = mainFrom.where(eq(clusteredId, bindMarker(clusteredId)));
            } else {
                where.and(eq(clusteredId, bindMarker(clusteredId)));
            }
        }
        return where;
    }

    public Update.Assignments prepareUpdateField(Update.Conditions updateConditions) {
        log.debug("Prepare UPDATE clause for property meta {} ", meta);
        String cql3ColumnName = meta.getCQL3ColumnName();
        return updateConditions.with(set(cql3ColumnName, bindMarker(cql3ColumnName)));
    }

    public Update.Assignments prepareUpdateField(Update.Assignments assignments) {
        log.debug("Prepare UPDATE clause for property meta {} ", meta);
        String cql3ColumnName = meta.getCQL3ColumnName();
        return assignments.and(set(cql3ColumnName, bindMarker(cql3ColumnName)));
    }

    public Update.Where prepareCommonWhereClauseForUpdate(Update.Assignments assignments, boolean onlyStaticColumns) {
        log.debug("Prepare common UPDATE WHERE clause for property meta {} with static column ? {}", meta, onlyStaticColumns);
        if (meta.structure().isEmbeddedId()) {
            return prepareCommonWhereClauseForUpdateWithCompound(assignments, onlyStaticColumns);
        } else {
            String cql3ColumnName = meta.getCQL3ColumnName();
            return assignments.where(eq(cql3ColumnName, bindMarker(cql3ColumnName)));
        }
    }

    private Update.Where prepareCommonWhereClauseForUpdateWithCompound(Update.Assignments assignments, boolean onlyStaticColumns) {
        log.debug("Prepare common UPDATE WHERE clause with compound primary key for property meta {} with static column ? {}", meta, onlyStaticColumns);
        Update.Where where = null;
        int i = 0;
        if (onlyStaticColumns) {
            for (String partitionKeys : meta.getEmbeddedIdProperties().getPartitionComponents().getCQL3ComponentNames()) {
                if (i++ == 0) {
                    where = assignments.where(eq(partitionKeys, bindMarker(partitionKeys)));
                } else {
                    where.and(eq(partitionKeys, bindMarker(partitionKeys)));
                }
            }
        } else {
            for (String clusteredId : meta.getEmbeddedIdProperties().getCQL3ComponentNames()) {
                if (i++ == 0) {
                    where = assignments.where(eq(clusteredId, bindMarker(clusteredId)));
                } else {
                    where.and(eq(clusteredId, bindMarker(clusteredId)));
                }
            }
        }
        return where;
    }

    public Pair<Update.Where, Object[]> generateWhereClauseForUpdate(Object entity, PropertyMeta pm, Update.Assignments update) {
        log.debug("Generate plain UPDATE WHERE clause for property meta {}", pm);
        Object primaryKey = meta.forValues().getPrimaryKey(entity);
        if (meta.structure().isEmbeddedId()) {
            return generateWhereClauseForUpdateWithCompound(primaryKey, pm, update);
        } else {
            Object id = meta.forTranscoding().encodeToCassandra(primaryKey);
            Update.Where where = update.where(eq(meta.getCQL3ColumnName(), id));
            Object[] boundValues = new Object[] { id };
            return Pair.create(where, boundValues);
        }
    }

    private Pair<Update.Where, Object[]> generateWhereClauseForUpdateWithCompound(Object primaryKey, PropertyMeta pm, Update.Assignments update) {
        log.debug("Generate plain UPDATE WHERE clause with compound primary key for property meta {}", pm);
        List<String> componentNames = meta.getEmbeddedIdProperties().getCQL3ComponentNames();
        List<Object> encodedComponents = meta.forTranscoding().encodeToComponents(primaryKey, pm.structure().isStaticColumn());
        Object[] boundValues = new Object[encodedComponents.size()];
        Update.Where where = null;
        for (int i = 0; i < encodedComponents.size(); i++) {
            String componentName = componentNames.get(i);
            Object componentValue = encodedComponents.get(i);
            if (i == 0) {
                where = update.where(eq(componentName, componentValue));
            } else {
                where.and(eq(componentName, componentValue));
            }
            boundValues[i] = componentValue;
        }
        return Pair.create(where, boundValues);
    }

    public Select.Selection prepareSelectField(Select.Selection select) {
        log.debug("Prepare SELECT clause for property meta {}", meta);
        if (meta.structure().isEmbeddedId()) {
            for (String component : meta.getEmbeddedIdProperties().getCQL3ComponentNames()) {
                select = select.column(component);
            }
            return select;
        } else {
            return select.column(meta.getCQL3ColumnName());
        }
    }


    // DirtyCheckChangeSet
    public Update.Assignments generateUpdateForRemoveAll(Update.Conditions conditions) {
        log.debug("Generate UPDATE for changing collection for property meta {}", meta);
        String cql3ColumnName = meta.getCQL3ColumnName();
        return conditions.with(set(cql3ColumnName, bindMarker(cql3ColumnName)));
    }

    public Update.Assignments generateUpdateForAddedElements(Update.Conditions conditions) {
        log.debug("Generate UPDATE for adding elements to collection for property meta {}", meta);
        String cql3ColumnName = meta.getCQL3ColumnName();
        return conditions.with(addAll(cql3ColumnName, bindMarker(cql3ColumnName)));
    }

    public Update.Assignments generateUpdateForRemovedElements(Update.Conditions conditions) {
        log.debug("Generate UPDATE for removing all elements from collection for property meta {}", meta);
        String cql3ColumnName = meta.getCQL3ColumnName();
        return conditions.with(removeAll(cql3ColumnName, bindMarker(cql3ColumnName)));
    }

    public Update.Assignments generateUpdateForAppendedElements(Update.Conditions conditions) {
        log.debug("Generate UPDATE for appending elements to collection for property meta {}", meta);
        String cql3ColumnName = meta.getCQL3ColumnName();
        return conditions.with(appendAll(cql3ColumnName, bindMarker(cql3ColumnName)));
    }

    public Update.Assignments generateUpdateForPrependedElements(Update.Conditions conditions) {
        log.debug("Generate UPDATE for prepending elements to collection for property meta {}", meta);
        String cql3ColumnName = meta.getCQL3ColumnName();
        return conditions.with(prependAll(cql3ColumnName, bindMarker(cql3ColumnName)));
    }

    public Update.Assignments generateUpdateForRemoveListElements(Update.Conditions conditions) {
        log.debug("Generate UPDATE for discarding all elements from list for property meta {}", meta);
        String cql3ColumnName = meta.getCQL3ColumnName();
        return conditions.with(discardAll(cql3ColumnName, bindMarker(cql3ColumnName)));
    }

    public Update.Assignments generateUpdateForAddedEntries(Update.Conditions conditions) {
        log.debug("Generate UPDATE for adding entries to map for property meta {}", meta);
        String cql3ColumnName = meta.getCQL3ColumnName();
        return conditions.with(putAll(cql3ColumnName, bindMarker(cql3ColumnName)));
    }

    public Update.Assignments generateUpdateForRemovedKey(Update.Conditions conditions) {
        log.debug("Generate UPDATE for removing from map by key for property meta {}", meta);
        String cql3ColumnName = meta.getCQL3ColumnName();
        return conditions.with(put(cql3ColumnName, bindMarker("key"), bindMarker("nullValue")));
    }

    public Update.Assignments generateUpdateForSetAtIndexElement(Update.Conditions conditions, int index, Object encoded) {
        log.debug("Generate UPDATE for setting list element at index for property meta {}", meta);
        String cql3ColumnName = meta.getCQL3ColumnName();
        return conditions.with(setIdx(cql3ColumnName, index, encoded));
    }

    public Update.Assignments generateUpdateForRemovedAtIndexElement(Update.Conditions conditions, int index) {
        log.debug("Generate UPDATE for removing list element at index for property meta {}", meta);
        String cql3ColumnName = meta.getCQL3ColumnName();
        return conditions.with(setIdx(cql3ColumnName, index, null));
    }
}
