package info.archinnov.achilles.internal.metadata.holder;

import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;
import com.google.common.base.Optional;
import info.archinnov.achilles.internal.utils.Pair;
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
        if (meta.structure().isCompoundPK()) {
            if (onlyStaticColumns) {
                for (PropertyMeta partitionComponents : meta.getCompoundPKProperties().getPartitionComponents().propertyMetas) {
                    final String cqlColumnName = partitionComponents.getCQLColumnName();
                    insert.value(cqlColumnName, bindMarker(cqlColumnName));
                }
            } else {
                for (String component : meta.getCompoundPKProperties().getCQLComponentNames()) {
                    insert.value(component, bindMarker(component));
                }
            }
            return insert;
        } else {
            return insert.value(meta.getCQLColumnName(), bindMarker(meta.getCQLColumnName()));
        }
    }

    public RegularStatement generateWhereClauseForSelect(Optional<PropertyMeta> pmO, Select from) {
        log.debug("Generate SELECT WHERE clause for property meta {} with static column ? {}", meta, pmO.isPresent());

        if (meta.structure().isCompoundPK()) {
            return generateWhereClauseForSelectWithCompound(pmO, from);
        } else {
            return from.where(eq(meta.getCQLColumnName(), bindMarker(meta.getCQLColumnName())));
        }
    }

    private Select.Where generateWhereClauseForSelectWithCompound(Optional<PropertyMeta> pmO, Select from) {
        log.debug("Generate SELECT WHERE clause with compound primary key for property meta {} with static column ? {}", meta, pmO.isPresent());

        Select.Where where = null;
        int i = 0;
        List<String> componentNames;
        if (pmO.isPresent() && pmO.get().structure().isStaticColumn()) {
            componentNames =  meta.getCompoundPKProperties().getPartitionComponents().getCQLComponentNames();
        } else {
            componentNames = meta.getCompoundPKProperties().getCQLComponentNames();
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
        if (meta.structure().isCompoundPK()) {
            return generateWhereClauseForDeleteWithCompound(onlyStaticColumns, mainFrom);
        } else {
            return mainFrom.where(eq(meta.getCQLColumnName(), bindMarker(meta.getCQLColumnName())));
        }
    }

    private Delete.Where generateWhereClauseForDeleteWithCompound(boolean onlyStaticColumns, Delete mainFrom) {
        log.debug("Generate DELETE WHERE clause with compound primary key for property meta {} with static column ? {}", meta, onlyStaticColumns);
        Delete.Where where = null;
        List<String> componentNames;
        if (onlyStaticColumns) {
            componentNames = meta.getCompoundPKProperties().getPartitionComponents().getCQLComponentNames();
        } else {
            componentNames = meta.getCompoundPKProperties().getCQLComponentNames();
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
        String cqlColumnName = meta.getCQLColumnName();
        return updateConditions.with(set(cqlColumnName, bindMarker(cqlColumnName)));
    }

    public Update.Assignments prepareUpdateField(Update.Assignments assignments) {
        log.debug("Prepare UPDATE clause for property meta {} ", meta);
        String cqlColumnName = meta.getCQLColumnName();
        return assignments.and(set(cqlColumnName, bindMarker(cqlColumnName)));
    }

    public Update.Where prepareCommonWhereClauseForUpdate(Update.Assignments assignments, boolean onlyStaticColumns) {
        log.debug("Prepare common UPDATE WHERE clause for property meta {} with static column ? {}", meta, onlyStaticColumns);
        if (meta.structure().isCompoundPK()) {
            return prepareCommonWhereClauseForUpdateWithCompound(assignments, onlyStaticColumns);
        } else {
            String cqlColumnName = meta.getCQLColumnName();
            return assignments.where(eq(cqlColumnName, bindMarker(cqlColumnName)));
        }
    }

    private Update.Where prepareCommonWhereClauseForUpdateWithCompound(Update.Assignments assignments, boolean onlyStaticColumns) {
        log.debug("Prepare common UPDATE WHERE clause with compound primary key for property meta {} with static column ? {}", meta, onlyStaticColumns);
        Update.Where where = null;
        int i = 0;
        if (onlyStaticColumns) {
            for (String partitionKeys : meta.getCompoundPKProperties().getPartitionComponents().getCQLComponentNames()) {
                if (i++ == 0) {
                    where = assignments.where(eq(partitionKeys, bindMarker(partitionKeys)));
                } else {
                    where.and(eq(partitionKeys, bindMarker(partitionKeys)));
                }
            }
        } else {
            for (String clusteredId : meta.getCompoundPKProperties().getCQLComponentNames()) {
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
        if (meta.structure().isCompoundPK()) {
            return generateWhereClauseForUpdateWithCompound(primaryKey, pm, update);
        } else {
            Object id = meta.forTranscoding().encodeToCassandra(primaryKey);
            Update.Where where = update.where(eq(meta.getCQLColumnName(), id));
            Object[] boundValues = new Object[] { id };
            return Pair.create(where, boundValues);
        }
    }

    private Pair<Update.Where, Object[]> generateWhereClauseForUpdateWithCompound(Object primaryKey, PropertyMeta pm, Update.Assignments update) {
        log.debug("Generate plain UPDATE WHERE clause with compound primary key for property meta {}", pm);
        List<String> componentNames = meta.getCompoundPKProperties().getCQLComponentNames();
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
        if (meta.structure().isCompoundPK()) {
            for (String component : meta.getCompoundPKProperties().getCQLComponentNames()) {
                select = select.column(component);
            }
            return select;
        } else {
            return select.column(meta.getCQLColumnName());
        }
    }


    // DirtyCheckChangeSet
    public Update.Assignments generateUpdateForRemoveAll(Update.Conditions conditions) {
        log.debug("Generate UPDATE for changing collection for property meta {}", meta);
        String cqlColumnName = meta.getCQLColumnName();
        return conditions.with(set(cqlColumnName, bindMarker(cqlColumnName)));
    }

    public Update.Assignments generateUpdateForAddedElements(Update.Conditions conditions) {
        log.debug("Generate UPDATE for adding elements to collection for property meta {}", meta);
        String cqlColumnName = meta.getCQLColumnName();
        return conditions.with(addAll(cqlColumnName, bindMarker(cqlColumnName)));
    }

    public Update.Assignments generateUpdateForRemovedElements(Update.Conditions conditions) {
        log.debug("Generate UPDATE for removing all elements from collection for property meta {}", meta);
        String cqlColumnName = meta.getCQLColumnName();
        return conditions.with(removeAll(cqlColumnName, bindMarker(cqlColumnName)));
    }

    public Update.Assignments generateUpdateForAppendedElements(Update.Conditions conditions) {
        log.debug("Generate UPDATE for appending elements to collection for property meta {}", meta);
        String cqlColumnName = meta.getCQLColumnName();
        return conditions.with(appendAll(cqlColumnName, bindMarker(cqlColumnName)));
    }

    public Update.Assignments generateUpdateForPrependedElements(Update.Conditions conditions) {
        log.debug("Generate UPDATE for prepending elements to collection for property meta {}", meta);
        String cqlColumnName = meta.getCQLColumnName();
        return conditions.with(prependAll(cqlColumnName, bindMarker(cqlColumnName)));
    }

    public Update.Assignments generateUpdateForRemoveListElements(Update.Conditions conditions) {
        log.debug("Generate UPDATE for discarding all elements from list for property meta {}", meta);
        String cqlColumnName = meta.getCQLColumnName();
        return conditions.with(discardAll(cqlColumnName, bindMarker(cqlColumnName)));
    }

    public Update.Assignments generateUpdateForAddedEntries(Update.Conditions conditions) {
        log.debug("Generate UPDATE for adding entries to map for property meta {}", meta);
        String cqlColumnName = meta.getCQLColumnName();
        return conditions.with(putAll(cqlColumnName, bindMarker(cqlColumnName)));
    }

    public Update.Assignments generateUpdateForRemovedKey(Update.Conditions conditions) {
        log.debug("Generate UPDATE for removing from map by key for property meta {}", meta);
        String cqlColumnName = meta.getCQLColumnName();
        return conditions.with(put(cqlColumnName, bindMarker("key"), bindMarker("nullValue")));
    }

    public Update.Assignments generateUpdateForSetAtIndexElement(Update.Conditions conditions, int index, Object encoded) {
        log.debug("Generate UPDATE for setting list element at index for property meta {}", meta);
        String cqlColumnName = meta.getCQLColumnName();
        return conditions.with(setIdx(cqlColumnName, index, encoded));
    }

    public Update.Assignments generateUpdateForRemovedAtIndexElement(Update.Conditions conditions, int index) {
        log.debug("Generate UPDATE for removing list element at index for property meta {}", meta);
        String cqlColumnName = meta.getCQLColumnName();
        return conditions.with(setIdx(cqlColumnName, index, null));
    }
}
