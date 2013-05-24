package info.archinnov.achilles.entity.parsing.context;

import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Pair;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * AchillesPropertyParsingContext
 * 
 * @author DuyHai DOAN
 * 
 */
public class AchillesPropertyParsingContext {
    private AchillesEntityParsingContext context;
    private Field currentField;
    private String currentPropertyName;
    private String currentExternalTableName;
    private boolean joinColumn = false;
    private boolean isCustomConsistencyLevels;
    private boolean primaryKey = false;
    private boolean multiKeyPrimaryKey = false;

    public AchillesPropertyParsingContext(AchillesEntityParsingContext context, //
            Field currentField) {
        this.context = context;
        this.currentField = currentField;
    }

    public ObjectMapper getCurrentObjectMapper() {
        return context.getCurrentObjectMapper();
    }

    public Map<PropertyMeta<?, ?>, String> getWideMaps() {
        return context.getWideMaps();
    }

    public Map<PropertyMeta<?, ?>, String> getJoinWideMaps() {
        return context.getJoinWideMaps();
    }

    public Map<String, PropertyMeta<?, ?>> getPropertyMetas() {
        return context.getPropertyMetas();
    }

    public Class<?> getCurrentEntityClass() {
        return context.getCurrentEntityClass();
    }

    public boolean isColumnFamilyDirectMapping() {
        return context.isWideRow();
    }

    public Field getCurrentField() {
        return currentField;
    }

    public boolean isJoinColumn() {
        return joinColumn;
    }

    public void setJoinColumn(boolean joinColumn) {
        this.joinColumn = joinColumn;
    }

    public String getCurrentPropertyName() {
        return currentPropertyName;
    }

    public void setCurrentPropertyName(String currentPropertyName) {
        this.currentPropertyName = currentPropertyName;
    }

    public String getCurrentExternalTableName() {
        return currentExternalTableName;
    }

    public void setCurrentExternalTableName(String currentExternalTableName) {
        this.currentExternalTableName = currentExternalTableName;
    }

    public List<PropertyMeta<?, ?>> getCounterMetas() {
        return context.getCounterMetas();
    }

    public Pair<ConsistencyLevel, ConsistencyLevel> getCurrentConsistencyLevels() {
        return context.getCurrentConsistencyLevels();
    }

    public AchillesConsistencyLevelPolicy getConfigurableCLPolicy() {
        return context.getConfigurableCLPolicy();
    }

    public Map<PropertyMeta<?, ?>, Class<?>> getJoinPropertyMetaToBeFilled() {
        return context.getJoinPropertyMetaToBeFilled();
    }

    public String getCurrentColumnFamilyName() {
        return context.getCurrentColumnFamilyName();
    }

    public boolean isExternal() {
        return !StringUtils.isBlank(currentExternalTableName);
    }

    public boolean isCustomConsistencyLevels() {
        return isCustomConsistencyLevels;
    }

    public void setCustomConsistencyLevels(boolean isCustomConsistencyLevels) {
        this.isCustomConsistencyLevels = isCustomConsistencyLevels;
    }

    public void hasSimpleCounterType() {
        context.setHasSimpleCounter(true);
    }

    public boolean isPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(boolean primaryKey) {
        this.primaryKey = primaryKey;
    }

    public boolean isMultiKeyPrimaryKey() {
        return multiKeyPrimaryKey;
    }

    public void hasMultiKeyPrimaryKey(boolean multiKeyPrimaryKey) {
        if (multiKeyPrimaryKey) {
            this.primaryKey = true;
        }
        this.multiKeyPrimaryKey = multiKeyPrimaryKey;
    }
}
