/*
 * Copyright (C) 2012-2014 DuyHai DOAN
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package info.archinnov.achilles.internal.metadata.parsing.context;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import com.fasterxml.jackson.databind.ObjectMapper;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.metadata.parsing.EntityIntrospector;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.NamingStrategy;
import info.archinnov.achilles.type.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropertyParsingContext {
    private static final Logger log = LoggerFactory.getLogger(PropertyParsingContext.class);
    private EntityIntrospector introspector = EntityIntrospector.Singleton.INSTANCE.get();
    private EntityParsingContext context;
    private Field currentField;
    private String currentPropertyName;
    private String currentCQL3ColumnName;
    private boolean isCustomConsistencyLevels;
    private boolean primaryKey = false;
    private boolean embeddedId = false;

    public PropertyParsingContext(EntityParsingContext context, Field currentField) {
        this.context = context;
        this.currentField = currentField;
        this.currentPropertyName = currentField.getName();
        this.currentCQL3ColumnName = introspector.inferCQLColumnName(currentField, context.getNamingStrategy());
    }

    public ObjectMapper getCurrentObjectMapper() {
        return context.getCurrentObjectMapper();
    }

    public Map<String, PropertyMeta> getPropertyMetas() {
        return context.getPropertyMetas();
    }

    @SuppressWarnings("unchecked")
    public <T> Class<T> getCurrentEntityClass() {
        return (Class<T>) context.getCurrentEntityClass();
    }

    public Field getCurrentField() {
        return currentField;
    }

    public String getCurrentPropertyName() {
        return currentPropertyName;
    }

    public String getCurrentCQL3ColumnName() {
        return currentCQL3ColumnName;
    }

    public List<PropertyMeta> getCounterMetas() {
        return context.getCounterMetas();
    }

    public Pair<ConsistencyLevel, ConsistencyLevel> getCurrentConsistencyLevels() {
        return context.getCurrentConsistencyLevels();
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

    public boolean isEmbeddedId() {
        return embeddedId;
    }

    public void setEmbeddedId(boolean embeddedId) {
        if (embeddedId) {
            this.primaryKey = true;
        }
        this.embeddedId = embeddedId;
    }

    public NamingStrategy getClassNamingStrategy() {
        return context.getNamingStrategy();
    }



    public PropertyParsingContext duplicateForField(Field field) {
        return new PropertyParsingContext(context.duplicateForClass(field.getDeclaringClass()), field);
    }
}
