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

package info.archinnov.achilles.internal.metadata.discovery;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import info.archinnov.achilles.internal.metadata.holder.EntityMetaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import info.archinnov.achilles.interceptor.Interceptor;
import info.archinnov.achilles.internal.context.ConfigurationContext;
import info.archinnov.achilles.internal.context.DaoContext;
import info.archinnov.achilles.internal.context.DaoContextFactory;
import info.archinnov.achilles.internal.context.SchemaContext;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.parsing.EntityParser;
import info.archinnov.achilles.internal.metadata.parsing.PropertyParser;
import info.archinnov.achilles.internal.metadata.parsing.context.EntityParsingContext;
import info.archinnov.achilles.internal.metadata.parsing.context.ParsingResult;
import info.archinnov.achilles.internal.validation.Validator;

public class AchillesBootstrapper {
    private static final Logger log = LoggerFactory.getLogger(AchillesBootstrapper.class);

    private EntityParser entityParser = EntityParser.Singleton.INSTANCE.get();
    private PropertyParser propertyParser = PropertyParser.Singleton.INSTANCE.get();

    private DaoContextFactory daoContextFactory = new DaoContextFactory();

    public ParsingResult buildMetaDatas(ConfigurationContext configContext, List<Class<?>> entities) {
        log.debug("Build meta data for candidate entities");
        Map<Class<?>, EntityMeta> entityMetaMap = new HashMap<>();
        boolean hasSimpleCounter = false;
        for (Class<?> entityClass : entities) {
            EntityParsingContext context = new EntityParsingContext(configContext, entityClass);
            EntityMeta entityMeta = entityParser.parseEntity(context);
            entityMetaMap.put(entityClass, entityMeta);

            hasSimpleCounter = hasSimpleCounter || (context.hasSimpleCounter() && !entityMeta.structure().isClusteredCounter());
            boolean shouldValidateBean = configContext.isClassConstrained(entityClass);
            if (shouldValidateBean) {
                configContext.addBeanValidationInterceptor(entityMeta);
            }
        }
        return new ParsingResult(entityMetaMap, hasSimpleCounter);
    }

    public void validateOrCreateTables(SchemaContext schemaContext) {
        log.debug("Start schema validation/creation");
        Map<String, TableMetadata> tableMetaDatas = schemaContext.fetchTableMetaData();

        for (Entry<Class<?>, EntityMeta> entry : schemaContext.entityMetaEntrySet()) {
            EntityMeta entityMeta = entry.getValue();
            final EntityMetaConfig metaConfig = entityMeta.config();
            String qualifiedTableName = metaConfig.getQualifiedTableName();

            if (tableMetaDatas.containsKey(qualifiedTableName)) {
                TableMetadata tableMetaData = tableMetaDatas.get(qualifiedTableName);
                schemaContext.validateForEntity(entityMeta, tableMetaData);
                schemaContext.updateForEntity(entityMeta, tableMetaData);
            } else {
                schemaContext.createTableForEntity(entry.getValue());
            }
        }

        if (schemaContext.hasSimpleCounter()) {            
            if (schemaContext.achillesCounterTableExists()) {
                schemaContext.validateAchillesCounter();
            } else {
                schemaContext.createTableForCounter();
            }
        }
    }
    
    public DaoContext buildDaoContext(Session session, ParsingResult parsingResult,
            ConfigurationContext configContext) {
        log.debug("Build DaoContext");
        return daoContextFactory.create(session, parsingResult, configContext);
    }

    public void addInterceptorsToEntityMetas(List<Interceptor<?>> interceptors, Map<Class<?>,
            EntityMeta> entityMetaMap) {
        for (Interceptor<?> interceptor : interceptors) {
            Class<?> entityClass = propertyParser.inferEntityClassFromInterceptor(interceptor);
            EntityMeta entityMeta = entityMetaMap.get(entityClass);
            Validator.validateBeanMappingTrue(entityMeta != null, "The entity class '%s' is not found",
                    entityClass.getCanonicalName());
            entityMeta.forInterception().addInterceptor(interceptor);
        }
    }
}
