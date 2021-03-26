/*
 * Copyright (C) 2012-2021 DuyHai DOAN
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package info.archinnov.achilles.internals.runtime;

import static info.archinnov.achilles.internals.schema.SchemaCreator.generateSchemaAtRuntime;
import static info.archinnov.achilles.internals.schema.SchemaCreator.generateUDTAtRuntime;
import static java.lang.String.format;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.*;
import com.datastax.driver.extras.codecs.arrays.DoubleArrayCodec;
import com.datastax.driver.extras.codecs.arrays.FloatArrayCodec;
import com.datastax.driver.extras.codecs.arrays.IntArrayCodec;
import com.datastax.driver.extras.codecs.arrays.LongArrayCodec;
import com.datastax.driver.extras.codecs.jdk8.InstantCodec;
import com.datastax.driver.extras.codecs.jdk8.LocalDateCodec;
import com.datastax.driver.extras.codecs.jdk8.LocalTimeCodec;
import com.datastax.driver.extras.codecs.jdk8.ZonedDateTimeCodec;

import info.archinnov.achilles.internals.cassandra_version.InternalCassandraVersion;
import info.archinnov.achilles.internals.context.ConfigurationContext;
import info.archinnov.achilles.internals.factory.TupleTypeFactory;
import info.archinnov.achilles.internals.factory.UserTypeFactory;
import info.archinnov.achilles.internals.metamodel.AbstractEntityProperty;
import info.archinnov.achilles.internals.metamodel.AbstractUDTClassProperty;
import info.archinnov.achilles.internals.metamodel.AbstractViewProperty;
import info.archinnov.achilles.internals.metamodel.functions.FunctionProperty;
import info.archinnov.achilles.internals.utils.CodecRegistryHelper;

public abstract class AbstractManagerFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractManagerFactory.class);

    protected final Cluster cluster;
    protected final ConfigurationContext configContext;
    protected final RuntimeEngine rte;

    protected List<AbstractEntityProperty<?>> entityProperties;
    protected List<Class<?>> entityClasses;
    protected List<FunctionProperty> functionProperties;

    public AbstractManagerFactory(Cluster cluster, ConfigurationContext configContext) {
        this.cluster = cluster;
        this.configContext = configContext;
        this.rte = new RuntimeEngine(configContext);
    }

    protected abstract InternalCassandraVersion getCassandraVersion();
    protected abstract List<AbstractUDTClassProperty<?>> getUdtClassProperties();

    /**
     * Provide the statically computed table name with keyspace (if defined) for a given entity class
     *
     * @param entityClass given entity class
     * @return statically computed table name with keyspace (if define)
     */
    public Optional<String> staticTableNameFor(Class<?> entityClass) {

        final Optional<String> tableName = entityProperties
                .stream()
                .filter(x -> x.entityClass.equals(entityClass))
                .map(x -> x.getKeyspace().map(ks -> ks + "." + x.getTableOrViewName()).orElseGet(x::getTableOrViewName))
                .findFirst();

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Determining table name for entity type %s : %s",
                    entityClass.getCanonicalName(), tableName));
        }

        return tableName;
    }

    /**
     * Shutdown the manager factory and the related session and executor service (if they are created by Achilles).
     * If the Java driver Session object and/or the executor service were provided as bootstrap parameter, Achilles
     * will <strong>NOT</strong> shut them down. This should be handled externally
     */
    @PreDestroy
    public void shutDown() {
        LOGGER.info("Calling shutdown on ManagerFactory");

        if (!configContext.isProvidedSession()) {
            LOGGER.info(format("Closing built Session object %s", rte.session));
            rte.session.close();
        }
        if (!configContext.isProvidedExecutorService()) {
            LOGGER.info(format("Closing built executor service (thread pool) %s", configContext.getExecutorService()));
            configContext.getExecutorService().shutdown();
        }
    }

    protected void bootstrap() {
        addNativeCodecs();
        injectDependencies();
        if (configContext.isForceSchemaGeneration()) {
            createSchema();
        }
        if (configContext.isValidateSchema()) {
            validateSchema();
        }
        prepareStaticStatements();
    }

    protected void addNativeCodecs() {
        LOGGER.trace("Add Java Driver extra codecs");
        final Configuration configuration = cluster.getConfiguration();
        final TupleType zonedDateTimeType = TupleType.of(configuration.getProtocolOptions().getProtocolVersion(), configuration.getCodecRegistry(),
                DataType.timestamp(), DataType.varchar());

        final CodecRegistry codecRegistry = configuration.getCodecRegistry();
        final CodecRegistryHelper codecRegistryHelper = new CodecRegistryHelper(codecRegistry);

        if (!codecRegistryHelper.hasCodecFor(DataType.list(DataType.cdouble()), double[].class)) {
            codecRegistry.register(DoubleArrayCodec.instance);
        }

        if (!codecRegistryHelper.hasCodecFor(DataType.list(DataType.cfloat()), float[].class)) {
            codecRegistry.register(FloatArrayCodec.instance);
        }

        if (!codecRegistryHelper.hasCodecFor(DataType.list(DataType.cint()), int[].class)) {
            codecRegistry.register(IntArrayCodec.instance);
        }

        if (!codecRegistryHelper.hasCodecFor(DataType.list(DataType.bigint()), long[].class)) {
            codecRegistry.register(LongArrayCodec.instance);
        }

        if (!codecRegistryHelper.hasCodecFor(DataType.timestamp(), java.time.Instant.class)) {
            codecRegistry.register(InstantCodec.instance);
        }

        if (!codecRegistryHelper.hasCodecFor(DataType.date(), java.time.LocalDate.class)) {
            codecRegistry.register(LocalDateCodec.instance);
        }

        if (!codecRegistryHelper.hasCodecFor(DataType.time(), java.time.LocalTime.class)) {
            codecRegistry.register(LocalTimeCodec.instance);
        }

        if (!codecRegistryHelper.hasCodecFor(zonedDateTimeType, java.time.ZonedDateTime.class)) {
            codecRegistry.register(new ZonedDateTimeCodec(zonedDateTimeType));
        }
    }

    protected void injectDependencies() {
        final CodecRegistry codecRegistry = cluster.getConfiguration().getCodecRegistry();
        final ProtocolVersion protocolVersion = cluster.getConfiguration().getProtocolOptions().getProtocolVersion();
        final TupleTypeFactory tupleTypeFactory = new TupleTypeFactory(protocolVersion, codecRegistry);
        final UserTypeFactory userTypeFactory = new UserTypeFactory(protocolVersion, codecRegistry);
        rte.tupleTypeFactory = tupleTypeFactory;
        rte.userTypeFactory = userTypeFactory;
        final List<Class<?>> manageEntities = configContext.getManageEntities().isEmpty() ? entityClasses : configContext.getManageEntities();
        entityProperties
                .stream()
                .filter(x -> manageEntities.contains(x.entityClass))
                .forEach(x -> configContext.injectDependencies(tupleTypeFactory, userTypeFactory, x));
    }

    protected void validateSchema() {
        final List<Class<?>> manageEntities = configContext.getManageEntities().isEmpty() ? entityClasses : configContext.getManageEntities();
        entityProperties
                .stream()
                .filter(x -> manageEntities.contains(x.entityClass))
                .forEach(x -> x.validateSchema(configContext));

        functionProperties
                .stream()
                .forEach(x -> x.validate(configContext));
    }


    protected void createSchema() {
        final Session session = configContext.getSession();
        final List<Class<?>> manageEntities = configContext.getManageEntities().isEmpty() ? entityClasses : configContext.getManageEntities();
        for (AbstractUDTClassProperty<?> x : getUdtClassProperties()) {
            final long udtCountForClass = entityProperties
                    .stream()
                    .filter(entityProperty -> manageEntities.contains(entityProperty.entityClass))
                    .flatMap(entityProperty -> entityProperty.allColumns.stream())
                    .filter(property -> property.containsUDTProperty())
                    .filter(property -> property.getUDTClassProperties().contains(x))
                    .count();

            if(udtCountForClass>0)
                generateUDTAtRuntime(session, x);
        }


        final long viewCount = entityProperties
                .stream()
                .filter(AbstractEntityProperty::isView)
                .count();

        //Inject base table property into view property
        if (viewCount > 0) {
            final Map<Class<?>, AbstractEntityProperty<?>> entityPropertiesMap = entityProperties
                    .stream()
                    .filter(AbstractEntityProperty::isTable)
                    .collect(Collectors.toMap(x -> x.entityClass, x -> x));

            entityProperties
                    .stream()
                    .filter(AbstractEntityProperty::isView)
                    .map(x -> (AbstractViewProperty<?>)x)
                    .forEach(x -> x.setBaseClassProperty(entityPropertiesMap.get(x.getBaseEntityClass())));
        }

        entityProperties
                .stream()
                .filter(x -> manageEntities.contains(x.entityClass))
                .forEach(x -> generateSchemaAtRuntime(session, x));


    }

    protected void prepareStaticStatements() {
        final List<Class<?>> manageEntities = configContext.getManageEntities().isEmpty() ? entityClasses : configContext.getManageEntities();
        entityProperties
                .stream()
                .filter(x -> manageEntities.contains(x.entityClass))
                .forEach(x -> x.prepareStaticStatements(getCassandraVersion(), configContext.getSession(), rte.cache));
    }


}
