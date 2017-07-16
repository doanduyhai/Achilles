/*
 * Copyright (C) 2012-2016 DuyHai DOAN
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

package info.archinnov.achilles.internals.metamodel;

import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.datastax.driver.core.schemabuilder.CreateType;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import info.archinnov.achilles.annotations.UDT;
import info.archinnov.achilles.internals.factory.TupleTypeFactory;
import info.archinnov.achilles.internals.factory.UserTypeFactory;
import info.archinnov.achilles.internals.injectable.InjectJacksonMapper;
import info.archinnov.achilles.internals.injectable.InjectKeyspace;
import info.archinnov.achilles.internals.injectable.InjectRuntimeCodecs;
import info.archinnov.achilles.internals.injectable.InjectSchemaStrategy;
import info.archinnov.achilles.internals.injectable.InjectUserAndTupleTypeFactory;
import info.archinnov.achilles.internals.options.CassandraOptions;
import info.archinnov.achilles.internals.schema.SchemaContext;
import info.archinnov.achilles.internals.strategy.naming.InternalNamingStrategy;
import info.archinnov.achilles.internals.types.OverridingOptional;
import info.archinnov.achilles.type.SchemaNameProvider;
import info.archinnov.achilles.type.codec.Codec;
import info.archinnov.achilles.type.codec.CodecSignature;
import info.archinnov.achilles.validation.Validator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

public abstract class AbstractUDTClassProperty<A>
        implements InjectUserAndTupleTypeFactory,
        InjectKeyspace,
        InjectJacksonMapper, InjectRuntimeCodecs, InjectSchemaStrategy {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractUDTClassProperty.class);

    protected static final Object[] NO_ARG = new Object[0];

    public final Optional<String> staticKeyspace;
    public final Optional<InternalNamingStrategy> staticNamingStrategy;
    public final Optional<String> staticUdtName;
    public final Class<A> udtClass;
    public final String udtName;
    public final List<AbstractProperty<A, ?, ?>> componentsProperty;
    public final List<AbstractProperty<A, ?, ?>> constructorProperties;
    public final Class<?> parentEntityClass;
    protected UserTypeFactory userTypeFactory;
    protected UserType userType;
    protected Optional<SchemaNameProvider> schemaNameProvider = Optional.empty();
    String keyspace;

    public AbstractUDTClassProperty() {
        this.staticKeyspace = getStaticKeyspace();
        this.staticNamingStrategy = getStaticNamingStrategy();
        this.staticUdtName = getStaticUdtName();
        this.udtName = getUdtName();
        this.udtClass = getUdtClass();
        this.parentEntityClass = getParentEntityClass();
        this.constructorProperties = getConstructorProperties();
        this.componentsProperty = getComponentsProperty().stream().filter(x -> !constructorProperties.contains(x)).collect(toList());
    }

    protected abstract A create(Object[] args);

    protected abstract List<AbstractProperty<A, ?, ?>> getConstructorProperties();

    protected abstract Optional<String> getStaticKeyspace();

    protected abstract Optional<InternalNamingStrategy> getStaticNamingStrategy();

    protected abstract Optional<String> getStaticUdtName();

    protected abstract String getUdtName();

    protected abstract Class<A> getUdtClass();

    protected abstract List<AbstractProperty<A, ?, ?>> getComponentsProperty();

    protected abstract Class<?> getParentEntityClass();

    protected abstract UDTValue createUDTFromBean(A instance, Optional<CassandraOptions> cassandraOptions);

    protected A createBeanFromUDT(UDTValue udtValue) {
        final List<AbstractProperty<A, ?, ?>> constructorProperties = getConstructorProperties();
        final A instance = create(constructorProperties.isEmpty() ? NO_ARG : constructorProperties.stream()
                        .map(x -> x.decodeFromGettable(udtValue))
                        .toArray(Object[]::new));
        getComponentsProperty().forEach(x -> x.decodeField(udtValue, instance));
        return instance;
    }

    protected UserType getUserType(Optional<CassandraOptions> cassandraOptions) {
        if (cassandraOptions.isPresent()) {
            return buildType(cassandraOptions);
        } else {
            return userType;
        }
    }

    public UserType buildType(Optional<CassandraOptions> cassandraOptions) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Building UserType instance for the current UDT class meta %s", this.toString()));
        }

        Optional<String> keyspaceName = OverridingOptional
                .from(cassandraOptions.flatMap(CassandraOptions::getSchemaNameProvider).map(x -> x.keyspaceFor(parentEntityClass)))
                .andThen(this.schemaNameProvider.map(x -> x.keyspaceFor(parentEntityClass)))
                .andThen(staticKeyspace.orElse(keyspace))
                .getOptional();

        Validator.validateTrue(keyspaceName.isPresent(),
                "The keyspace name for the UDT type '%s' should be either provided by the '%s' annotation or at runtime",
                udtClass.getCanonicalName(), UDT.class.getSimpleName());
        List<UserType.Field> fields = getComponentsProperty()
                .stream()
                .map(property -> userTypeFactory.fieldFor(property.fieldInfo.cqlColumn, property.buildType(cassandraOptions)))
                .collect(Collectors.toList());
        return userTypeFactory.typeFor(keyspaceName.get(), udtName, fields);
    }

    public String generateSchema(SchemaContext context) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Generating creation script for current UDT class meta %s", this.toString()));
        }
        Optional<String> keyspace = Optional.ofNullable(context.keyspace.orElse(staticKeyspace.orElse(null)));
        final CreateType type;
        if (keyspace.isPresent()) {
            type = SchemaBuilder.createType(keyspace.get(), udtName).ifNotExists();
        } else {
            type = SchemaBuilder.createType(udtName).ifNotExists();
        }

        for (AbstractProperty<A, ?, ?> x : componentsProperty) {
            type.addColumn(x.fieldInfo.quotedCqlColumn, x.buildType(Optional.empty()));
        }

        return type.getQueryString().replaceFirst("\t+", "") + ";";
    }

    @Override
    public void inject(UserTypeFactory userTypeFactory, TupleTypeFactory tupleTypeFactory) {
        this.userTypeFactory = userTypeFactory;
        for (AbstractProperty<A, ?, ?> x : componentsProperty) {
            x.inject(userTypeFactory, tupleTypeFactory);
        }
        userType = this.buildType(schemaNameProvider.map(CassandraOptions::withSchemaNameProvider));
    }

    @Override
    public void inject(ObjectMapper jacksonMapper) {
        for (AbstractProperty<A, ?, ?> x : componentsProperty) {
            x.inject(jacksonMapper);
        }
    }

    @Override
    public void injectRuntimeCodecs(Map<CodecSignature<?, ?>, Codec<?, ?>> runtimeCodecs) {
        for (AbstractProperty<A, ?, ?> x : componentsProperty) {
            x.injectRuntimeCodecs(runtimeCodecs);
        }
    }

    @Override
    public void injectKeyspace(String keyspace) {
        this.keyspace = keyspace;
        for (AbstractProperty<A, ?, ?> x : componentsProperty) {
            x.injectKeyspace(keyspace);
        }
    }

    @Override
    public void inject(SchemaNameProvider schemaNameProvider) {
        this.schemaNameProvider = Optional.ofNullable(schemaNameProvider);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AbstractUDTClassProperty{");
        sb.append("componentsProperty=").append(componentsProperty);
        sb.append(", udtName='").append(udtName).append('\'');
        sb.append(", keyspace='").append(keyspace).append('\'');
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AbstractUDTClassProperty<?> that = (AbstractUDTClassProperty<?>) o;
        return Objects.equals(staticKeyspace, that.staticKeyspace) &&
                Objects.equals(staticNamingStrategy, that.staticNamingStrategy) &&
                Objects.equals(staticUdtName, that.staticUdtName) &&
                Objects.equals(udtClass, that.udtClass) &&
                Objects.equals(udtName, that.udtName) &&
                Objects.equals(keyspace, that.keyspace);
    }

    @Override
    public int hashCode() {
        return Objects.hash(staticKeyspace, staticNamingStrategy, staticUdtName, udtClass, udtName, keyspace);
    }
}
