/*
 * Copyright (C) 2012-2015 DuyHai DOAN
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

import static java.lang.String.format;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.datastax.driver.core.schemabuilder.CreateType;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;

import info.archinnov.achilles.annotations.UDT;
import info.archinnov.achilles.internals.factory.TupleTypeFactory;
import info.archinnov.achilles.internals.factory.UserTypeFactory;
import info.archinnov.achilles.internals.injectable.*;
import info.archinnov.achilles.internals.schema.SchemaContext;
import info.archinnov.achilles.internals.strategy.naming.InternalNamingStrategy;
import info.archinnov.achilles.type.factory.BeanFactory;
import info.archinnov.achilles.validation.Validator;

public abstract class AbstractUDTClassProperty<A>
        implements InjectUserTypeFactory,
        InjectTupleTypeFactory, InjectBeanFactory, InjectKeyspace,
        InjectJacksonMapper {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractUDTClassProperty.class);

    public final Optional<String> staticKeyspace;
    public final Optional<InternalNamingStrategy> staticNamingStrategy;
    public final Optional<String> staticUdtName;
    public final Class<A> udtClass;
    public final String udtName;
    public final List<AbstractProperty<A, ?, ?>> componentsProperty;
    protected BeanFactory udtFactory;
    protected UserTypeFactory userTypeFactory;
    protected UserType userType;
    String keyspace;

    public AbstractUDTClassProperty() {
        this.staticKeyspace = getStaticKeyspace();
        this.staticNamingStrategy = getStaticNamingStrategy();
        this.staticUdtName = getStaticUdtName();
        this.udtName = getUdtName();
        this.udtClass = getUdtClass();
        this.componentsProperty = getComponentsProperty();
    }

    protected abstract Optional<String> getStaticKeyspace();

    protected abstract Optional<InternalNamingStrategy> getStaticNamingStrategy();

    protected abstract Optional<String> getStaticUdtName();

    protected abstract String getUdtName();

    protected abstract Class<A> getUdtClass();

    protected abstract List<AbstractProperty<A, ?, ?>> getComponentsProperty();


    protected abstract UDTValue createUDTFromBean(A instance);

    protected abstract A createBeanFromUDT(UDTValue udtValue);

    public UserType buildType() {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Building UserType instance for the current UDT class meta %s", this.toString()));
        }

        Optional<String> keyspaceName = Optional.ofNullable(staticKeyspace.orElse(keyspace));

        Validator.validateTrue(keyspaceName.isPresent(),
                "The keyspace name for the UDT type '%s' should be either provided by the '%s' annotation or at runtime",
                udtClass.getCanonicalName(), UDT.class.getSimpleName());
        List<UserType.Field> fields = getComponentsProperty()
                .stream()
                .map(property -> userTypeFactory.fieldFor(property.fieldInfo.cqlColumn, property.buildType()))
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

        componentsProperty
                .stream()
                .forEach(x -> type.addColumn(x.fieldInfo.cqlColumn, x.buildType()));

        return type.getQueryString().replaceFirst("\t+", "") + ";";
    }

    @Override
    public void inject(BeanFactory factory) {
        udtFactory = factory;
    }

    @Override
    public void inject(UserTypeFactory factory) {
        userTypeFactory = factory;
        userType = this.buildType();
        componentsProperty
                .stream()
                .forEach(x -> x.inject(factory));
    }

    @Override
    public void inject(TupleTypeFactory factory) {
        componentsProperty
                .stream()
                .forEach(x -> x.inject(factory));
    }

    @Override
    public void inject(ObjectMapper mapper) {
        componentsProperty
                .stream()
                .forEach(x -> x.inject(mapper));
    }

    @Override
    public void injectKeyspace(String keyspace) {
        this.keyspace = keyspace;
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
}
