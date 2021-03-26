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

package info.archinnov.achilles.schema;

import static com.datastax.driver.core.ProtocolVersion.NEWEST_SUPPORTED;
import static com.google.common.collect.Sets.newHashSet;
import static info.archinnov.achilles.internals.parser.TypeUtils.ENTITY_META_PACKAGE;
import static info.archinnov.achilles.internals.parser.TypeUtils.UDT_META_PACKAGE;
import static info.archinnov.achilles.validation.Validator.validateNotBlank;
import static info.archinnov.achilles.validation.Validator.validateTrue;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.CodecRegistry;

import info.archinnov.achilles.internals.factory.TupleTypeFactory;
import info.archinnov.achilles.internals.factory.UserTypeFactory;
import info.archinnov.achilles.internals.metamodel.AbstractEntityProperty;
import info.archinnov.achilles.internals.metamodel.AbstractUDTClassProperty;
import info.archinnov.achilles.internals.metamodel.AbstractViewProperty;
import info.archinnov.achilles.internals.schema.SchemaContext;
import info.archinnov.achilles.type.tuples.Tuple2;

public class SchemaGenerator {

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaGenerator.class);

    private static final CodecRegistry CODEC_REGISTRY = new CodecRegistry();
    private static final TupleTypeFactory TUPLE_TYPE_FACTORY = new TupleTypeFactory(NEWEST_SUPPORTED, CODEC_REGISTRY);
    private static final UserTypeFactory USER_TYPE_FACTORY = new UserTypeFactory(NEWEST_SUPPORTED, CODEC_REGISTRY);
    private static final Comparator<Tuple2<String, Class<AbstractEntityProperty<?>>>> BY_NAME_ENTITY_CLASS_SORTER =
            (o1, o2) -> o1._1().compareTo(o2._1());
    private static final Comparator<Tuple2<String, Class<AbstractUDTClassProperty<?>>>> BY_NAME_UDT_CLASS_SORTER =
            (o1, o2) -> o1._1().compareTo(o2._1());
    private Optional<String> keyspace = Optional.empty();
    private boolean createIndex = true;
    private boolean createUdt = true;

    private SchemaGenerator(String keyspaceName) {
        this.keyspace = Optional.ofNullable(keyspaceName);
    }

    public static void main(String... args) throws IOException {
        if (args == null || args.length != 4) {
            System.out.println(displayUsage());
        } else {
            final String targetFile = args[1];
            final String keyspaceName = args[3];
            final Path path = new File(targetFile).toPath();
            Files.deleteIfExists(path);
            Files.createFile(path);
            final SchemaGenerator generator = new SchemaGenerator(keyspaceName);
            generator.createIndex = true;
            generator.createUdt = true;
            generator.generateTo(path);
        }
    }

    private static String displayUsage() {
        StringBuilder builder = new StringBuilder();
        builder.append("*********************************************************************************************************************************************************************\n");
        builder.append("\n");
        builder.append("Usage for Schema Generator : \n");
        builder.append("\n");
        builder.append("java -cp ./your_compiled_entities.jar:./achilles-schema-generator-<version>-shaded.jar info.archinnov.achilles.schema.SchemaGenerator -target <schema_file> -keyspace <keyspace_name> \n");
        builder.append("\n");
        builder.append("*********************************************************************************************************************************************************************\n");

        return builder.toString();
    }

    public static SchemaGenerator builder() {
        return new SchemaGenerator(null);
    }

    public SchemaGenerator withKeyspace(String keyspace) {
        validateNotBlank(keyspace, "Provided keyspace for SchemaGenerator should not be blank");
        this.keyspace = Optional.of(keyspace);
        return this;
    }

    public SchemaGenerator generateCustomTypes(boolean generateCustomTypes) {
        this.createUdt = generateCustomTypes;
        return this;
    }

    public SchemaGenerator withCustomTypes() {
        this.createUdt = true;
        return this;
    }

    public SchemaGenerator withoutCustomTypes() {
        this.createUdt = false;
        return this;
    }

    public SchemaGenerator generateIndices(boolean generateIndices) {
        this.createIndex = generateIndices;
        return this;
    }

    public SchemaGenerator withIndices() {
        this.createIndex = true;
        return this;
    }

    public SchemaGenerator withoutIndices() {
        this.createIndex = false;
        return this;
    }

    @SuppressWarnings("")
    public String generate() {
        LOGGER.info("Start generating schema file ");
        validateNotBlank(keyspace.orElse(""), "Keyspace should be provided to generate schema");
        final SchemaContext context = new SchemaContext(keyspace.get(), createUdt, createIndex);
        ReflectionsHelper.registerUrlTypes(".mar", ".jnilib", ".zip");
        Reflections reflections = new Reflections(newHashSet(ENTITY_META_PACKAGE, UDT_META_PACKAGE), this.getClass().getClassLoader());
        StringBuilder builder = new StringBuilder();

        final List<AbstractEntityProperty<?>> entityMetas = reflections
                .getSubTypesOf(AbstractEntityProperty.class)
                .stream()
                .map(x -> Tuple2.of(x.getCanonicalName(), (Class<AbstractEntityProperty<?>>) x))
                .sorted(BY_NAME_ENTITY_CLASS_SORTER)
                .map(x -> x._2())
                .map(SchemaGenerator::newInstanceForEntityProperty)
                .filter(x -> x != null)
                .collect(toList());

        //Inject keyspace to entity metas
        entityMetas.forEach(x -> x.injectKeyspace(keyspace.get()));

        LOGGER.info(format("Found %s entity meta classes", entityMetas.size()));

        //Generate UDT BEFORE tables
        if (context.createUdt) {
            LOGGER.info(format("Generating schema for UDT"));
            final List<AbstractUDTClassProperty<?>> udtMetas = reflections
                    .getSubTypesOf(AbstractUDTClassProperty.class)
                    .stream()
                    .map(x -> Tuple2.of(x.getCanonicalName(), (Class<AbstractUDTClassProperty<?>>) x))
                    .sorted(BY_NAME_UDT_CLASS_SORTER)
                    .map(x -> x._2())
                    .map(SchemaGenerator::newInstanceForUDTProperty)
                    .filter(x -> x != null)
                    .collect(toList());

            LOGGER.info(format("Found %s udt classes", udtMetas.size()));

            for (AbstractUDTClassProperty<?> instance : udtMetas) {
                instance.injectKeyspace(keyspace.get());
                instance.inject(USER_TYPE_FACTORY, TUPLE_TYPE_FACTORY);
                builder.append(instance.generateSchema(context));
            }

        }

        final long viewCount = entityMetas
                .stream()
                .filter(AbstractEntityProperty::isView)
                .count();


        //Inject base table property into view property
        if (viewCount > 0) {
            final Map<Class<?>, AbstractEntityProperty<?>> entityPropertiesMap = entityMetas
                    .stream()
                    .filter(AbstractEntityProperty::isTable)
                    .collect(Collectors.toMap(x -> x.entityClass, x -> x));

            entityMetas
                    .stream()
                    .filter(AbstractEntityProperty::isView)
                    .map(x -> (AbstractViewProperty<?>)x)
                    .forEach(x -> x.setBaseClassProperty(entityPropertiesMap.get(x.getBaseEntityClass())));
        }

        for (AbstractEntityProperty<?> instance : entityMetas) {
            instance.inject(USER_TYPE_FACTORY, TUPLE_TYPE_FACTORY);
            builder.append(instance.generateSchema(context));
        }

        return builder.toString();
    }

    public void generateTo(Appendable appendable) throws IOException {
        final String schemaString = generate();
        appendable.append(schemaString);
    }

    public void generateTo(Path file) throws IOException {
        validateTrue(Files.exists(file), "'%s' should exist", file.toString());
        validateTrue(Files.isWritable(file), "'%s' should have write permission", file.toString());
        Writer writer = new OutputStreamWriter(Files.newOutputStream(file));
        generateTo(writer);
        writer.flush();
        writer.close();
    }

    public void generateTo(File file) throws IOException {
        generateTo(file.toPath());
    }

    public static AbstractEntityProperty<?> newInstanceForEntityProperty(Class<? extends AbstractEntityProperty<?>> clazz) {
        try {
            return clazz.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static AbstractUDTClassProperty<?> newInstanceForUDTProperty(Class<? extends AbstractUDTClassProperty<?>> clazz) {
        try {
            return clazz.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
        }
        return null;
    }
}
