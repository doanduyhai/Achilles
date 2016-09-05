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

package info.archinnov.achilles.internals.parser.context;

import static info.archinnov.achilles.internals.cassandra_version.InternalCassandraVersion.V2_1;
import static info.archinnov.achilles.internals.cassandra_version.InternalCassandraVersion.V2_2;
import static info.archinnov.achilles.internals.cassandra_version.InternalCassandraVersion.V3_0;
import static info.archinnov.achilles.internals.parser.TypeUtils.*;
import static info.archinnov.achilles.internals.strategy.field_filtering.FieldFilter.*;
import static info.archinnov.achilles.type.CassandraVersion.CASSANDRA_2_1_X;
import static info.archinnov.achilles.type.CassandraVersion.CASSANDRA_2_2_X;
import static info.archinnov.achilles.type.CassandraVersion.CASSANDRA_3_0_X;
import static info.archinnov.achilles.type.strategy.ColumnMappingStrategy.EXPLICIT;
import static info.archinnov.achilles.type.strategy.ColumnMappingStrategy.IMPLICIT;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

import info.archinnov.achilles.annotations.CompileTimeConfig;
import info.archinnov.achilles.internals.cassandra_version.CassandraFeature;
import info.archinnov.achilles.internals.cassandra_version.InternalCassandraVersion;
import info.archinnov.achilles.internals.codegen.crud.CrudAPICodeGen;
import info.archinnov.achilles.internals.codegen.dsl.delete.DeleteDSLCodeGen;
import info.archinnov.achilles.internals.codegen.dsl.delete.DeleteWhereDSLCodeGen;
import info.archinnov.achilles.internals.codegen.dsl.select.SelectDSLCodeGen;
import info.archinnov.achilles.internals.codegen.dsl.select.SelectWhereDSLCodeGen;
import info.archinnov.achilles.internals.codegen.dsl.update.UpdateDSLCodeGen;
import info.archinnov.achilles.internals.codegen.dsl.update.UpdateWhereDSLCodeGen;
import info.archinnov.achilles.internals.codegen.function.FunctionsRegistryCodeGen;
import info.archinnov.achilles.internals.parser.CodecFactory.CodecInfo;
import info.archinnov.achilles.internals.parser.FieldParser.UDTMetaSignature;
import info.archinnov.achilles.internals.parser.validator.BeanValidator;
import info.archinnov.achilles.internals.parser.validator.FieldValidator;
import info.archinnov.achilles.internals.parser.validator.NestedTypesValidator;
import info.archinnov.achilles.internals.parser.validator.TypeValidator;
import info.archinnov.achilles.internals.strategy.field_filtering.FieldFilter;
import info.archinnov.achilles.internals.strategy.naming.InternalNamingStrategy;
import info.archinnov.achilles.internals.strategy.naming.LowerCaseNaming;
import info.archinnov.achilles.internals.utils.NamingHelper;
import info.archinnov.achilles.type.CassandraVersion;
import info.archinnov.achilles.type.strategy.ColumnMappingStrategy;
import info.archinnov.achilles.type.strategy.InsertStrategy;
import info.archinnov.achilles.type.tuples.Tuple2;

public class GlobalParsingContext {

    private static final Map<CassandraVersion, InternalCassandraVersion> VERSION_MAPPING = new HashMap<>();
    private static final Map<ColumnMappingStrategy, Tuple2<FieldFilter, FieldFilter>> COLUMNS_MAPPING = new HashMap<>();

    static {
        VERSION_MAPPING.put(CASSANDRA_2_1_X, V2_1);
        VERSION_MAPPING.put(CASSANDRA_2_2_X, V2_2);
        VERSION_MAPPING.put(CASSANDRA_3_0_X, V3_0);

        COLUMNS_MAPPING.put(EXPLICIT, Tuple2.of(EXPLICIT_ENTITY_FIELD_FILTER, EXPLICIT_UDT_FIELD_FILTER));
        COLUMNS_MAPPING.put(IMPLICIT, Tuple2.of(IMPLICIT_ENTITY_FIELD_FILTER, IMPLICIT_UDT_FIELD_FILTER));
    }

    public final InternalCassandraVersion cassandraVersion;
    public final InsertStrategy insertStrategy;
    public final InternalNamingStrategy namingStrategy;
    public final FieldFilter fieldFilter;
    public final FieldFilter udtFieldFilter;
    public final Optional<String> projectName;
    public final Map<TypeName, TypeSpec> udtTypes = new HashMap<>();
    public final Map<TypeName, UDTMetaSignature> udtMetaSignatures = new HashMap<>();
    public final Map<TypeName, CodecInfo> codecRegistry = new HashMap<>();

    public static GlobalParsingContext fromCompileTimeConfig(CompileTimeConfig compileTimeConfig) {
        final InternalCassandraVersion version = VERSION_MAPPING.get(compileTimeConfig.cassandraVersion());
        final InsertStrategy insertStrategy = compileTimeConfig.insertStrategy();
        final InternalNamingStrategy namingStrategy = InternalNamingStrategy.getNamingStrategy(compileTimeConfig.namingStrategy());
        final Tuple2<FieldFilter, FieldFilter> fieldFilters = COLUMNS_MAPPING.get(compileTimeConfig.columnMappingStrategy());
        final Optional<String> projectName = StringUtils.isBlank(compileTimeConfig.projectName())
                ? Optional.empty()
                : Optional.of(compileTimeConfig.projectName());
        return new GlobalParsingContext(version, insertStrategy, namingStrategy, fieldFilters._1(), fieldFilters._2(), projectName);
    }

    public static GlobalParsingContext defaultContext() {
        return new GlobalParsingContext(V3_0, InsertStrategy.ALL_FIELDS, new LowerCaseNaming(),
                EXPLICIT_ENTITY_FIELD_FILTER, EXPLICIT_UDT_FIELD_FILTER, Optional.empty());
    }

    public GlobalParsingContext(InternalCassandraVersion cassandraVersion, InsertStrategy insertStrategy, InternalNamingStrategy namingStrategy,
                                FieldFilter fieldFilter, FieldFilter udtFieldFilter, Optional<String> projectName) {
        this.cassandraVersion = cassandraVersion;
        this.insertStrategy = insertStrategy;
        this.fieldFilter = fieldFilter;
        this.udtFieldFilter = udtFieldFilter;
        this.namingStrategy = namingStrategy;
        this.projectName = projectName;
    }

    public String managerFactoryBuilderClassName() {
        return projectName.isPresent()
                ? MANAGER_FACTORY_BUILDER_CLASS_NAME + "_For_" + NamingHelper.upperCaseFirst(projectName.get())
                : MANAGER_FACTORY_BUILDER_CLASS_NAME;
    }

    public String managerFactoryClassName() {
        return projectName.isPresent()
                ? MANAGER_FACTORY_CLASS_NAME + "_For_" + NamingHelper.upperCaseFirst(projectName.get())
                : MANAGER_FACTORY_CLASS_NAME;
    }

    public TypeName managerFactoryBuilderTypeName() {
        return projectName.isPresent()
                ? ClassName.get(GENERATED_PACKAGE, managerFactoryBuilderClassName())
                : MANAGER_FACTORY_BUILDER_TYPE_NAME;
    }

    public TypeName managerFactoryTypeName() {
        return projectName.isPresent()
                ? ClassName.get(GENERATED_PACKAGE, managerFactoryClassName())
                : MANAGER_FACTORY_TYPE_NAME;
    }

    public BeanValidator beanValidator() {
        return cassandraVersion.beanValidator();
    }

    public FieldValidator fieldValidator() {
        return cassandraVersion.fieldValidator();
    }

    public TypeValidator typeValidator() {
        return cassandraVersion.typeValidator();
    }

    public NestedTypesValidator nestedTypesValidator() {
        return cassandraVersion.nestedTypesValidator();
    }

    public CrudAPICodeGen crudAPICodeGen() {
        return cassandraVersion.crudApiCodeGen();
    }

    public SelectDSLCodeGen selectDSLCodeGen() {
        return cassandraVersion.selectDslCodeGen();
    }

    public SelectWhereDSLCodeGen selectWhereDSLCodeGen() {
        return cassandraVersion.selectWhereDSLCodeGen();
    }

    public UpdateDSLCodeGen updateDSLCodeGen() {
        return cassandraVersion.updateDslCodeGen();
    }

    public UpdateWhereDSLCodeGen updateWhereDSLCodeGen() {
        return cassandraVersion.updateWhereDslCodeGen();
    }

    public DeleteDSLCodeGen deleteDSLCodeGen() {
        return cassandraVersion.deleteDslCodeGen();
    }

    public DeleteWhereDSLCodeGen deleteWhereDSLCodeGen() {
        return cassandraVersion.deleteWhereDslCodeGen();
    }

    public FunctionsRegistryCodeGen functionsRegistryCodeGen() {
        return cassandraVersion.functionsRegistryCodeGen();
    }

    public boolean supportsFeature(CassandraFeature feature) {
        return cassandraVersion.supportsFeature(feature);
    }

    public boolean hasCodecFor(TypeName typeName) {
        return codecRegistry.containsKey(typeName);
    }

    public CodecInfo getCodecFor(TypeName typeName) {
        return codecRegistry.get(typeName);
    }
}
