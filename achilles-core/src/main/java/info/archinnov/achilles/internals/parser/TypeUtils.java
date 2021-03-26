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

package info.archinnov.achilles.internals.parser;

import static com.squareup.javapoet.TypeName.OBJECT;
import static java.lang.String.format;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.lang3.ArrayUtils;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.*;
import com.fasterxml.jackson.databind.type.SimpleType;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import com.squareup.javapoet.*;

import info.archinnov.achilles.annotations.DSE_Search;
import info.archinnov.achilles.annotations.SASI.Analyzer;
import info.archinnov.achilles.annotations.SASI.IndexMode;
import info.archinnov.achilles.annotations.SASI.Normalization;
import info.archinnov.achilles.bootstrap.AbstractManagerFactoryBuilder;
import info.archinnov.achilles.configuration.ConfigurationParameters;
import info.archinnov.achilles.generated.function.AbstractCQLCompatibleType;
import info.archinnov.achilles.internals.apt.annotations.AchillesMeta;
import info.archinnov.achilles.internals.cassandra_version.InternalCassandraVersion;
import info.archinnov.achilles.internals.codec.*;
import info.archinnov.achilles.internals.codegen.function.InternalSystemFunctionRegistry;
import info.archinnov.achilles.internals.context.ConfigurationContext;
import info.archinnov.achilles.internals.dsl.crud.*;
import info.archinnov.achilles.internals.dsl.query.delete.*;
import info.archinnov.achilles.internals.dsl.query.select.*;
import info.archinnov.achilles.internals.dsl.query.update.*;
import info.archinnov.achilles.internals.dsl.raw.NativeQuery;
import info.archinnov.achilles.internals.dsl.raw.TypedQuery;
import info.archinnov.achilles.internals.metamodel.*;
import info.archinnov.achilles.internals.metamodel.columns.*;
import info.archinnov.achilles.internals.metamodel.functions.FunctionCall;
import info.archinnov.achilles.internals.metamodel.functions.FunctionProperty;
import info.archinnov.achilles.internals.metamodel.index.IndexInfo;
import info.archinnov.achilles.internals.metamodel.index.IndexType;
import info.archinnov.achilles.internals.options.CassandraOptions;
import info.archinnov.achilles.internals.runtime.AbstractManager;
import info.archinnov.achilles.internals.runtime.AbstractManagerFactory;
import info.archinnov.achilles.internals.runtime.RuntimeEngine;
import info.archinnov.achilles.internals.strategy.naming.InternalNamingStrategy;
import info.archinnov.achilles.internals.types.ConfigMap;
import info.archinnov.achilles.internals.types.RuntimeCodecWrapper;
import info.archinnov.achilles.internals.utils.TypeNameHelper;
import info.archinnov.achilles.type.SchemaNameProvider;
import info.archinnov.achilles.type.strategy.InsertStrategy;
import info.archinnov.achilles.type.tuples.*;
import info.archinnov.achilles.validation.Validator;

public class TypeUtils {

    public static final String META_SUFFIX = "_AchillesMeta";

    public static final String DSL_TOKEN = "Token";
    public static final String DSL_RELATION = "Relation";
    public static final String DSL_GROUP_BY = "GroupBy";

    public static final String COLUMNS_DSL_SUFFIX = "Cols";
    public static final String COLUMNS_TYPED_MAP_DSL_SUFFIX = "ColsTM";
    public static final String FROM_DSL_SUFFIX = "F";
    public static final String FROM_JSON_DSL_SUFFIX = "F_J";
    public static final String FROM_TYPED_MAP_DSL_SUFFIX = "F_TM";
    public static final String WHERE_DSL_SUFFIX = "W";
    public static final String WHERE_TYPED_MAP_DSL_SUFFIX = "W_TM";
    public static final String WHERE_JSON_DSL_SUFFIX = "W_J";
    public static final String END_DSL_SUFFIX = "E";
    public static final String END_TYPED_MAP_DSL_SUFFIX = "E_TM";
    public static final String END_JSON_DSL_SUFFIX = "E_J";

    public static final String SELECT_DSL_SUFFIX = "_Select";
    public static final String INDEX_SELECT_DSL_SUFFIX = "_SelectIndex";

    public static final String DELETE_DSL_SUFFIX = "_Delete";
    public static final String DELETE_STATIC_DSL_SUFFIX = "_DeleteStatic";


    public static final String UPDATE_DSL_SUFFIX = "_Update";
    public static final String UPDATE_STATIC_DSL_SUFFIX = "_UpdateStatic";

    public static final String MANAGER_SUFFIX = "_Manager";
    public static final String CRUD_SUFFIX = "_CRUD";
    public static final String DSL_SUFFIX = "_DSL";
    public static final String INDEX_SUFFIX = "_INDEX";
    public static final String RAW_QUERY_SUFFIX = "_RAW_QUERY";
    public static final String FUNCTION_TYPE_SUFFIX = "_Type";
    public static final String FUNCTION_PROPERTY_SUFFIX = "_FunctionProperty";
    public static final String GENERATED_PACKAGE = "info.archinnov.achilles.generated";
    public static final String ENTITY_META_PACKAGE = "info.archinnov.achilles.generated.meta.entity";
    public static final String UDT_META_PACKAGE = "info.archinnov.achilles.generated.meta.udt";
    public static final String MANAGER_PACKAGE = "info.archinnov.achilles.generated.manager";
    public static final String FUNCTION_PACKAGE = "info.archinnov.achilles.generated.function";
    public static final String DSL_PACKAGE = "info.archinnov.achilles.generated.dsl";
    public static final String MANAGER_FACTORY_BUILDER_CLASS_NAME = "ManagerFactoryBuilder";
    public static final String MANAGER_FACTORY_CLASS_NAME = "ManagerFactory";
    public static final String FUNCTIONS_REGISTRY_CLASS = "FunctionsRegistry";
    public static final String SYSTEM_FUNCTIONS_CLASS = "SystemFunctions";
    public static final String COLUMNS_FOR_FUNCTIONS_CLASS = "ColumnsForFunctions";



    // Codecs
    public static final ClassName JSON_CODEC = ClassName.get(JSONCodec.class);
    public static final ClassName ENUM_NAME_CODEC = ClassName.get(EnumNameCodec.class);

    public static final ClassName ENUM_ORDINAL_CODEC = ClassName.get(EnumOrdinalCodec.class);
    public static final ClassName BYTE_ARRAY_PRIMITIVE_CODEC = ClassName.get(ByteArrayPrimitiveCodec.class);
    public static final ClassName BYTE_ARRAY_CODEC = ClassName.get(ByteArrayCodec.class);
    public static final ClassName FALL_THROUGH_CODEC = ClassName.get(FallThroughCodec.class);
    public static final ClassName RUNTIME_CODEC_WRAPPER = ClassName.get(RuntimeCodecWrapper.class);

    // Meta data
    public static final ClassName COMPUTED_PROPERTY = ClassName.get(ComputedProperty.class);
    public static final ClassName SIMPLE_PROPERTY = ClassName.get(SimpleProperty.class);
    public static final ClassName LIST_PROPERTY = ClassName.get(ListProperty.class);
    public static final ClassName SET_PROPERTY = ClassName.get(SetProperty.class);
    public static final ClassName MAP_PROPERTY = ClassName.get(MapProperty.class);
    public static final ClassName UDT_PROPERTY = ClassName.get(UDTProperty.class);
    public static final ClassName JDK_OPTIONAL_PROPERTY = ClassName.get(JdkOptionalProperty.class);
    public static final ClassName TUPLE1_PROPERTY = ClassName.get(Tuple1Property.class);
    public static final ClassName TUPLE2_PROPERTY = ClassName.get(Tuple2Property.class);
    public static final ClassName TUPLE3_PROPERTY = ClassName.get(Tuple3Property.class);
    public static final ClassName TUPLE4_PROPERTY = ClassName.get(Tuple4Property.class);
    public static final ClassName TUPLE5_PROPERTY = ClassName.get(Tuple5Property.class);
    public static final ClassName TUPLE6_PROPERTY = ClassName.get(Tuple6Property.class);
    public static final ClassName TUPLE7_PROPERTY = ClassName.get(Tuple7Property.class);
    public static final ClassName TUPLE8_PROPERTY = ClassName.get(Tuple8Property.class);
    public static final ClassName TUPLE9_PROPERTY = ClassName.get(Tuple9Property.class);
    public static final ClassName TUPLE10_PROPERTY = ClassName.get(Tuple10Property.class);
    public static final ClassName FIELD_INFO = ClassName.get(FieldInfo.class);
    public static final ClassName COLUMN_TYPE = ClassName.get(ColumnType.class);
    public static final ClassName PARTITION_KEY_INFO = ClassName.get(PartitionKeyInfo.class);
    public static final ClassName CLUSTERING_COLUMN_INFO = ClassName.get(ClusteringColumnInfo.class);
    public static final ClassName COMPUTED_COLUMN_INFO = ClassName.get(ComputedColumnInfo.class);
    public static final ClassName COLUMN_INFO = ClassName.get(ColumnInfo.class);
    public static final ClassName CLUSTERING_ORDER = ClassName.get(ClusteringOrder.class);
    public static final ClassName INDEX_INFO = ClassName.get(IndexInfo.class);
    public static final ClassName INDEX_TYPE = ClassName.get(IndexType.class);
    public static final ClassName SASI_INDEX_MODE = ClassName.get(IndexMode.class);
    public static final ClassName SASI_ANALYZER = ClassName.get(Analyzer.class);
    public static final ClassName SASI_NORMALIZATION = ClassName.get(Normalization.class);

    public static final ClassName ABSTRACT_PROPERTY = ClassName.get(AbstractProperty.class);
    public static final ClassName ABSTRACT_UDT_CLASS_PROPERTY = ClassName.get(AbstractUDTClassProperty.class);
    public static final ClassName FUNCTION_PROPERTY = ClassName.get(FunctionProperty.class);

    public static final ClassName CONSISTENCY_LEVEL = ClassName.get(ConsistencyLevel.class);
    public static final ClassName INSERT_STRATEGY = ClassName.get(InsertStrategy.class);
    public static final ClassName NAMING_STRATEGY = ClassName.get(InternalNamingStrategy.class);
    public static final ClassName BIMAP = ClassName.get(BiMap.class);
    public static final ClassName HASHBIMAP = ClassName.get(HashBiMap.class);
    public static final ClassName ACHILLES_META_ANNOT = ClassName.get(AchillesMeta.class);
    public static final ClassName DSE_SEARCH_ANNOT = ClassName.get(DSE_Search.class);

    // DSL
    public static final ClassName ABSTRACT_SELECT = ClassName.get(AbstractSelect.class);
    public static final ClassName ABSTRACT_SELECT_COLUMNS = ClassName.get(AbstractSelectColumns.class);
    public static final ClassName ABSTRACT_SELECT_COLUMNS_TYPED_MAP = ClassName.get(AbstractSelectColumnsTypeMap.class);
    public static final ClassName ABSTRACT_SELECT_FROM = ClassName.get(AbstractSelectFrom.class);
    public static final ClassName ABSTRACT_SELECT_FROM_TYPED_MAP = ClassName.get(AbstractSelectFromTypeMap.class);
    public static final ClassName ABSTRACT_SELECT_FROM_JSON = ClassName.get(AbstractSelectFromJSON.class);
    public static final ClassName ABSTRACT_SELECT_WHERE = ClassName.get(AbstractSelectWhere.class);
    public static final ClassName ABSTRACT_INDEX_SELECT_WHERE = ClassName.get(AbstractIndexSelectWhere.class);
    public static final ClassName ABSTRACT_SELECT_WHERE_TYPED_MAP = ClassName.get(AbstractSelectWhereTypeMap.class);
    public static final ClassName ABSTRACT_INDEX_SELECT_WHERE_TYPED_MAP = ClassName.get(AbstractIndexSelectWhereTypeMap.class);
    public static final ClassName ABSTRACT_SELECT_WHERE_JSON = ClassName.get(AbstractSelectWhereJSON.class);
    public static final ClassName ABSTRACT_INDEX_SELECT_WHERE_JSON = ClassName.get(AbstractIndexSelectWhereJSON.class);
    public static final ClassName ABSTRACT_SELECT_WHERE_PARTITION = ClassName.get(AbstractSelectWherePartition.class);
    public static final ClassName ABSTRACT_SELECT_WHERE_PARTITION_TYPED_MAP = ClassName.get(AbstractSelectWherePartitionTypeMap.class);
    public static final ClassName ABSTRACT_SELECT_WHERE_PARTITION_JSON = ClassName.get(AbstractSelectWherePartitionJSON.class);
    public static final ClassName ABSTRACT_DELETE = ClassName.get(AbstractDelete.class);
    public static final ClassName ABSTRACT_DELETE_COLUMNS = ClassName.get(AbstractDeleteColumns.class);
    public static final ClassName ABSTRACT_DELETE_FROM = ClassName.get(AbstractDeleteFrom.class);
    public static final ClassName ABSTRACT_DELETE_WHERE_PARTITION = ClassName.get(AbstractDeleteWherePartition.class);
    public static final ClassName ABSTRACT_DELETE_WHERE = ClassName.get(AbstractDeleteWhere.class);
    public static final ClassName ABSTRACT_DELETE_END = ClassName.get(AbstractDeleteEnd.class);
    public static final ClassName ABSTRACT_UPDATE = ClassName.get(AbstractUpdate.class);
    public static final ClassName ABSTRACT_UPDATE_COLUMNS = ClassName.get(AbstractUpdateColumns.class);
    public static final ClassName ABSTRACT_UPDATE_FROM = ClassName.get(AbstractUpdateFrom.class);
    public static final ClassName ABSTRACT_UPDATE_WHERE = ClassName.get(AbstractUpdateWhere.class);
    public static final ClassName ABSTRACT_UPDATE_END = ClassName.get(AbstractUpdateEnd.class);
    public static final ClassName NOT_EQ = ClassName.get(NotEq.class);

    // Query
    public static final ClassName TYPED_QUERY = ClassName.get(TypedQuery.class);
    public static final ClassName NATIVE_QUERY = ClassName.get(NativeQuery.class);

    // Achilles
    public static final ClassName OPTIONS = ClassName.get(CassandraOptions.class);
    public static final ClassName VALIDATOR = ClassName.get(Validator.class);
    public static final ClassName CONFIG_MAP = ClassName.get(ConfigMap.class);
    public static final ClassName SCHEMA_NAME_PROVIDER = ClassName.get(SchemaNameProvider.class);
    public static final ClassName ABSTRACT_MANAGER_FACTORY_BUILDER = ClassName.get(AbstractManagerFactoryBuilder.class);
    public static final ClassName CONFIGURATION_PARAMETERS = ClassName.get(ConfigurationParameters.class);
    public static final ClassName CONFIGURATION_CONTEXT = ClassName.get(ConfigurationContext.class);
    public static final ClassName MANAGER_FACTORY_BUILDER_TYPE_NAME = ClassName.get(GENERATED_PACKAGE, MANAGER_FACTORY_BUILDER_CLASS_NAME);
    public static final ClassName ABSTRACT_MANAGER_FACTORY = ClassName.get(AbstractManagerFactory.class);
    public static final ClassName MANAGER_FACTORY_TYPE_NAME = ClassName.get(GENERATED_PACKAGE, MANAGER_FACTORY_CLASS_NAME);
    public static final ClassName ABSTRACT_MANAGER = ClassName.get(AbstractManager.class);
    public static final ClassName ABSTRACT_ENTITY_PROPERTY = ClassName.get(AbstractEntityProperty.class);
    public static final ClassName ABSTRACT_VIEW_PROPERTY = ClassName.get(AbstractViewProperty.class);
    public static final ClassName RUNTIME_ENGINE = ClassName.get(RuntimeEngine.class);
    public static final ClassName INSERT_WITH_OPTIONS = ClassName.get(InsertWithOptions.class);
    public static final ClassName UPDATE_WITH_OPTIONS = ClassName.get(UpdateWithOptions.class);
    public static final ClassName INSERT_JSON_WITH_OPTIONS = ClassName.get(InsertJSONWithOptions.class);
    public static final ClassName FIND_WITH_OPTIONS = ClassName.get(FindWithOptions.class);
    public static final ClassName DELETE_WITH_OPTIONS = ClassName.get(DeleteWithOptions.class);
    public static final ClassName DELETE_BY_PARTITION_WITH_OPTIONS = ClassName.get(DeleteByPartitionWithOptions.class);
    public static final ClassName INTERNAL_CASSANDRA_VERSION = ClassName.get(InternalCassandraVersion.class);

    // UDF & UDA
    public static final ClassName ABSTRACT_CQL_COMPATIBLE_TYPE = ClassName.get(AbstractCQLCompatibleType.class);
    public static final ClassName FUNCTION_CALL = ClassName.get(FunctionCall.class);
    public static final TypeName SYSTEM_FUNCTION_REGISTRY = ClassName.get(InternalSystemFunctionRegistry.class);


    // Common
    public static final TypeName WILDCARD = WildcardTypeName.subtypeOf(TypeName.OBJECT);
    public static final ClassName OPTIONAL = ClassName.get(Optional.class);
    public static final ClassName CLASS = ClassName.get(Class.class);
    public static final ClassName ARRAYS_UTILS = ClassName.get(ArrayUtils.class);
    public static final ClassName ARRAY_LIST = ClassName.get(ArrayList.class);
    public static final ClassName ARRAYS = ClassName.get(Arrays.class);
    public static final ClassName COLLECTORS = ClassName.get(Collectors.class);
    public static final ClassName SETS = ClassName.get(Sets.class);
    public static final ClassName SIMPLE_DATE_FORMAT = ClassName.get(SimpleDateFormat.class);
    public static final TypeName LIST_OBJECT = ParameterizedTypeName.get(ClassName.get(List.class), TypeName.OBJECT);
    public static final TypeName OVERRIDE_ANNOTATION = ClassName.get(Override.class);

    // Jackson types
    public static final ClassName SIMPLE_TYPE = ClassName.get(SimpleType.class);

    // CQL types
    public static final ClassName JAVA_DRIVER_TUPLE_VALUE_TYPE = ClassName.get(TupleValue.class);
    public static final ClassName JAVA_DRIVER_UDT_VALUE_TYPE = ClassName.get(UDTValue.class);
    public static final ClassName JAVA_DRIVER_USER_TYPE = ClassName.get(UserType.class);
    public static final ClassName DATATYPE = ClassName.get(DataType.class);
    public static final ClassName LIST = ClassName.get(List.class);
    public static final ClassName SET = ClassName.get(Set.class);
    public static final ClassName MAP = ClassName.get(Map.class);

    public static final TypeName NATIVE_BYTE_ARRAY = TypeName.get(byte[].class);
    public static final TypeName OBJECT_BYTE_ARRAY = TypeName.get(Byte[].class);
    public static final TypeName BYTE_BUFFER = ClassName.get(ByteBuffer.class);
    public static final TypeName STRING = ClassName.get(String.class);
    public static final TypeName DOUBLE_ARRAY = TypeName.get(double[].class);
    public static final TypeName FLOAT_ARRAY = TypeName.get(float[].class);
    public static final TypeName INT_ARRAY = TypeName.get(int[].class);
    public static final TypeName LONG_ARRAY = TypeName.get(long[].class);
    public static final TypeName NATIVE_BOOLEAN = TypeName.get(boolean.class);
    public static final TypeName OBJECT_BOOLEAN = ClassName.get(Boolean.class);
    public static final TypeName NATIVE_BYTE = TypeName.get(byte.class);
    public static final TypeName OBJECT_BYTE = ClassName.get(Byte.class);
    public static final TypeName NATIVE_SHORT = TypeName.get(short.class);
    public static final TypeName OBJECT_SHORT = ClassName.get(Short.class);
    public static final TypeName NATIVE_INT = TypeName.get(int.class);
    public static final TypeName OBJECT_INT = ClassName.get(Integer.class);
    public static final TypeName BIG_INT = ClassName.get(BigInteger.class);
    public static final TypeName NATIVE_LONG = TypeName.get(long.class);
    public static final TypeName OBJECT_LONG = ClassName.get(Long.class);
    public static final TypeName NATIVE_DOUBLE = TypeName.get(double.class);
    public static final TypeName OBJECT_DOUBLE = ClassName.get(Double.class);
    public static final TypeName NATIVE_FLOAT = TypeName.get(float.class);
    public static final TypeName OBJECT_FLOAT = ClassName.get(Float.class);
    public static final TypeName BIG_DECIMAL = ClassName.get(BigDecimal.class);
    public static final TypeName INET_ADDRESS = ClassName.get(InetAddress.class);
    public static final TypeName UUID = ClassName.get(UUID.class);
    public static final TypeName JAVA_DRIVER_LOCAL_DATE = ClassName.get(LocalDate.class);
    public static final TypeName JAVA_DRIVER_DURATION = ClassName.get(Duration.class);
    public static final TypeName JAVA_UTIL_DATE = ClassName.get(Date.class);
    public static final TypeName JAVA_TIME_INSTANT = ClassName.get(java.time.Instant.class);
    public static final TypeName JAVA_TIME_LOCAL_DATE = ClassName.get(java.time.LocalDate.class);
    public static final TypeName JAVA_TIME_LOCAL_TIME = ClassName.get(java.time.LocalTime.class);
    public static final TypeName JAVA_TIME_ZONED_DATE_TME = ClassName.get(java.time.ZonedDateTime.class);
    public static final TypeName TUPLE_TYPE = ClassName.get(TupleType.class);

    // Java Driver types
    public static final TypeName CLUSTER = ClassName.get(Cluster.class);
    public static final TypeName ROW = ClassName.get(Row.class);
    public static final TypeName SELECT_DOT_SELECTION = ClassName.get(Select.Selection.class);
    public static final TypeName SELECT_DOT_WHERE = ClassName.get(Select.Where.class);
    public static final TypeName DELETE_DOT_WHERE = ClassName.get(Delete.Where.class);
    public static final TypeName DELETE_DOT_SELECTION = ClassName.get(Delete.Selection.class);
    public static final TypeName UPDATE_DOT_WHERE = ClassName.get(Update.Where.class);
    public static final TypeName QUERY_BUILDER = ClassName.get(QueryBuilder.class);
    public static final TypeName BOUND_STATEMENT = ClassName.get(BoundStatement.class);
    public static final TypeName PREPARED_STATEMENT = ClassName.get(PreparedStatement.class);
    public static final TypeName REGULAR_STATEMENT = ClassName.get(RegularStatement.class);
    public static final ClassName TYPE_TOKEN = ClassName.get(TypeToken.class);
    public static final ClassName NON_ESCAPING_ASSIGNMENT = ClassName.get(NonEscapingSetAssignment.class);
    public static final ClassName MAP_ENTRY_CLAUSE = ClassName.get(MapEntryClause.class);

    // Java 8 Types
    public static final TypeName JDK_ZONED_DATE_TIME = ClassName.get(ZonedDateTime.class);

    // Achilles Tuple Types
    public static final ClassName TUPLE1 = ClassName.get(Tuple1.class);

    public static final ClassName TUPLE2 = ClassName.get(Tuple2.class);
    public static final ClassName TUPLE3 = ClassName.get(Tuple3.class);
    public static final ClassName TUPLE4 = ClassName.get(Tuple4.class);
    public static final ClassName TUPLE5 = ClassName.get(Tuple5.class);
    public static final ClassName TUPLE6 = ClassName.get(Tuple6.class);
    public static final ClassName TUPLE7 = ClassName.get(Tuple7.class);
    public static final ClassName TUPLE8 = ClassName.get(Tuple8.class);
    public static final ClassName TUPLE9 = ClassName.get(Tuple9.class);
    public static final ClassName TUPLE10 = ClassName.get(Tuple10.class);


    public static final List<TypeName> ALLOWED_TYPES_2_1 = new ArrayList<>();
    public static final List<TypeName> ALLOWED_TYPES_3_10 = new ArrayList<>();
    public static final Set<TypeName> NATIVE_TYPES_2_1 = new HashSet<>();
    public static final Set<TypeName> NATIVE_TYPES_3_10 = new HashSet<>();
//    public static final Map<TypeName, TypeName> NATIVE_TYPES_MAPPING = new HashMap<>();
    public static final Map<TypeName, String> DRIVER_TYPES_MAPPING = new HashMap<>();
    public static final Map<TypeName, String> DRIVER_TYPES_FUNCTION_PARAM_MAPPING = new HashMap<>();

    static {
        // Bytes
        ALLOWED_TYPES_2_1.add(NATIVE_BYTE);
        ALLOWED_TYPES_2_1.add(OBJECT_BYTE);
        ALLOWED_TYPES_2_1.add(NATIVE_BYTE_ARRAY);
        ALLOWED_TYPES_2_1.add(OBJECT_BYTE_ARRAY);
        ALLOWED_TYPES_2_1.add(BYTE_BUFFER);
        ALLOWED_TYPES_2_1.add(DOUBLE_ARRAY);
        ALLOWED_TYPES_2_1.add(FLOAT_ARRAY);
        ALLOWED_TYPES_2_1.add(INT_ARRAY);
        ALLOWED_TYPES_2_1.add(LONG_ARRAY);

        NATIVE_TYPES_2_1.add(OBJECT_BYTE);
        NATIVE_TYPES_2_1.add(BYTE_BUFFER);

        DRIVER_TYPES_MAPPING.put(NATIVE_BYTE, "tinyint()");
        DRIVER_TYPES_MAPPING.put(OBJECT_BYTE, "tinyint()");
        DRIVER_TYPES_MAPPING.put(NATIVE_BYTE_ARRAY, "blob()");
        DRIVER_TYPES_MAPPING.put(OBJECT_BYTE_ARRAY, "blob()");
        DRIVER_TYPES_MAPPING.put(BYTE_BUFFER, "blob()");

        DRIVER_TYPES_FUNCTION_PARAM_MAPPING.put(NATIVE_BYTE, "tinyint");
        DRIVER_TYPES_FUNCTION_PARAM_MAPPING.put(OBJECT_BYTE, "tinyint");
        DRIVER_TYPES_FUNCTION_PARAM_MAPPING.put(NATIVE_BYTE_ARRAY, "blob");
        DRIVER_TYPES_FUNCTION_PARAM_MAPPING.put(OBJECT_BYTE_ARRAY, "blob");
        DRIVER_TYPES_FUNCTION_PARAM_MAPPING.put(BYTE_BUFFER, "blob");

        DRIVER_TYPES_MAPPING.put(DOUBLE_ARRAY, "frozenList(DataType.cdouble())");
        DRIVER_TYPES_MAPPING.put(FLOAT_ARRAY, "frozenList(DataType.cfloat())");
        DRIVER_TYPES_MAPPING.put(INT_ARRAY, "frozenList(DataType.cint())");
        DRIVER_TYPES_MAPPING.put(LONG_ARRAY, "frozenList(DataType.bigint())");

        DRIVER_TYPES_FUNCTION_PARAM_MAPPING.put(DOUBLE_ARRAY, "list<double>");
        DRIVER_TYPES_FUNCTION_PARAM_MAPPING.put(FLOAT_ARRAY, "list<float>");
        DRIVER_TYPES_FUNCTION_PARAM_MAPPING.put(INT_ARRAY, "list<int>");
        DRIVER_TYPES_FUNCTION_PARAM_MAPPING.put(LONG_ARRAY, "list<bigint>");

//        NATIVE_TYPES_MAPPING.put(DOUBLE_ARRAY, genericType(LIST, OBJECT_DOUBLE));
//        NATIVE_TYPES_MAPPING.put(FLOAT_ARRAY, genericType(LIST, OBJECT_FLOAT));
//        NATIVE_TYPES_MAPPING.put(INT_ARRAY, genericType(LIST, OBJECT_INT));
//        NATIVE_TYPES_MAPPING.put(LONG_ARRAY, genericType(LIST, OBJECT_LONG));

        // Boolean
        ALLOWED_TYPES_2_1.add(NATIVE_BOOLEAN);
        ALLOWED_TYPES_2_1.add(OBJECT_BOOLEAN);

        NATIVE_TYPES_2_1.add(OBJECT_BOOLEAN);

        DRIVER_TYPES_MAPPING.put(NATIVE_BOOLEAN, "cboolean()");
        DRIVER_TYPES_MAPPING.put(OBJECT_BOOLEAN, "cboolean()");

        DRIVER_TYPES_FUNCTION_PARAM_MAPPING.put(NATIVE_BOOLEAN, "boolean");
        DRIVER_TYPES_FUNCTION_PARAM_MAPPING.put(OBJECT_BOOLEAN, "boolean");

        // Datetime
        ALLOWED_TYPES_2_1.add(JAVA_UTIL_DATE);
        NATIVE_TYPES_2_1.add(JAVA_UTIL_DATE);

        DRIVER_TYPES_MAPPING.put(JAVA_UTIL_DATE, "timestamp()");
        DRIVER_TYPES_FUNCTION_PARAM_MAPPING.put(JAVA_UTIL_DATE, "timestamp");

        // Date
        ALLOWED_TYPES_2_1.add(JAVA_DRIVER_LOCAL_DATE);
        NATIVE_TYPES_2_1.add(JAVA_DRIVER_LOCAL_DATE);

        DRIVER_TYPES_MAPPING.put(JAVA_DRIVER_LOCAL_DATE, "date()");
        DRIVER_TYPES_FUNCTION_PARAM_MAPPING.put(JAVA_DRIVER_LOCAL_DATE, "date");

        //Short
        ALLOWED_TYPES_2_1.add(NATIVE_SHORT);
        ALLOWED_TYPES_2_1.add(OBJECT_SHORT);
        NATIVE_TYPES_2_1.add(OBJECT_SHORT);

        DRIVER_TYPES_MAPPING.put(NATIVE_SHORT, "smallint()");
        DRIVER_TYPES_MAPPING.put(OBJECT_SHORT, "smallint()");

        DRIVER_TYPES_FUNCTION_PARAM_MAPPING.put(NATIVE_SHORT, "smallint");
        DRIVER_TYPES_FUNCTION_PARAM_MAPPING.put(OBJECT_SHORT, "smallint");

        // Double
        ALLOWED_TYPES_2_1.add(NATIVE_DOUBLE);
        ALLOWED_TYPES_2_1.add(OBJECT_DOUBLE);
        NATIVE_TYPES_2_1.add(OBJECT_DOUBLE);

        DRIVER_TYPES_MAPPING.put(NATIVE_DOUBLE, "cdouble()");
        DRIVER_TYPES_MAPPING.put(OBJECT_DOUBLE, "cdouble()");

        DRIVER_TYPES_FUNCTION_PARAM_MAPPING.put(NATIVE_DOUBLE, "double");
        DRIVER_TYPES_FUNCTION_PARAM_MAPPING.put(OBJECT_DOUBLE, "double");

        // Float
        ALLOWED_TYPES_2_1.add(BIG_DECIMAL);
        ALLOWED_TYPES_2_1.add(NATIVE_FLOAT);
        ALLOWED_TYPES_2_1.add(OBJECT_FLOAT);
        NATIVE_TYPES_2_1.add(BIG_DECIMAL);
        NATIVE_TYPES_2_1.add(OBJECT_FLOAT);

        DRIVER_TYPES_MAPPING.put(BIG_DECIMAL, "decimal()");
        DRIVER_TYPES_MAPPING.put(NATIVE_FLOAT, "cfloat()");
        DRIVER_TYPES_MAPPING.put(OBJECT_FLOAT, "cfloat()");

        DRIVER_TYPES_FUNCTION_PARAM_MAPPING.put(BIG_DECIMAL, "decimal");
        DRIVER_TYPES_FUNCTION_PARAM_MAPPING.put(NATIVE_FLOAT, "float");
        DRIVER_TYPES_FUNCTION_PARAM_MAPPING.put(OBJECT_FLOAT, "float");

        // InetAddress
        ALLOWED_TYPES_2_1.add(INET_ADDRESS);
        NATIVE_TYPES_2_1.add(INET_ADDRESS);

        DRIVER_TYPES_MAPPING.put(INET_ADDRESS, "inet()");
        DRIVER_TYPES_FUNCTION_PARAM_MAPPING.put(INET_ADDRESS, "inet");

        // Integer
        ALLOWED_TYPES_2_1.add(NATIVE_INT);
        ALLOWED_TYPES_2_1.add(OBJECT_INT);
        ALLOWED_TYPES_2_1.add(BIG_INT);
        NATIVE_TYPES_2_1.add(OBJECT_INT);
        NATIVE_TYPES_2_1.add(BIG_INT);

        DRIVER_TYPES_MAPPING.put(NATIVE_INT, "cint()");
        DRIVER_TYPES_MAPPING.put(OBJECT_INT, "cint()");
        DRIVER_TYPES_MAPPING.put(BIG_INT, "varint()");

        DRIVER_TYPES_FUNCTION_PARAM_MAPPING.put(NATIVE_INT, "int");
        DRIVER_TYPES_FUNCTION_PARAM_MAPPING.put(OBJECT_INT, "int");
        DRIVER_TYPES_FUNCTION_PARAM_MAPPING.put(BIG_INT, "varint");

        // Long
        ALLOWED_TYPES_2_1.add(NATIVE_LONG);
        ALLOWED_TYPES_2_1.add(OBJECT_LONG);
        NATIVE_TYPES_2_1.add(OBJECT_LONG);

        DRIVER_TYPES_MAPPING.put(NATIVE_LONG, "bigint()");
        DRIVER_TYPES_MAPPING.put(OBJECT_LONG, "bigint()");

        DRIVER_TYPES_FUNCTION_PARAM_MAPPING.put(NATIVE_LONG, "bigint");
        DRIVER_TYPES_FUNCTION_PARAM_MAPPING.put(OBJECT_LONG, "bigint");

        // String
        ALLOWED_TYPES_2_1.add(STRING);
        NATIVE_TYPES_2_1.add(STRING);

        DRIVER_TYPES_MAPPING.put(STRING, "text()");
        DRIVER_TYPES_FUNCTION_PARAM_MAPPING.put(STRING, "text");

        // UUID
        ALLOWED_TYPES_2_1.add(UUID);
        NATIVE_TYPES_2_1.add(UUID);

        DRIVER_TYPES_MAPPING.put(UUID, "uuid()");
        DRIVER_TYPES_FUNCTION_PARAM_MAPPING.put(UUID, "uuid");

        // Tuples
        ALLOWED_TYPES_2_1.add(TUPLE1);
        ALLOWED_TYPES_2_1.add(TUPLE2);
        ALLOWED_TYPES_2_1.add(TUPLE3);
        ALLOWED_TYPES_2_1.add(TUPLE4);
        ALLOWED_TYPES_2_1.add(TUPLE5);
        ALLOWED_TYPES_2_1.add(TUPLE6);
        ALLOWED_TYPES_2_1.add(TUPLE7);
        ALLOWED_TYPES_2_1.add(TUPLE8);
        ALLOWED_TYPES_2_1.add(TUPLE9);
        ALLOWED_TYPES_2_1.add(TUPLE10);


        // Collections
        ALLOWED_TYPES_2_1.add(LIST);
        ALLOWED_TYPES_2_1.add(SET);
        ALLOWED_TYPES_2_1.add(MAP);
        NATIVE_TYPES_2_1.add(LIST);
        NATIVE_TYPES_2_1.add(SET);
        NATIVE_TYPES_2_1.add(MAP);

        // Driver tuple
        ALLOWED_TYPES_2_1.add(JAVA_DRIVER_TUPLE_VALUE_TYPE);
        NATIVE_TYPES_2_1.add(JAVA_DRIVER_TUPLE_VALUE_TYPE);

        // Drive UDTValue
        ALLOWED_TYPES_2_1.add(JAVA_DRIVER_UDT_VALUE_TYPE);
        NATIVE_TYPES_2_1.add(JAVA_DRIVER_UDT_VALUE_TYPE);

        //Java 8 types
        ALLOWED_TYPES_2_1.add(JAVA_TIME_INSTANT);
        ALLOWED_TYPES_2_1.add(JAVA_TIME_LOCAL_DATE);
        ALLOWED_TYPES_2_1.add(JAVA_TIME_LOCAL_TIME);
        ALLOWED_TYPES_2_1.add(JAVA_TIME_ZONED_DATE_TME);

        // Optional
        ALLOWED_TYPES_2_1.add(TypeName.get(java.util.Optional.class));


        DRIVER_TYPES_MAPPING.put(JAVA_TIME_INSTANT, "timestamp()");
        DRIVER_TYPES_MAPPING.put(JAVA_TIME_LOCAL_DATE, "date()");
        DRIVER_TYPES_MAPPING.put(JAVA_TIME_LOCAL_TIME, "time()");
        DRIVER_TYPES_MAPPING.put(JAVA_TIME_ZONED_DATE_TME,
            "com.datastax.driver.core.TupleType.of(com.datastax.driver.core.ProtocolVersion.NEWEST_SUPPORTED, " +
                    "new com.datastax.driver.core.CodecRegistry(), " +
                    "com.datastax.driver.core.DataType.timestamp(), " +
                    "com.datastax.driver.core.DataType.varchar())");

        DRIVER_TYPES_FUNCTION_PARAM_MAPPING.put(JAVA_TIME_INSTANT, "timestamp");
        DRIVER_TYPES_FUNCTION_PARAM_MAPPING.put(JAVA_TIME_LOCAL_DATE, "date");
        DRIVER_TYPES_FUNCTION_PARAM_MAPPING.put(JAVA_TIME_LOCAL_TIME, "time");
        DRIVER_TYPES_FUNCTION_PARAM_MAPPING.put(JAVA_TIME_ZONED_DATE_TME,"tuple<timestamp, varchar>");

//        NATIVE_TYPES_MAPPING.put(JAVA_TIME_INSTANT, JAVA_UTIL_DATE);
//        NATIVE_TYPES_MAPPING.put(JAVA_TIME_LOCAL_DATE, JAVA_DRIVER_LOCAL_DATE);
//        NATIVE_TYPES_MAPPING.put(JAVA_TIME_LOCAL_TIME, OBJECT_LONG);
//        NATIVE_TYPES_MAPPING.put(JAVA_TIME_ZONED_DATE_TME, JAVA_DRIVER_TUPLE_VALUE_TYPE);

        // Duration
        ALLOWED_TYPES_3_10.addAll(ALLOWED_TYPES_2_1);
        ALLOWED_TYPES_3_10.add(JAVA_DRIVER_DURATION);
        NATIVE_TYPES_3_10.addAll(NATIVE_TYPES_2_1);
        NATIVE_TYPES_3_10.add(JAVA_DRIVER_DURATION);

        DRIVER_TYPES_MAPPING.put(JAVA_DRIVER_DURATION, "duration()");
        DRIVER_TYPES_FUNCTION_PARAM_MAPPING.put(JAVA_DRIVER_DURATION, "duration");
    }

    public static String gettableDataGetter(TypeName typeName, String cqlColumn) {
        return "get(\""+escapeDoubleQuotes(cqlColumn)+"\", "+typeName.toString()+".class)";
    }

    public static String settableDataSetter(TypeName typeName, String cqlColumn) {
        return "set(\""+escapeDoubleQuotes(cqlColumn)+"\", value$, "+typeName.toString()+".class)";
    }

    public static CodeBlock buildDataTypeFor(TypeName typeName) {
        final String dataType = DRIVER_TYPES_MAPPING.get(typeName);
        if (typeName.equals(JDK_ZONED_DATE_TIME)) {
            return CodeBlock.builder().add(dataType).build();
        } else {
            return CodeBlock.builder().add("$T.$L", DATATYPE, dataType).build();
        }
    }

    public static String getNativeDataTypeFor(TypeName typeName) {
        if (typeName.isPrimitive()) {
            return getNativeDataTypeFor(typeName.box());
        } else if (typeName instanceof ParameterizedTypeName) {
            final ParameterizedTypeName parameterizedTypeName = (ParameterizedTypeName) typeName;
            final ClassName rawType = parameterizedTypeName.rawType;
            if (rawType.equals(LIST)) {
                return "list<" + getNativeDataTypeFor(parameterizedTypeName.typeArguments.get(0)) + ">";
            } else if (rawType.equals(SET)) {
                return "set<" + getNativeDataTypeFor(parameterizedTypeName.typeArguments.get(0)) + ">";
            } else if (rawType.equals(MAP)) {
                return "map<" + getNativeDataTypeFor(parameterizedTypeName.typeArguments.get(0))
                        + "," + getNativeDataTypeFor(parameterizedTypeName.typeArguments.get(1)) + ">";
            } else {
                throw new IllegalStateException(format("Cannot map Java type '%s' to native Cassandra type", typeName));
            }
        } else {
            return DRIVER_TYPES_MAPPING.get(typeName).replaceAll("\\(","").replaceAll("\\)","");
        }
    }

    public static ParameterizedTypeName genericType(ClassName baseType, TypeName... argTypes) {
        return ParameterizedTypeName.get(baseType, argTypes);
    }

    public static ParameterizedTypeName classTypeOf(TypeName classType) {
        return ParameterizedTypeName.get(CLASS, classType);
    }

    private static String escapeDoubleQuotes(String cqlColumn) {
        if (cqlColumn.startsWith("\"") && cqlColumn.endsWith("\"")) {
            return cqlColumn.replaceAll("\"", "\\\\\"");
        } else {
            return cqlColumn;
        }
    }

    public static TypeName getRawType(TypeName typeName) {
        if (typeName instanceof ParameterizedTypeName) {
            return ((ParameterizedTypeName) typeName).rawType;
        } else {
            return typeName;
        }
    }

    public static TypeName determineTypeForFunctionParam(TypeName typeName) {
         return typeName.equals(OBJECT) ? typeName : ClassName.get(FUNCTION_PACKAGE, TypeNameHelper.asString(typeName) + FUNCTION_TYPE_SUFFIX);
    }
}
