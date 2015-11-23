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

package info.archinnov.achilles.internals.parser;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
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

import info.archinnov.achilles.bootstrap.AbstractManagerFactoryBuilder;
import info.archinnov.achilles.configuration.ConfigurationParameters;
import info.archinnov.achilles.internals.apt.annotations.AchillesMeta;
import info.archinnov.achilles.internals.codec.*;
import info.archinnov.achilles.internals.context.ConfigurationContext;
import info.archinnov.achilles.internals.metamodel.*;
import info.archinnov.achilles.internals.metamodel.columns.*;
import info.archinnov.achilles.internals.metamodel.index.IndexInfo;
import info.archinnov.achilles.internals.metamodel.index.IndexType;
import info.archinnov.achilles.internals.options.Options;
import info.archinnov.achilles.internals.query.crud.DeleteByPartitionWithOptions;
import info.archinnov.achilles.internals.query.crud.DeleteWithOptions;
import info.archinnov.achilles.internals.query.crud.FindWithOptions;
import info.archinnov.achilles.internals.query.crud.InsertWithOptions;
import info.archinnov.achilles.internals.query.dsl.delete.*;
import info.archinnov.achilles.internals.query.dsl.select.*;
import info.archinnov.achilles.internals.query.dsl.update.*;
import info.archinnov.achilles.internals.query.raw.NativeQuery;
import info.archinnov.achilles.internals.query.typed.TypedQuery;
import info.archinnov.achilles.internals.runtime.AbstractManager;
import info.archinnov.achilles.internals.runtime.AbstractManagerFactory;
import info.archinnov.achilles.internals.runtime.RuntimeEngine;
import info.archinnov.achilles.internals.strategy.naming.InternalNamingStrategy;
import info.archinnov.achilles.internals.types.ConfigMap;
import info.archinnov.achilles.type.SchemaNameProvider;
import info.archinnov.achilles.type.strategy.InsertStrategy;
import info.archinnov.achilles.type.tuples.*;
import info.archinnov.achilles.validation.Validator;

public class TypeUtils {

    public static final String META_SUFFIX = "_AchillesMeta";
    public static final String SELECT_COLUMNS_DSL_SUFFIX = "_SelectColumns";
    public static final String SELECT_DSL_SUFFIX = "_Select";
    public static final String SELECT_FROM_DSL_SUFFIX = "_SelectFrom";
    public static final String SELECT_WHERE_DSL_SUFFIX = "_SelectWhere";
    public static final String SELECT_END_DSL_SUFFIX = "_SelectEnd";
    public static final String DELETE_DSL_SUFFIX = "_Delete";
    public static final String DELETE_STATIC_DSL_SUFFIX = "_DeleteStatic";
    public static final String DELETE_COLUMNS_DSL_SUFFIX = "_DeleteColumns";
    public static final String DELETE_STATIC_COLUMNS_DSL_SUFFIX = "_DeleteStaticColumns";
    public static final String DELETE_FROM_DSL_SUFFIX = "_DeleteFrom";
    public static final String DELETE_STATIC_FROM_DSL_SUFFIX = "_DeleteStaticFrom";
    public static final String DELETE_WHERE_DSL_SUFFIX = "_DeleteWhere";
    public static final String DELETE_STATIC_WHERE_DSL_SUFFIX = "_DeleteStaticWhere";
    public static final String DELETE_END_DSL_SUFFIX = "_DeleteEnd";
    public static final String DELETE_STATIC_END_DSL_SUFFIX = "_DeleteStaticEnd";
    public static final String UPDATE_DSL_SUFFIX = "_Update";
    public static final String UPDATE_STATIC_DSL_SUFFIX = "_UpdateStatic";
    public static final String UPDATE_FROM_DSL_SUFFIX = "_UpdateFrom";
    public static final String UPDATE_STATIC_FROM_DSL_SUFFIX = "_UpdateStaticFrom";
    public static final String UPDATE_COLUMNS_DSL_SUFFIX = "_UpdateColumns";
    public static final String UPDATE_STATIC_COLUMNS_DSL_SUFFIX = "_UpdateStaticColumns";
    public static final String UPDATE_WHERE_DSL_SUFFIX = "_UpdateWhere";
    public static final String UPDATE_STATIC_WHERE_DSL_SUFFIX = "_UpdateStaticWhere";
    public static final String UPDATE_END_DSL_SUFFIX = "_UpdateEnd";
    public static final String UPDATE_STATIC_END_DSL_SUFFIX = "_UpdateStaticEnd";
    public static final String MANAGER_SUFFIX = "_Manager";
    public static final String CRUD_SUFFIX = "_CRUD";
    public static final String DSL_SUFFIX = "_DSL";
    public static final String QUERY_SUFFIX = "_QUERY";
    public static final String GENERATED_PACKAGE = "info.archinnov.achilles.generated";
    public static final String ENTITY_META_PACKAGE = "info.archinnov.achilles.generated.meta.entity";
    public static final String UDT_META_PACKAGE = "info.archinnov.achilles.generated.meta.udt";
    public static final String MANAGER_PACKAGE = "info.archinnov.achilles.generated.manager";
    public static final String PROXY_PACKAGE = "info.archinnov.achilles.generated.proxy";
    public static final String DSL_PACKAGE = "info.archinnov.achilles.generated.dsl";
    public static final String MANAGER_FACTORY_BUILDER_CLASS = "ManagerFactoryBuilder";
    public static final String MANAGER_FACTORY_CLASS = "ManagerFactory";

    // Codecs
    public static final ClassName JSON_CODEC = ClassName.get(JSONCodec.class);
    public static final ClassName ENUM_NAME_CODEC = ClassName.get(EnumNameCodec.class);

    public static final ClassName ENUM_ORDINAL_CODEC = ClassName.get(EnumOrdinalCodec.class);
    public static final ClassName BYTE_ARRAY_PRIMITIVE_CODEC = ClassName.get(ByteArrayPrimitiveCodec.class);
    public static final ClassName BYTE_ARRAY_CODEC = ClassName.get(ByteArrayCodec.class);
    public static final ClassName FALL_THROUGH_CODEC = ClassName.get(FallThroughCodec.class);

    // Meta data
    public static final ClassName COMPUTED_PROPERTY = ClassName.get(ComputedProperty.class);
    public static final ClassName SIMPLE_PROPERTY = ClassName.get(SimpleProperty.class);
    public static final ClassName LIST_PROPERTY = ClassName.get(ListProperty.class);
    public static final ClassName SET_PROPERTY = ClassName.get(SetProperty.class);
    public static final ClassName MAP_PROPERTY = ClassName.get(MapProperty.class);
    public static final ClassName UDT_PROPERTY = ClassName.get(UDTProperty.class);
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

    public static final ClassName ABSTRACT_PROPERTY = ClassName.get(AbstractProperty.class);
    public static final ClassName ABSTRACT_UDT_CLASS_PROPERTY = ClassName.get(AbstractUDTClassProperty.class);
    public static final ClassName CONSISTENCY_LEVEL = ClassName.get(ConsistencyLevel.class);
    public static final ClassName INSERT_STRATEGY = ClassName.get(InsertStrategy.class);
    public static final ClassName NAMING_STRATEGY = ClassName.get(InternalNamingStrategy.class);
    public static final ClassName BIMAP = ClassName.get(BiMap.class);
    public static final ClassName HASHBIMAP = ClassName.get(HashBiMap.class);
    public static final ClassName ACHILLES_META_ANNOT = ClassName.get(AchillesMeta.class);

    // DSL
    public static final ClassName ABSTRACT_SELECT = ClassName.get(AbstractSelect.class);
    public static final ClassName ABSTRACT_SELECT_COLUMNS = ClassName.get(AbstractSelectColumns.class);
    public static final ClassName ABSTRACT_SELECT_FROM = ClassName.get(AbstractSelectFrom.class);
    public static final ClassName ABSTRACT_SELECT_WHERE = ClassName.get(AbstractSelectWhere.class);
    public static final ClassName ABSTRACT_SELECT_WHERE_PARTITION = ClassName.get(AbstractSelectWherePartition.class);
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
    public static final ClassName OPTIONS = ClassName.get(Options.class);
    public static final ClassName VALIDATOR = ClassName.get(Validator.class);
    public static final ClassName CONFIG_MAP = ClassName.get(ConfigMap.class);
    public static final ClassName SCHEMA_NAME_PROVIDER = ClassName.get(SchemaNameProvider.class);
    public static final ClassName ABSTRACT_MANAGER_FACTORY_BUILDER = ClassName.get(AbstractManagerFactoryBuilder.class);
    public static final ClassName CONFIGURATION_PARAMETERS = ClassName.get(ConfigurationParameters.class);
    public static final ClassName CONFIGURATION_CONTEXT = ClassName.get(ConfigurationContext.class);
    public static final ClassName MANAGER_FACTORY_BUILDER = ClassName.get(GENERATED_PACKAGE, MANAGER_FACTORY_BUILDER_CLASS);
    public static final ClassName ABSTRACT_MANAGER_FACTORY = ClassName.get(AbstractManagerFactory.class);
    public static final ClassName MANAGER_FACTORY = ClassName.get(GENERATED_PACKAGE, MANAGER_FACTORY_CLASS);
    public static final ClassName ABSTRACT_MANAGER = ClassName.get(AbstractManager.class);
    public static final ClassName ABSTRACT_ENTITY_PROPERTY = ClassName.get(AbstractEntityProperty.class);
    public static final ClassName RUNTIME_ENGINE = ClassName.get(RuntimeEngine.class);
    public static final ClassName INSERT_WITH_OPTIONS = ClassName.get(InsertWithOptions.class);
    public static final ClassName FIND_WITH_OPTIONS = ClassName.get(FindWithOptions.class);
    public static final ClassName DELETE_WITH_OPTIONS = ClassName.get(DeleteWithOptions.class);
    public static final ClassName DELETE_BY_PARTITION_WITH_OPTIONS = ClassName.get(DeleteByPartitionWithOptions.class);

    // Common
    public static final TypeName WILDCARD = WildcardTypeName.subtypeOf(TypeName.OBJECT);
    public static final ClassName OPTIONAL = ClassName.get(Optional.class);
    public static final ClassName CLASS = ClassName.get(Class.class);
    public static final ClassName ARRAYS_UTILS = ClassName.get(ArrayUtils.class);
    public static final ClassName ARRAY_LIST = ClassName.get(ArrayList.class);
    public static final ClassName ARRAYS = ClassName.get(Arrays.class);
    public static final ClassName COLLECTORS = ClassName.get(Collectors.class);
    public static final ClassName SETS = ClassName.get(Sets.class);
    public static final TypeName LIST_OBJECT = ParameterizedTypeName.get(ClassName.get(List.class), TypeName.OBJECT);

    // Jackson types
    public static final ClassName SIMPLE_TYPE = ClassName.get(SimpleType.class);

    // CQL types
    public static final ClassName JAVA_DRIVER_TUPLE_VALUE_TYPE = ClassName.get(TupleValue.class);
    public static final ClassName JAVA_DRIVER_UDT_VALUE_TYPE = ClassName.get(UDTValue.class);
    public static final ClassName DATATYPE = ClassName.get(DataType.class);
    public static final ClassName LIST = ClassName.get(List.class);
    public static final ClassName SET = ClassName.get(Set.class);
    public static final ClassName MAP = ClassName.get(Map.class);
    public static final TypeName PRIMITIVE_BYTE = TypeName.BYTE;
    public static final TypeName OBJECT_BYTE = TypeName.BYTE.box();
    public static final TypeName PRIMITIVE_BYTE_ARRAY = ArrayTypeName.of(PRIMITIVE_BYTE);
    public static final TypeName OBJECT_BYTE_ARRAY = ArrayTypeName.of(OBJECT_BYTE);
    public static final TypeName BYTE_BUFFER = ClassName.get(ByteBuffer.class);
    public static final TypeName PRIMITIVE_BOOL = TypeName.BOOLEAN;
    public static final TypeName OBJECT_BOOL = TypeName.BOOLEAN.box();
    public static final TypeName DATE = ClassName.get(Date.class);
    public static final TypeName LOCAL_DATE = ClassName.get(LocalDate.class);
    public static final TypeName PRIMITIVE_SHORT = TypeName.SHORT;
    public static final TypeName OBJECT_SHORT = TypeName.SHORT.box();
    public static final TypeName PRIMITIVE_DOUBLE = TypeName.DOUBLE;
    public static final TypeName OBJECT_DOUBLE = TypeName.DOUBLE.box();
    public static final TypeName PRIMITIVE_FLOAT = TypeName.FLOAT;
    public static final TypeName OBJECT_FLOAT = TypeName.FLOAT.box();
    public static final TypeName BIG_DECIMAL = ClassName.get(BigDecimal.class);
    public static final TypeName PRIMITIVE_LONG = TypeName.LONG;

    public static final TypeName OBJECT_LONG = TypeName.LONG.box();
    public static final TypeName INET = ClassName.get(InetAddress.class);
    public static final TypeName STRING = ClassName.get(String.class);
    public static final TypeName PRIMITIVE_INT = TypeName.INT;
    public static final TypeName OBJECT_INT = TypeName.INT.box();
    public static final TypeName BIGINT = ClassName.get(BigInteger.class);

    public static final TypeName UUID = ClassName.get(UUID.class);
    public static final TypeName TUPLE_TYPE = ClassName.get(TupleType.class);

    // Java Driver types
    public static final TypeName CLUSTER = ClassName.get(Cluster.class);
    public static final TypeName SELECT_COLUMNS = ClassName.get(Select.Selection.class);
    public static final TypeName SELECT_WHERE = ClassName.get(Select.Where.class);
    public static final TypeName DELETE_WHERE = ClassName.get(Delete.Where.class);
    public static final TypeName DELETE_COLUMNS = ClassName.get(Delete.Selection.class);
    public static final TypeName UPDATE_WHERE = ClassName.get(Update.Where.class);
    public static final TypeName QUERY_BUILDER = ClassName.get(QueryBuilder.class);
    public static final TypeName BOUND_STATEMENT = ClassName.get(BoundStatement.class);
    public static final TypeName PREPARED_STATEMENT = ClassName.get(PreparedStatement.class);
    public static final TypeName REGULAR_STATEMENT = ClassName.get(RegularStatement.class);
    public static final ClassName TYPE_TOKEN = ClassName.get(TypeToken.class);

    public static final List<TypeName> ALLOWED_TYPES = new ArrayList<>();
    public static final Map<TypeName, String> DRIVER_GETTABLEDATA_GETTERS = new HashMap<>();
    public static final Map<TypeName, String> DRIVER_SETTABLEDATA_SETTERS = new HashMap<>();
    public static final Map<TypeName, String> DRIVER_TYPES_MAPPING = new HashMap<>();


    static {
        // Bytes
        ALLOWED_TYPES.add(TypeName.get(byte.class));
        ALLOWED_TYPES.add(TypeName.get(Byte.class));
        ALLOWED_TYPES.add(TypeName.get(byte[].class));
        ALLOWED_TYPES.add(TypeName.get(Byte[].class));
        ALLOWED_TYPES.add(TypeName.get(ByteBuffer.class));

        DRIVER_TYPES_MAPPING.put(TypeName.get(byte.class), "tinyint()");
        DRIVER_TYPES_MAPPING.put(TypeName.get(Byte.class), "tinyint()");
        DRIVER_TYPES_MAPPING.put(TypeName.get(byte[].class), "blob()");
        DRIVER_TYPES_MAPPING.put(TypeName.get(Byte[].class), "blob()");
        DRIVER_TYPES_MAPPING.put(TypeName.get(ByteBuffer.class), "blob()");

        DRIVER_GETTABLEDATA_GETTERS.put(PRIMITIVE_BYTE, "getByte");
        DRIVER_GETTABLEDATA_GETTERS.put(OBJECT_BYTE, "getByte");
        DRIVER_GETTABLEDATA_GETTERS.put(PRIMITIVE_BYTE_ARRAY, "getBytes");
        DRIVER_GETTABLEDATA_GETTERS.put(OBJECT_BYTE_ARRAY, "getBytes");
        DRIVER_GETTABLEDATA_GETTERS.put(BYTE_BUFFER, "getBytes");

        DRIVER_SETTABLEDATA_SETTERS.put(PRIMITIVE_BYTE, "setByte");
        DRIVER_SETTABLEDATA_SETTERS.put(OBJECT_BYTE, "setByte");
        DRIVER_SETTABLEDATA_SETTERS.put(PRIMITIVE_BYTE_ARRAY, "setBytes");
        DRIVER_SETTABLEDATA_SETTERS.put(OBJECT_BYTE_ARRAY, "setBytes");
        DRIVER_SETTABLEDATA_SETTERS.put(BYTE_BUFFER, "setBytes");

        // Boolean
        ALLOWED_TYPES.add(TypeName.get(boolean.class));
        ALLOWED_TYPES.add(TypeName.get(Boolean.class));

        DRIVER_TYPES_MAPPING.put(TypeName.get(boolean.class), "cboolean()");
        DRIVER_TYPES_MAPPING.put(TypeName.get(Boolean.class), "cboolean()");

        DRIVER_GETTABLEDATA_GETTERS.put(PRIMITIVE_BOOL, "getBool");
        DRIVER_GETTABLEDATA_GETTERS.put(OBJECT_BOOL, "getBool");

        DRIVER_SETTABLEDATA_SETTERS.put(PRIMITIVE_BOOL, "setBool");
        DRIVER_SETTABLEDATA_SETTERS.put(OBJECT_BOOL, "setBool");

        // Datetime
        ALLOWED_TYPES.add(TypeName.get(Date.class));
        DRIVER_GETTABLEDATA_GETTERS.put(DATE, "getTimestamp");
        DRIVER_SETTABLEDATA_SETTERS.put(DATE, "setTimestamp");

        DRIVER_TYPES_MAPPING.put(TypeName.get(Date.class), "timestamp()");

        // Date
        ALLOWED_TYPES.add(TypeName.get(LocalDate.class));
        DRIVER_GETTABLEDATA_GETTERS.put(LOCAL_DATE, "getDate");
        DRIVER_SETTABLEDATA_SETTERS.put(LOCAL_DATE, "setDate");

        DRIVER_TYPES_MAPPING.put(TypeName.get(LocalDate.class), "date()");

        //Short
        ALLOWED_TYPES.add(TypeName.get(short.class));
        ALLOWED_TYPES.add(TypeName.get(Short.class));

        DRIVER_TYPES_MAPPING.put(TypeName.get(short.class), "smallint()");
        DRIVER_TYPES_MAPPING.put(TypeName.get(Short.class), "smallint()");

        DRIVER_GETTABLEDATA_GETTERS.put(PRIMITIVE_SHORT, "getShort");
        DRIVER_GETTABLEDATA_GETTERS.put(OBJECT_SHORT, "getShort");

        DRIVER_SETTABLEDATA_SETTERS.put(PRIMITIVE_SHORT, "setShort");
        DRIVER_SETTABLEDATA_SETTERS.put(OBJECT_SHORT, "setShort");

        // Double
        ALLOWED_TYPES.add(TypeName.get(double.class));
        ALLOWED_TYPES.add(TypeName.get(Double.class));

        DRIVER_TYPES_MAPPING.put(TypeName.get(double.class), "cdouble()");
        DRIVER_TYPES_MAPPING.put(TypeName.get(Double.class), "cdouble()");

        DRIVER_GETTABLEDATA_GETTERS.put(PRIMITIVE_DOUBLE, "getDouble");
        DRIVER_GETTABLEDATA_GETTERS.put(OBJECT_DOUBLE, "getDouble");

        DRIVER_SETTABLEDATA_SETTERS.put(PRIMITIVE_DOUBLE, "setDouble");
        DRIVER_SETTABLEDATA_SETTERS.put(OBJECT_DOUBLE, "setDouble");

        // Float
        ALLOWED_TYPES.add(TypeName.get(BigDecimal.class));
        ALLOWED_TYPES.add(TypeName.get(float.class));
        ALLOWED_TYPES.add(TypeName.get(Float.class));

        DRIVER_TYPES_MAPPING.put(TypeName.get(BigDecimal.class), "decimal()");
        DRIVER_TYPES_MAPPING.put(TypeName.get(float.class), "cfloat()");
        DRIVER_TYPES_MAPPING.put(TypeName.get(Float.class), "cfloat()");

        DRIVER_GETTABLEDATA_GETTERS.put(BIG_DECIMAL, "getDecimal");
        DRIVER_GETTABLEDATA_GETTERS.put(PRIMITIVE_FLOAT, "getFloat");
        DRIVER_GETTABLEDATA_GETTERS.put(OBJECT_FLOAT, "getFloat");

        DRIVER_SETTABLEDATA_SETTERS.put(BIG_DECIMAL, "setDecimal");
        DRIVER_SETTABLEDATA_SETTERS.put(PRIMITIVE_FLOAT, "setFloat");
        DRIVER_SETTABLEDATA_SETTERS.put(OBJECT_FLOAT, "setFloat");

        // InetAddress
        ALLOWED_TYPES.add(TypeName.get(InetAddress.class));

        DRIVER_TYPES_MAPPING.put(TypeName.get(InetAddress.class), "inet()");

        DRIVER_GETTABLEDATA_GETTERS.put(INET, "getInet");
        DRIVER_SETTABLEDATA_SETTERS.put(INET, "setInet");

        // Integer
        ALLOWED_TYPES.add(TypeName.get(int.class));
        ALLOWED_TYPES.add(TypeName.get(Integer.class));
        ALLOWED_TYPES.add(TypeName.get(BigInteger.class));

        DRIVER_TYPES_MAPPING.put(TypeName.get(int.class), "cint()");
        DRIVER_TYPES_MAPPING.put(TypeName.get(Integer.class), "cint()");
        DRIVER_TYPES_MAPPING.put(TypeName.get(BigInteger.class), "varint()");

        DRIVER_GETTABLEDATA_GETTERS.put(PRIMITIVE_INT, "getInt");
        DRIVER_GETTABLEDATA_GETTERS.put(OBJECT_INT, "getInt");
        DRIVER_GETTABLEDATA_GETTERS.put(BIGINT, "getVarint");

        DRIVER_SETTABLEDATA_SETTERS.put(PRIMITIVE_INT, "setInt");
        DRIVER_SETTABLEDATA_SETTERS.put(OBJECT_INT, "setInt");
        DRIVER_SETTABLEDATA_SETTERS.put(BIGINT, "setVarint");

        // Long
        ALLOWED_TYPES.add(TypeName.get(long.class));
        ALLOWED_TYPES.add(TypeName.get(Long.class));

        DRIVER_TYPES_MAPPING.put(TypeName.get(long.class), "bigint()");
        DRIVER_TYPES_MAPPING.put(TypeName.get(Long.class), "bigint()");

        DRIVER_GETTABLEDATA_GETTERS.put(PRIMITIVE_LONG, "getLong");
        DRIVER_GETTABLEDATA_GETTERS.put(OBJECT_LONG, "getLong");

        DRIVER_SETTABLEDATA_SETTERS.put(PRIMITIVE_LONG, "setLong");
        DRIVER_SETTABLEDATA_SETTERS.put(OBJECT_LONG, "setLong");

        // String
        ALLOWED_TYPES.add(TypeName.get(String.class));

        DRIVER_TYPES_MAPPING.put(TypeName.get(String.class), "text()");

        DRIVER_GETTABLEDATA_GETTERS.put(STRING, "getString");
        DRIVER_SETTABLEDATA_SETTERS.put(STRING, "setString");

        // UUID
        ALLOWED_TYPES.add(TypeName.get(UUID.class));

        DRIVER_TYPES_MAPPING.put(TypeName.get(UUID.class), "uuid()");

        DRIVER_GETTABLEDATA_GETTERS.put(UUID, "getUUID");
        DRIVER_SETTABLEDATA_SETTERS.put(UUID, "setUUID");

        // Tuples
        ALLOWED_TYPES.add(TypeName.get(Tuple1.class));
        ALLOWED_TYPES.add(TypeName.get(Tuple2.class));
        ALLOWED_TYPES.add(TypeName.get(Tuple3.class));
        ALLOWED_TYPES.add(TypeName.get(Tuple4.class));
        ALLOWED_TYPES.add(TypeName.get(Tuple5.class));
        ALLOWED_TYPES.add(TypeName.get(Tuple6.class));
        ALLOWED_TYPES.add(TypeName.get(Tuple7.class));
        ALLOWED_TYPES.add(TypeName.get(Tuple8.class));
        ALLOWED_TYPES.add(TypeName.get(Tuple9.class));
        ALLOWED_TYPES.add(TypeName.get(Tuple10.class));


        // Collections
        ALLOWED_TYPES.add(TypeName.get(List.class));
        ALLOWED_TYPES.add(TypeName.get(Set.class));
        ALLOWED_TYPES.add(TypeName.get(Map.class));

        // Driver tuple
        ALLOWED_TYPES.add(TypeName.get(TupleValue.class));

        // Drive UDTValue
        ALLOWED_TYPES.add(TypeName.get(UDTValue.class));
    }

    public static ParameterizedTypeName genericType(ClassName baseType, TypeName... argTypes) {
        return ParameterizedTypeName.get(baseType, argTypes);
    }

    public static ParameterizedTypeName classTypeOf(TypeName classType) {
        return ParameterizedTypeName.get(CLASS, classType);
    }
}
