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

package info.archinnov.achilles.internals.metamodel;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.GettableData;
import com.datastax.driver.core.SettableData;
import com.datastax.driver.core.UDTValue;
import com.google.common.reflect.TypeToken;

import info.archinnov.achilles.internals.injectable.*;
import info.archinnov.achilles.internals.metamodel.columns.FieldInfo;
import info.archinnov.achilles.internals.options.CassandraOptions;
import info.archinnov.achilles.internals.utils.NamingHelper;
import info.archinnov.achilles.type.SchemaNameProvider;

public abstract class AbstractProperty<ENTITY, VALUEFROM, VALUETO>
        implements InjectUserAndTupleTypeFactory, InjectBeanFactory,
        InjectJacksonMapper, InjectRuntimeCodecs, InjectKeyspace, InjectSchemaStrategy {
    public final FieldInfo<ENTITY, VALUEFROM> fieldInfo;
    public final String fieldName;
    public TypeToken<VALUEFROM> valueFromTypeToken;
    public TypeToken<VALUETO> valueToTypeToken;
    protected Optional<SchemaNameProvider> schemaNameProvider = Optional.empty();
    private DataType dataType;

    AbstractProperty(TypeToken<VALUEFROM> valueFromTypeToken, TypeToken<VALUETO> valueToTypeToken, FieldInfo<ENTITY, VALUEFROM> fieldInfo) {
        this.valueFromTypeToken = valueFromTypeToken;
        this.valueToTypeToken = valueToTypeToken;
        this.fieldInfo = fieldInfo;
        this.fieldName = fieldInfo.fieldName;
    }

    AbstractProperty(FieldInfo<ENTITY, VALUEFROM> fieldInfo) {
        this.fieldInfo = fieldInfo;
        this.fieldName = fieldInfo.fieldName;
    }


    /**
     * Encode given java value to CQL-compatible value using Achilles codec system
     * @param javaValue
     * @return
     */
    public VALUETO encodeFromJava(VALUEFROM javaValue) {
        return encodeFromJava(javaValue, Optional.empty());
    }

    /**
     * Encode given java value to CQL-compatible value using Achilles codec system and a CassandraOptions
     * containing a runtime SchemaNameProvider. Use the
     * <br/>
     * <br/>
     * <pre class="code"><code class="java">
     *     CassandraOptions.withSchemaNameProvider(SchemaNameProvider provider)
     * </code></pre>
     * <br/>
     * static method to build such a CassandraOptions instance
     * @param javaValue
     * @param cassandraOptions
     * @return
     */
    public VALUETO encodeFromJava(VALUEFROM javaValue, Optional<CassandraOptions> cassandraOptions) {
        if (javaValue == null) return null;
        return encodeFromJavaInternal(javaValue, cassandraOptions);
    }

    /**
     * Encode the given Java value to CQL-compatible value using Achilles codec system into the given SettableData (Row, UDTValue ...)
     * @param valueto
     * @param settableData
     */
    public abstract void encodeToSettable(VALUETO valueto, SettableData<?> settableData);

    abstract VALUETO encodeFromJavaInternal(VALUEFROM javaValue, Optional<CassandraOptions> cassandraOptions);

    /**
     * Encode the given raw Java object to CQL-compatible value using Achilles codec system
     * @param o
     * @return
     */
    public VALUETO encodeFromRaw(Object o) {
        return encodeFromRaw(o, Optional.empty());
    }

    /**
     * Encode given java raw object to CQL-compatible value using Achilles codec system and a CassandraOptions
     * containing a runtime SchemaNameProvider. Use the
     * <br/>
     * <br/>
     * <pre class="code"><code class="java">
     *     CassandraOptions.withSchemaNameProvider(SchemaNameProvider provider)
     * </code></pre>
     * <br/>
     * static method to build such a CassandraOptions instance
     * @param o
     * @param cassandraOptions
     * @return
     */
    public VALUETO encodeFromRaw(Object o, Optional<CassandraOptions> cassandraOptions) {
        if (o == null) return null;
        return encodeFromRawInternal(o, cassandraOptions);
    }

    abstract VALUETO encodeFromRawInternal(Object o, Optional<CassandraOptions> cassandraOptions);

    /**
     * Decode the given GettableData (Row, UDTValue, ...) to Java value value using Achilles codec system
     * @param gettableData
     * @return
     */
    public VALUEFROM decodeFromGettable(GettableData gettableData) {
        if (gettableData.isNull(NamingHelper.maybeQuote(getColumnForSelect())) && !isOptional()) return null;
        return decodeFromGettableInternal(gettableData);
    }

    abstract VALUEFROM decodeFromGettableInternal(GettableData gettableData);

    /**
     * Decode the given raw object to Java value value using Achilles codec system
     * @param o
     * @return
     */
    public VALUEFROM decodeFromRaw(Object o) {
        if (o == null && !isOptional()) return null;
        return decodeFromRawInternal(o);
    }

    abstract VALUEFROM decodeFromRawInternal(Object o);

    /**
     * Build the Java driver DataType of this column given a CassandraOptions
     * containing a runtime SchemaNameProvider. Use the
     * <br/>
     * <br/>
     * <pre class="code"><code class="java">
     *     CassandraOptions.withSchemaNameProvider(SchemaNameProvider provider)
     * </code></pre>
     * <br/>
     * static method to build such a CassandraOptions instance
     * @param cassandraOptions
     * @return
     */
    public abstract DataType buildType(Optional<CassandraOptions> cassandraOptions);

    abstract boolean isOptional();

    /**
     * Encode the field of the given entity into CQL-compatible value using Achilles codec system
     * @param entity
     * @return
     */
    public VALUETO encodeField(ENTITY entity) {
        return encodeField(entity, Optional.empty());
    }

    /**
     * Encode the field of the given entity into CQL-compatible value using Achilles codec system and a CassandraOptions
     * containing a runtime SchemaNameProvider. Use the
     * <br/>
     * <br/>
     * <pre class="code"><code class="java">
     *     CassandraOptions.withSchemaNameProvider(SchemaNameProvider provider)
     * </code></pre>
     * <br/>
     * static method to build such a CassandraOptions instance
     * @param entity
     * @return
     */
    public VALUETO encodeField(ENTITY entity, Optional<CassandraOptions> cassandraOptions) {
        return encodeFromJava(getJavaValue(entity), cassandraOptions);
    }

    /**
     * Get the raw value associated with this field from the given entity (non encoded)
     * @param entity
     * @return
     */
    public VALUEFROM getJavaValue(ENTITY entity) {
        return fieldInfo.getter.get(entity);
    }

    /**
     * <ol>
     *     <li>First extract all the values from the given entity</li>
     *     <li>Then encode each of the extracted value into CQL-compatible value using Achilles codec system</li>
     *     <li>Finally set the encoded value to the given UDTValue instance</li>
     * </ol>
     * @param entity
     * @param udtValue
     */
    public void encodeFieldToUdt(ENTITY entity, UDTValue udtValue) {
        encodeFieldToUdt(entity, udtValue, Optional.empty());
    }

    /**
     * <ol>
     *     <li>First extract all the values from the given entity</li>
     *     <li>Then encode each of the extracted value into CQL-compatible value using Achilles codec system and a CassandraOptions
     * containing a runtime SchemaNameProvider. Use the
     * <br/>
     * <pre class="code"><code class="java">
     *     CassandraOptions.withSchemaNameProvider(SchemaNameProvider provider)
     * </code></pre>
     * static method to build such a CassandraOptions instance</li>
     *     <li>Finally set the encoded value to the given UDTValue instance</li>
     * </ol>
     * @param entity
     * @param udtValue
     */
    public abstract void encodeFieldToUdt(ENTITY entity, UDTValue udtValue, Optional<CassandraOptions> cassandraOptions);

    public abstract boolean containsUDTProperty();

    public abstract List<AbstractUDTClassProperty<?>> getUDTClassProperties();

    /**
     * <ol>
     *     <li>First extract the column value from the given GettableData (Row, UDTValue, ...)</li>
     *     <li>Then call the setter on the given entity to set the value</li>
     * </ol>
     * @param gettableData
     * @param entity
     */
    public void decodeField(GettableData gettableData, ENTITY entity) {
        final VALUEFROM valuefrom = decodeFromGettable(gettableData);
        fieldInfo.setter.set(entity, valuefrom);
    }

    /**
     * Call the getter on the given entity to get the value
     * @param entity
     * @return
     */
    public VALUEFROM getFieldValue(ENTITY entity) {
        return fieldInfo.getter.get(entity);
    }

    public void setField(ENTITY entity, VALUEFROM value) {
        fieldInfo.setter.set(entity, value);
    }

    public String getColumnForSelect() {
        return fieldInfo.cqlColumn;
    }

    public DataType getDataType() {
        if (dataType == null) {
            dataType = buildType(Optional.empty());
        }
        return dataType;
    }

    @Override
    public void inject(SchemaNameProvider schemaNameProvider) {
        this.schemaNameProvider = Optional.ofNullable(schemaNameProvider);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AbstractProperty<?, ?, ?> that = (AbstractProperty<?, ?, ?>) o;
        return Objects.equals(fieldName, that.fieldName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldName);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AbstractProperty{");
        sb.append("fieldName='").append(fieldName).append('\'');
        sb.append(", valueFromTypeToken=").append(valueFromTypeToken);
        sb.append(", valueToTypeToken=").append(valueToTypeToken);
        sb.append('}');
        return sb.toString();
    }


}
