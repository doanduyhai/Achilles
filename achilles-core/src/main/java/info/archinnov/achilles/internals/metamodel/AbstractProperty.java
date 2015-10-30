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

import java.util.Objects;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.GettableData;
import com.datastax.driver.core.SettableData;
import com.datastax.driver.core.UDTValue;
import com.google.common.reflect.TypeToken;

import info.archinnov.achilles.internals.injectable.InjectBeanFactory;
import info.archinnov.achilles.internals.injectable.InjectJacksonMapper;
import info.archinnov.achilles.internals.injectable.InjectTupleTypeFactory;
import info.archinnov.achilles.internals.injectable.InjectUserTypeFactory;
import info.archinnov.achilles.internals.metamodel.columns.FieldInfo;

public abstract class AbstractProperty<ENTITY, VALUEFROM, VALUETO>
        implements InjectTupleTypeFactory, InjectUserTypeFactory, InjectBeanFactory,
        InjectJacksonMapper {
    public final FieldInfo<ENTITY, VALUEFROM> fieldInfo;
    public final String fieldName;
    public TypeToken<VALUEFROM> valueFromTypeToken;
    public TypeToken<VALUETO> valueToTypeToken;
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

    public VALUETO encodeFromJava(VALUEFROM javaValue) {
        if (javaValue == null) return null;
        return encodeFromJavaInternal(javaValue);
    }

    public abstract void encodeToSettable(VALUETO valueto, SettableData<?> settableData);

    abstract VALUETO encodeFromJavaInternal(VALUEFROM javaValue);

    public VALUETO encodeFromRaw(Object o) {
        if (o == null) return null;
        return encodeFromRawInternal(o);
    }

    abstract VALUETO encodeFromRawInternal(Object o);

    public VALUEFROM decodeFromGettable(GettableData gettableData) {
        if (gettableData.isNull(getColumnForSelect())) return null;
        return decodeFromGettableInternal(gettableData);
    }

    abstract VALUEFROM decodeFromGettableInternal(GettableData gettableData);

    public VALUEFROM decodeFromRaw(Object o) {
        if (o == null) return null;
        return decodeFromRawInternal(o);
    }

    abstract VALUEFROM decodeFromRawInternal(Object o);

    public abstract DataType buildType();

    public VALUETO encodeField(ENTITY entity) {
        return encodeFromJava(getJavaValue(entity));
    }

    public VALUEFROM getJavaValue(ENTITY entity) {
        return fieldInfo.getter.get(entity);
    }

    public abstract void encodeFieldToUdt(ENTITY entity, UDTValue udtValue);

    public void decodeField(GettableData gettableData, ENTITY entity) {
        final VALUEFROM valuefrom = decodeFromGettable(gettableData);
        fieldInfo.setter.set(entity, valuefrom);
    }

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
            dataType = buildType();
        }
        return dataType;
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
