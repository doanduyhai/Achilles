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

package info.archinnov.achilles.internals.utils;

import static info.archinnov.achilles.internals.parser.TypeUtils.*;
import static java.lang.String.format;

import java.util.StringJoiner;

import com.squareup.javapoet.ArrayTypeName;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;

public class TypeNameHelper {

    public static String asString(TypeName type) throws IllegalStateException {
        if (type.isPrimitive()) {
            return type.box().toString().replaceAll("java\\.lang\\.","");
        } else if (type instanceof ParameterizedTypeName) {
            final ParameterizedTypeName parameterizedTypeName = (ParameterizedTypeName) type;
            final ClassName rawType = parameterizedTypeName.rawType;
            if (rawType.equals(LIST)) {
                return "List_" + asString(parameterizedTypeName.typeArguments.get(0));
            } else if (rawType.equals(SET)) {
                return "Set_" + asString(parameterizedTypeName.typeArguments.get(0));
            } else if (rawType.equals(MAP)) {
                return "Map_" + asString(parameterizedTypeName.typeArguments.get(0)) +"_" +asString(parameterizedTypeName.typeArguments.get(1));
            } else if (rawType.equals(TUPLE1)) {
                return "Tuple1_" +asString(parameterizedTypeName.typeArguments.get(0));
            } else if (rawType.equals(TUPLE2)) {
                StringJoiner joiner = new StringJoiner("_");
                parameterizedTypeName.typeArguments.forEach(x -> joiner.add(asString(x)));
                return "Tuple2_" + joiner.toString();
            } else if (rawType.equals(TUPLE3)) {
                StringJoiner joiner = new StringJoiner("_");
                parameterizedTypeName.typeArguments.forEach(x -> joiner.add(asString(x)));
                return "Tuple3_" + joiner.toString();
            } else if (rawType.equals(TUPLE4)) {
                StringJoiner joiner = new StringJoiner("_");
                parameterizedTypeName.typeArguments.forEach(x -> joiner.add(asString(x)));
                return "Tuple4_" + joiner.toString();
            } else if (rawType.equals(TUPLE5)) {
                StringJoiner joiner = new StringJoiner("_");
                parameterizedTypeName.typeArguments.forEach(x -> joiner.add(asString(x)));
                return "Tuple5_" + joiner.toString();
            } else if (rawType.equals(TUPLE6)) {
                StringJoiner joiner = new StringJoiner("_");
                parameterizedTypeName.typeArguments.forEach(x -> joiner.add(asString(x)));
                return "Tuple6_" + joiner.toString();
            } else if (rawType.equals(TUPLE7)) {
                StringJoiner joiner = new StringJoiner("_");
                parameterizedTypeName.typeArguments.forEach(x -> joiner.add(asString(x)));
                return "Tuple7_" + joiner.toString();
            } else if (rawType.equals(TUPLE8)) {
                StringJoiner joiner = new StringJoiner("_");
                parameterizedTypeName.typeArguments.forEach(x -> joiner.add(asString(x)));
                return "Tuple8_" + joiner.toString();
            } else if (rawType.equals(TUPLE9)) {
                StringJoiner joiner = new StringJoiner("_");
                parameterizedTypeName.typeArguments.forEach(x -> joiner.add(asString(x)));
                return "Tuple9_" + joiner.toString();
            } else if (rawType.equals(TUPLE10)) {
                StringJoiner joiner = new StringJoiner("_");
                parameterizedTypeName.typeArguments.forEach(x -> joiner.add(asString(x)));
                return "Tuple10_" + joiner.toString();
            } else if (rawType.equals(OPTIONAL)) {
                return "Optional_" + asString(parameterizedTypeName.typeArguments.get(0));
            } else {
                throw new IllegalStateException(format("Cannot extract name from unexpected type '%s'", type));
            }
        } else if (type instanceof ArrayTypeName) {
            final ArrayTypeName arrayTypeName = (ArrayTypeName) type;
            final TypeName componentType = arrayTypeName.componentType;
            if (componentType.isPrimitive()) {
                return "Array_Primitive_" + componentType.toString().replaceAll("java\\.lang\\.","");
            } else {
                return "Array_" + asString(componentType);
            }
        } else if (type instanceof ClassName) {
            if (type.equals(JAVA_DRIVER_LOCAL_DATE)) {
                return "DriverLocalDate";
            } else if (type.equals(JAVA_TIME_LOCAL_DATE)) {
                return "JavaTimeLocalDate";
            } else {
                final ClassName className = (ClassName) type;
                return className.simpleName();
            }
        } else {
            throw new IllegalStateException(format("Cannot extract name from unexpected type '%s'", type));
        }
    }
}
