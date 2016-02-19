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

package info.archinnov.achilles.internals.utils;

import static info.archinnov.achilles.internals.parser.TypeUtils.LIST;
import static info.archinnov.achilles.internals.parser.TypeUtils.MAP;
import static info.archinnov.achilles.internals.parser.TypeUtils.SET;
import static java.lang.String.format;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;

import info.archinnov.achilles.internals.parser.TypeUtils;

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
            } else {
               throw new IllegalStateException(format("Cannot extract name from unexpected type '%s'", type));
            }
        } else if (type instanceof ClassName) {
            final ClassName className = (ClassName) type;
            return className.simpleName();
        } else {
            throw new IllegalStateException(format("Cannot extract name from unexpected type '%s'", type));
        }
    }
}
