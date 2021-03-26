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

package info.archinnov.achilles.internals.parser.context;


import static info.archinnov.achilles.internals.parser.TypeUtils.*;
import static java.util.stream.Collectors.toList;

import java.util.*;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.TypeName;

import info.archinnov.achilles.internals.parser.TypeUtils;

public class FunctionSignature {
    public final Optional<String> keyspace;
    public final TypeName sourceClass;
    public final String name;
    public final String methodName;
    public final TypeName sourceReturnType;
    public final List<TypeName> sourceParameterTypes;
    public final FunctionParamSignature returnTypeSignature;
    public final List<FunctionParamSignature> parameterSignatures;

    public FunctionSignature(Optional<String> keyspace, TypeName sourceClass, String name, String methodName, FunctionParamSignature returnTypeSignature,
                             List<FunctionParamSignature> parameterSignatures) {
        this.keyspace = keyspace;
        this.sourceClass = sourceClass;
        this.name = name;
        this.methodName = methodName;
        this.returnTypeSignature = returnTypeSignature;
        this.sourceReturnType = returnTypeSignature.sourceTypeName;
        this.parameterSignatures = parameterSignatures;
        this.sourceParameterTypes = parameterSignatures.stream().map(x -> x.sourceTypeName).collect(toList());
    }

    public FunctionSignature(Optional<String> keyspace, TypeName sourceClass, String name, FunctionParamSignature returnTypeSignature,
                             List<FunctionParamSignature> parameterSignatures) {
        this.keyspace = keyspace;
        this.sourceClass = sourceClass;
        this.name = name;
        this.methodName = name;
        this.returnTypeSignature = returnTypeSignature;
        this.sourceReturnType = returnTypeSignature.sourceTypeName;
        this.parameterSignatures = parameterSignatures;
        this.sourceParameterTypes = parameterSignatures.stream().map(x -> x.sourceTypeName).collect(toList());
    }

    public String getFunctionName() {
        return keyspace.isPresent() ? keyspace.get() + "." + name : name;
    }

    public TypeName returnTypeForFunctionParam() {
        return TypeUtils.determineTypeForFunctionParam(returnTypeSignature.sourceTypeName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FunctionSignature that = (FunctionSignature) o;
        return Objects.equals(keyspace, that.keyspace) &&
                Objects.equals(name.toLowerCase(), that.name.toLowerCase()) &&
                Objects.equals(sourceReturnType, that.sourceReturnType) &&
                Objects.equals(sourceParameterTypes, that.sourceParameterTypes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(keyspace, name.toLowerCase(), methodName.toLowerCase(), sourceReturnType, sourceParameterTypes);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("UDFSignature{");
        sb.append("keyspace=").append(keyspace);
        sb.append(", sourceClass=").append(sourceClass);
        sb.append(", methodName='").append(methodName).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", returnType=").append(sourceReturnType);
        sb.append(", sourceParameterTypes=").append(sourceParameterTypes);
        sb.append('}');
        return sb.toString();
    }

    public static class FunctionParamSignature {
        public final String name;
        public final TypeName sourceTypeName;
        public final TypeName targetCQLTypeName;
        public final String targetCQLDataType;


        public static FunctionParamSignature VOID() {
            return new FunctionParamSignature("returnType", TypeName.VOID, TypeName.VOID, "");
        }

        public FunctionParamSignature(String name, TypeName sourceTypeName, TypeName targetCQLTypeName, String targetCQLDataType) {
            this.name = name;
            this.sourceTypeName = sourceTypeName;
            this.targetCQLTypeName = targetCQLTypeName;
            this.targetCQLDataType = targetCQLDataType;
        }

        public TypeName typeForFunctionParam() {
            return TypeUtils.determineTypeForFunctionParam(this.sourceTypeName);
        }

        public static FunctionParamSignature tupleType(String paramName, ClassName tupleTypeName, FunctionParamSignature... signatures) {
            final List<TypeName> typeNames = Arrays.stream(signatures)
                    .map(x -> x.sourceTypeName)
                    .collect(toList());
            final StringJoiner joiner = new StringJoiner(", ", "frozen<tuple<", ">>");

            Arrays.stream(signatures)
                    .forEach(x -> joiner.add(x.targetCQLDataType));

            return new FunctionParamSignature(paramName, genericType(tupleTypeName, typeNames.toArray(new TypeName[typeNames.size()])), JAVA_DRIVER_TUPLE_VALUE_TYPE, joiner.toString());
        }

        public static FunctionParamSignature listType(String paramName, FunctionParamSignature nestedSignature, boolean isFrozen) {
            if (isFrozen) {
                return new FunctionParamSignature(paramName, genericType(LIST, nestedSignature.sourceTypeName), genericType(LIST, nestedSignature.targetCQLTypeName), "frozen<list<" + nestedSignature.targetCQLDataType + ">>");
            } else {
                return new FunctionParamSignature(paramName, genericType(LIST, nestedSignature.sourceTypeName), genericType(LIST, nestedSignature.targetCQLTypeName), "list<" + nestedSignature.targetCQLDataType + ">");
            }
        }

        public static FunctionParamSignature setType(String paramName, FunctionParamSignature nestedSignature, boolean isFrozen) {
            if (isFrozen) {
                return new FunctionParamSignature(paramName, genericType(SET, nestedSignature.sourceTypeName), genericType(SET, nestedSignature.targetCQLTypeName), "frozen<set<" + nestedSignature.targetCQLDataType + ">>");
            } else {
                return new FunctionParamSignature(paramName, genericType(SET, nestedSignature.sourceTypeName), genericType(SET, nestedSignature.targetCQLTypeName), "set<" + nestedSignature.targetCQLDataType + ">");
            }
        }

        public static FunctionParamSignature mapType(String paramName, FunctionParamSignature keySignature, FunctionParamSignature valueSignature, boolean isFrozen) {
            if (isFrozen) {
                return new FunctionParamSignature(paramName,
                        genericType(MAP, keySignature.sourceTypeName, valueSignature.sourceTypeName),
                        genericType(MAP, keySignature.targetCQLTypeName, valueSignature.targetCQLTypeName),
                        "frozen<map<" + keySignature.targetCQLDataType + ", " + valueSignature.targetCQLDataType + ">>");
            } else {
                return new FunctionParamSignature(paramName,
                        genericType(MAP, keySignature.sourceTypeName, valueSignature.sourceTypeName),
                        genericType(MAP, keySignature.targetCQLTypeName, valueSignature.targetCQLTypeName),
                        "map<" + keySignature.targetCQLDataType + ", " + valueSignature.targetCQLDataType + ">");
            }
        }

        public static FunctionParamSignature optionalType(String paramName, FunctionParamSignature nestedSignature) {
            return new FunctionParamSignature(paramName,  genericType(OPTIONAL, nestedSignature.sourceTypeName), nestedSignature.targetCQLTypeName, nestedSignature.targetCQLDataType);
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("UDFParamSignature{");
            sb.append("name='").append(name).append('\'');
            sb.append(", sourceTypeName=").append(sourceTypeName);
            sb.append(", targetCQLTypeName=").append(targetCQLTypeName);
            sb.append(", targetCQLDataType='").append(targetCQLDataType).append('\'');
            sb.append('}');
            return sb.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FunctionParamSignature that = (FunctionParamSignature) o;
            return Objects.equals(name, that.name) &&
                    Objects.equals(sourceTypeName, that.sourceTypeName) &&
                    Objects.equals(targetCQLTypeName, that.targetCQLTypeName) &&
                    Objects.equals(targetCQLDataType, that.targetCQLDataType);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, sourceTypeName, targetCQLTypeName, targetCQLDataType);
        }
    }
}
