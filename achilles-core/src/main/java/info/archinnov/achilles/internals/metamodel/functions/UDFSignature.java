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

package info.archinnov.achilles.internals.metamodel.functions;


import java.util.List;
import java.util.Objects;
import java.util.Optional;

import com.squareup.javapoet.TypeName;

import info.archinnov.achilles.internals.parser.TypeUtils;

public class UDFSignature {
    public final Optional<String> keyspace;
    public final TypeName sourceClass;
    public final String name;
    public final String methodName;
    public final TypeName returnType;
    public final List<TypeName> parameterTypes;
    public final List<UDFParamSignature> parameterSignatures;

    public UDFSignature(Optional<String> keyspace, TypeName sourceClass, String name, TypeName returnType, List<TypeName> parameterTypes,
                        List<UDFParamSignature> parameterSignatures) {
        this.keyspace = keyspace;
        this.sourceClass = sourceClass;
        this.name = name;
        this.methodName = name;
        this.returnType = returnType;
        this.parameterTypes = parameterTypes;
        this.parameterSignatures = parameterSignatures;
    }

    public UDFSignature(Optional<String> keyspace, TypeName sourceClass, String name, String methodName, TypeName returnType, List<TypeName> parameterTypes,
                        List<UDFParamSignature> parameterSignatures) {
        this.keyspace = keyspace;
        this.sourceClass = sourceClass;
        this.name = name;
        this.methodName = methodName;
        this.returnType = returnType;
        this.parameterTypes = parameterTypes;
        this.parameterSignatures = parameterSignatures;
    }

    public String getFunctionName() {
        return keyspace.isPresent() ? keyspace.get() + "." + name : name;
    }

    public TypeName returnTypeForFunctionParam() {
        return TypeUtils.determineTypeForFunctionParam(returnType);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UDFSignature that = (UDFSignature) o;
        return Objects.equals(keyspace, that.keyspace) &&
                Objects.equals(name.toLowerCase(), that.name.toLowerCase()) &&
                Objects.equals(returnType, that.returnType) &&
                Objects.equals(parameterTypes, that.parameterTypes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(keyspace, name.toLowerCase(), methodName.toLowerCase(), returnType, parameterTypes);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("UDFSignature{");
        sb.append("keyspace=").append(keyspace);
        sb.append(", sourceClass=").append(sourceClass);
        sb.append(", methodName='").append(methodName).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", returnType=").append(returnType);
        sb.append(", parameterTypes=").append(parameterTypes);
        sb.append('}');
        return sb.toString();
    }

    public static class UDFParamSignature {
        public final String name;
        public final TypeName typeName;

        public UDFParamSignature(TypeName typeName, String name) {
            this.typeName = typeName;
            this.name = name;
        }

        public TypeName typeForFunctionParam() {
            return TypeUtils.determineTypeForFunctionParam(this.typeName);
        }
    }
}
