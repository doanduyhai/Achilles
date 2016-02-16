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

import com.google.common.reflect.TypeToken;
import com.squareup.javapoet.TypeName;

public class FunctionSignature {
    public final String name;
    public final TypeName returnType;
    public final List<TypeName> parameterTypes;
    public final List<TypeToken<?>> parameterTypeTokens;

    public FunctionSignature(String name, TypeName returnType, List<TypeName> parameterTypes, List<TypeToken<?>> parameterTypeTokens) {
        this.name = name;
        this.returnType = returnType;
        this.parameterTypes = parameterTypes;
        this.parameterTypeTokens = parameterTypeTokens;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FunctionSignature that = (FunctionSignature) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(returnType, that.returnType) &&
                Objects.equals(parameterTypes, that.parameterTypes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, returnType, parameterTypes);
    }
}
