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

package info.archinnov.achilles.internals.metamodel.functions;


import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;

import info.archinnov.achilles.internals.context.ConfigurationContext;
import info.archinnov.achilles.internals.types.OverridingOptional;
import info.archinnov.achilles.validation.Validator;

public class FunctionProperty {
    public final Optional<String> keyspace;
    public final String name;
    public final String returnDataType;
    public final List<String> parameterDataTypes;

    public FunctionProperty(Optional<String> keyspace, String name, String returnDataType, List<String> parameterDataTypes) {
        this.keyspace = keyspace;
        this.name = name;
        this.returnDataType = returnDataType;
        this.parameterDataTypes = parameterDataTypes;
    }

    public void validate(ConfigurationContext configContext) {
        final Metadata metadata = configContext.getSession().getCluster().getMetadata();

        final Optional<String> definedKeyspace = OverridingOptional
                .from(keyspace)
                .andThen(configContext.getCurrentKeyspace())
                .getOptional();

        Validator.validateTrue(definedKeyspace.isPresent(), "No keyspace value defined on function '%s' annotation " +
                "nor at Achilles runtime", this.toString());

        final KeyspaceMetadata keyspaceMetadata = metadata.getKeyspace(definedKeyspace.get());

        final long matchingUDF = keyspaceMetadata
                .getFunctions()
                .stream()
                .filter(x -> x.getSimpleName().equalsIgnoreCase(this.name))
                .filter(x -> x.getReturnType().asFunctionParameterString().equalsIgnoreCase(this.returnDataType))
                .filter(x -> x.getArguments()
                        .entrySet()
                        .stream()
                        .map(entry -> entry.getValue().toString())
                        .collect(Collectors.toList())
                        .equals(this.parameterDataTypes))
                .count();

        final long matchingUDA = keyspaceMetadata
                .getAggregates()
                .stream()
                .filter(x -> x.getSimpleName().equalsIgnoreCase(this.name))
                .filter(x -> x.getReturnType().asFunctionParameterString().equalsIgnoreCase(this.returnDataType))
                .filter(x -> x.getArgumentTypes()
                        .stream()
                        .map(DataType::toString)
                        .collect(Collectors.toList())
                        .equals(this.parameterDataTypes))
                .count();

        Validator.validateBeanMappingFalse(matchingUDF > 1 && matchingUDA > 1, "Error: found an UDF and UDA with the same name and signature in keyspace '%s'",
                definedKeyspace.get());

        Validator.validateBeanMappingTrue(matchingUDA + matchingUDF == 1,
                "The declared function '%s' cannot be found in the runtime Cassandra keyspace '%s'",
                this.toString(), definedKeyspace.get());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FunctionProperty{");
        sb.append("keyspace=").append(keyspace);
        sb.append(", name='").append(name).append('\'');
        sb.append(", returnDataType='").append(returnDataType).append('\'');
        sb.append(", parameterDataTypes=").append(parameterDataTypes);
        sb.append('}');
        return sb.toString();
    }
}
