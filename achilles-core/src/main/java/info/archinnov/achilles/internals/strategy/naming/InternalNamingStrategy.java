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

package info.archinnov.achilles.internals.strategy.naming;

import static java.util.Arrays.asList;
import static org.apache.commons.lang3.StringUtils.splitByCharacterTypeCamelCase;

import java.util.Optional;
import java.util.StringJoiner;

import info.archinnov.achilles.annotations.Strategy;
import info.archinnov.achilles.type.strategy.NamingStrategy;

public interface InternalNamingStrategy {

    static InternalNamingStrategy inferNamingStrategy(Optional<Strategy> strategy, InternalNamingStrategy defaultStrategy) {
        return strategy
                .map(x -> getNamingStrategy(x.naming()))
                .orElse(defaultStrategy);
    }

    static InternalNamingStrategy getNamingStrategy(NamingStrategy namingStrategy) {
        switch (namingStrategy) {
            case SNAKE_CASE:
                return new SnakeCaseNaming();
            case CASE_SENSITIVE:
                return new CaseSensitiveNaming();
            default:
                return new LowerCaseNaming();
        }
    }

    String apply(String name);

    String FQCN();

    default String toCaseSensitive(String name) {
        if (name.equals(name.toLowerCase())) return name;
        else return "\"" + name + "\"";
    }

    default String toSnakeCase(String name) {
        final String[] tokens = splitByCharacterTypeCamelCase(name);
        final StringJoiner joiner = new StringJoiner("_");
        asList(tokens)
                .stream()
                .filter(x -> x != null)
                .map(x -> x.toLowerCase())
                .forEach(joiner::add);

        return joiner.toString().replaceAll("_+", "_");
    }
}
