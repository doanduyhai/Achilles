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

package com.datastax.driver.core;

import static java.lang.String.format;

import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UserTypeBuilder {

    private static final Logger LOGGER = LoggerFactory.getLogger(UserTypeBuilder.class);

    public static UserType buildUserType(ProtocolVersion version, CodecRegistry registry, String keyspace, String typeName, boolean frozen, Collection<UserType.Field> fields) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Creating UserType instance for UDT %s in keyspace %s", typeName, keyspace));
        }
        String plainKeyspace = keyspace.replaceAll("\"", "");
        String plainTypeName = typeName.replaceAll("\"", "");
        return new UserType(plainKeyspace, plainTypeName, frozen, fields, version, registry);
    }

    public static UserType.Field buildField(String name, DataType dataType) {
        String plainName = name.replaceAll("\"", "");
        return new UserType.Field(plainName, dataType);
    }
}
