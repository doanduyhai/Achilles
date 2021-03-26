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
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toList;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TupleTypeBuilder {

    private static final Logger LOGGER = LoggerFactory.getLogger(TupleTypeBuilder.class);

    public static TupleType of(ProtocolVersion version, CodecRegistry registry, DataType... dataTypes) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Create TupleType instance for data types %s", stream(dataTypes).map(DataType::toString).collect(toList())));
        }
        return new TupleType(Arrays.asList(dataTypes), version, registry);
    }
}
