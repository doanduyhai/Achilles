/*
 * Copyright (C) 2012-2014 DuyHai DOAN
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package info.archinnov.achilles.type;

import java.io.IOException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import info.archinnov.achilles.type.Counter;
import info.archinnov.achilles.type.CounterBuilder;


public final class CounterDeserializer extends JsonDeserializer<Counter> {

    @Override
    public Counter deserialize(JsonParser parser, DeserializationContext context) throws IOException {

        Counter counter = null;
        String value = parser.getText();
        if (value != null && !"".equals(value.trim())) {
            try {
                counter = CounterBuilder.incr(Long.parseLong(value));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Cannot deserialize '" + value + "' as Long for Counter ");
            }
        }
        return counter;
    }
}
