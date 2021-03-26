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

import static java.lang.Math.min;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.datastax.driver.core.utils.Bytes;

public abstract class LoggerHelper {
    public static final int HEX_STRING_LOG_LIMIT = 16;

    public static List<Object> replaceByteBuffersByHexString(Object... values) {
        ArrayList<Object> boundValues = new ArrayList<>(Arrays.asList(values));

        for (int valuePos = 0; valuePos < boundValues.size(); valuePos++) {
            Object boundValue = boundValues.get(valuePos);
            if (boundValue instanceof ByteBuffer) {
                ByteBuffer bbBoundedValue = (ByteBuffer) boundValue;
                byte[] firstBytes = new byte[min(bbBoundedValue.remaining(), HEX_STRING_LOG_LIMIT)];
                bbBoundedValue.get(firstBytes).rewind();
                boundValues.set(valuePos, toHexString(firstBytes, bbBoundedValue.remaining()));
            } else if (boundValue instanceof byte[]) {
                byte[] baBoundedValue = (byte[]) boundValue;
                byte[] firstBytes = baBoundedValue.length > HEX_STRING_LOG_LIMIT ? Arrays.copyOfRange(baBoundedValue, 0, HEX_STRING_LOG_LIMIT) : baBoundedValue;
                boundValues.set(valuePos, toHexString(firstBytes, baBoundedValue.length));
            }
        }
        return boundValues;
    }

    public static String toHexString(byte[] firstBytes, int originalLength) {
        return Bytes.toHexString(firstBytes) + more(originalLength);
    }

    private static String more(int length) {
        return ((length > HEX_STRING_LOG_LIMIT) ? String.format("... (%d)", length) : "");
    }
}
