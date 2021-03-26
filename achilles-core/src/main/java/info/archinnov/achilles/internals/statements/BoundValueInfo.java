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

package info.archinnov.achilles.internals.statements;

import java.util.function.BiConsumer;

import com.datastax.driver.core.SettableData;

public class BoundValueInfo {

    public final BiConsumer<Object, SettableData> setter;
    public final Object boundValue;
    public final Object encodedValue;

    private BoundValueInfo(BiConsumer<Object, SettableData> setter, Object boundValue, Object encodedValue) {
        this.setter = setter;
        this.boundValue = boundValue;
        this.encodedValue = encodedValue;
    }

    public static BoundValueInfo of(BiConsumer<Object, SettableData> setter, Object boundValue, Object encodedValue) {
        return new BoundValueInfo(setter, boundValue, encodedValue);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("BoundValueInfo{");
        sb.append("boundValue=").append(boundValue);
        sb.append(", encodedValue=").append(encodedValue);
        sb.append('}');
        return sb.toString();
    }
}
