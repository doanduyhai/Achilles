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

package info.archinnov.achilles.type.tuples;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Tuple1
 */
public class Tuple1<A> implements Tuple {

    private final A _1;

    public Tuple1(A _1) {
        this._1 = _1;
    }

    public static <A> Tuple1<A> of(A _1) {
        return new Tuple1<>(_1);
    }

    public A _1() {
        return _1;
    }

    @Override
    public List<Object> values() {
        return Arrays.asList(_1);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Tuple1<?> tuple1 = (Tuple1<?>) o;
        return Objects.equals(_1, tuple1._1);
    }

    @Override
    public int hashCode() {
        return Objects.hash(_1);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Tuple1{");
        sb.append("_1=").append(_1);
        sb.append('}');
        return sb.toString();
    }
}
