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
 * Tuple5
 */
public class Tuple5<A, B, C, D, E> implements Tuple {

    private final A _1;
    private final B _2;
    private final C _3;
    private final D _4;
    private final E _5;


    public Tuple5(A _1, B _2, C _3, D _4, E _5) {
        this._1 = _1;
        this._2 = _2;
        this._3 = _3;
        this._4 = _4;
        this._5 = _5;
    }

    public static <A, B, C, D, E> Tuple5<A, B, C, D, E> of(A _1, B _2, C _3, D _4, E _5) {
        return new Tuple5<>(_1, _2, _3, _4, _5);
    }

    public A _1() {
        return _1;
    }

    public B _2() {
        return _2;
    }

    public C _3() {
        return _3;
    }

    public D _4() {
        return _4;
    }

    public E _5() {
        return _5;
    }

    @Override
    public List<Object> values() {
        return Arrays.asList(_1, _2, _3, _4, _5);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Tuple5<?, ?, ?, ?, ?> tuple5 = (Tuple5<?, ?, ?, ?, ?>) o;
        return Objects.equals(_1, tuple5._1) &&
                Objects.equals(_2, tuple5._2) &&
                Objects.equals(_3, tuple5._3) &&
                Objects.equals(_4, tuple5._4) &&
                Objects.equals(_5, tuple5._5);
    }

    @Override
    public int hashCode() {
        return Objects.hash(_1, _2, _3, _4, _5);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Tuple5{");
        sb.append("_1=").append(_1);
        sb.append(", _2=").append(_2);
        sb.append(", _3=").append(_3);
        sb.append(", _4=").append(_4);
        sb.append(", _5=").append(_5);
        sb.append('}');
        return sb.toString();
    }
}
