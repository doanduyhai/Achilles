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

package info.archinnov.achilles.internals.types;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class OverridingOptional<T> {

    private final List<Optional<T>> options = new ArrayList<>();
    private T defaultValue;

    private OverridingOptional(Optional<T> source) {
        options.add(source);
    }

    public static <T> OverridingOptional<T> from(Optional<T> source) {
        return new OverridingOptional<>(source);
    }

    public static <T> OverridingOptional<T> from(T nullableSource) {
        return new OverridingOptional<>(Optional.ofNullable(nullableSource));
    }

    public OverridingOptional<T> andThen(Optional<T> next) {
        options.add(next);
        return this;
    }

    public OverridingOptional<T> andThen(T nullableNext) {
        options.add(Optional.ofNullable(nullableNext));
        return this;
    }

    public Optional<T> getOptional() {
        return options
                .stream()
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst();
    }

    public OverridingOptionalFinal defaultValue(T defaultValue) {
        this.defaultValue = defaultValue;
        return new OverridingOptionalFinal();
    }

    public class OverridingOptionalFinal {

        private OverridingOptionalFinal() {
        }

        public T get() {
            final Optional<T> finalOption = options
                    .stream()
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .findFirst();

            return finalOption.orElse(defaultValue);
        }
    }

}
