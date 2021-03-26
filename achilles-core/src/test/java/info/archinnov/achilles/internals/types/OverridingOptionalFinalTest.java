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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;

import org.junit.Test;

public class OverridingOptionalFinalTest {

    @Test
    public void should_get_first_present_value() throws Exception {
        //When
        final String actual = OverridingOptional
                .from(Optional.<String>empty())
                .andThen(Optional.<String>empty())
                .andThen(Optional.ofNullable("value"))
                .defaultValue("default")
                .get();

        //Then
        assertThat(actual).isEqualTo("value");
    }


    @Test
    public void should_get_first_value() throws Exception {
        //When
        final String actual = OverridingOptional
                .from(Optional.ofNullable("first"))
                .andThen(Optional.<String>empty())
                .andThen(Optional.ofNullable("value"))
                .defaultValue("default")
                .get();

        //Then
        assertThat(actual).isEqualTo("first");
    }

    @Test
    public void should_get_default_value() throws Exception {
        //When
        final String actual = OverridingOptional
                .from(Optional.<String>empty())
                .andThen(Optional.<String>empty())
                .andThen(Optional.ofNullable(null))
                .defaultValue("default")
                .get();

        //Then
        assertThat(actual).isEqualTo("default");
    }
}