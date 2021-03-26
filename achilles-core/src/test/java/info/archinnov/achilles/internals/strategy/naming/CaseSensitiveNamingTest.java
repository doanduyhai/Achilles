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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class CaseSensitiveNamingTest {

    InternalNamingStrategy strategy = new CaseSensitiveNaming();

    @Test
    public void should_return_blank_name() throws Exception {
        //Given
        String name = "     ";

        //When
        final String actual = strategy.apply(name);

        //Then
        assertThat(actual).isEmpty();
    }

    @Test
    public void should_return_blank_name_on_null() throws Exception {
        //Given
        String name = null;

        //When
        final String actual = strategy.apply(name);

        //Then
        assertThat(actual).isEmpty();
    }

    @Test
    public void should_return_case_sensitive() throws Exception {
        //Given
        String name = "dcSdIfk_sdf";

        //When
        final String actual = strategy.apply(name);

        //Then
        assertThat(actual).isEqualTo("\"dcSdIfk_sdf\"");
    }
}