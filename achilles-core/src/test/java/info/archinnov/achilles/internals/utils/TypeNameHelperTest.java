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

import static info.archinnov.achilles.internals.parser.TypeUtils.*;
import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;

import org.junit.Test;

import com.datastax.driver.core.LocalDate;
import com.squareup.javapoet.TypeName;

public class TypeNameHelperTest {

    @Test
    public void should_convert_type_name_to_string() throws Exception {
        //Given
        final TypeName nativeBool = TypeName.get(boolean.class);
        final TypeName objectLong = TypeName.get(Long.class);
        final TypeName localDate = TypeName.get(LocalDate.class);
        final TypeName bigDecimal = TypeName.get(BigDecimal.class);
        final TypeName listString = genericType(LIST, STRING);
        final TypeName mapStringTuple = genericType(MAP, STRING, JAVA_DRIVER_TUPLE_VALUE_TYPE);
        final TypeName mapStringListUDT = genericType(MAP, STRING, genericType(LIST, JAVA_DRIVER_UDT_VALUE_TYPE));


        //When
        assertThat(TypeNameHelper.asString(nativeBool)).isEqualTo("Boolean");
        assertThat(TypeNameHelper.asString(objectLong)).isEqualTo("Long");
        assertThat(TypeNameHelper.asString(localDate)).isEqualTo("DriverLocalDate");
        assertThat(TypeNameHelper.asString(bigDecimal)).isEqualTo("BigDecimal");
        assertThat(TypeNameHelper.asString(listString)).isEqualTo("List_String");
        assertThat(TypeNameHelper.asString(mapStringTuple)).isEqualTo("Map_String_TupleValue");
        assertThat(TypeNameHelper.asString(mapStringListUDT)).isEqualTo("Map_String_List_UDTValue");

        //Then
    
    }

}