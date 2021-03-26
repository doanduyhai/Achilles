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

package info.archinnov.achilles.internals.parser.validator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.TupleValue;
import com.squareup.javapoet.TypeName;

import info.archinnov.achilles.internals.apt.AptUtils;
import info.archinnov.achilles.internals.parser.TypeUtils;
import info.archinnov.achilles.type.tuples.Tuple1;
import info.archinnov.achilles.type.tuples.Tuple2;
import info.archinnov.achilles.type.tuples.Tuple3;

@RunWith(MockitoJUnitRunner.class)
public class TypeValidatorTest {

    @Captor
    ArgumentCaptor<String> messageCaptor;
    @Captor
    ArgumentCaptor<Object> objectCaptor;
    @Mock
    private AptUtils aptUtils;

    private TypeValidator typeValidator = new TypeValidator() {
        @Override
        public List<TypeName> getAllowedTypes() {
            return TypeUtils.ALLOWED_TYPES_3_10;
        }
    };

    @Test
    public void should_validate_primitiveByte() throws Exception {
        final TypeName typeName = TypeName.get(TestTypes.class.getDeclaredField("primitiveByte").getGenericType());
        typeValidator.validateAllowedTypes(aptUtils, typeName, typeName);
    }

    @Test
    public void should_validate_objectByte() throws Exception {
        final TypeName typeName = TypeName.get(TestTypes.class.getDeclaredField("objectByte").getGenericType());
        typeValidator.validateAllowedTypes(aptUtils, typeName, typeName);
        verify(aptUtils).validateTrue(eq(true), anyString(), anyVararg());
    }

    @Test
    public void should_validate_primitiveByteArray() throws Exception {
        final TypeName typeName = TypeName.get(TestTypes.class.getDeclaredField("primitiveByteArray").getGenericType());
        typeValidator.validateAllowedTypes(aptUtils, typeName, typeName);
        verify(aptUtils).validateTrue(eq(true), anyString(), anyVararg());
    }

    @Test
    public void should_validate_objectByteArray() throws Exception {
        final TypeName typeName = TypeName.get(TestTypes.class.getDeclaredField("objectByteArray").getGenericType());
        typeValidator.validateAllowedTypes(aptUtils, typeName, typeName);
        verify(aptUtils).validateTrue(eq(true), anyString(), anyVararg());
    }

    @Test
    public void should_validate_listString() throws Exception {
        final TypeName typeName = TypeName.get(TestTypes.class.getDeclaredField("listString").getGenericType());
        typeValidator.validateAllowedTypes(aptUtils, typeName, typeName);
        verify(aptUtils, times(2)).validateTrue(eq(true), anyString(), anyVararg());
    }

    @Test
    public void should_validate_setByte() throws Exception {
        final TypeName typeName = TypeName.get(TestTypes.class.getDeclaredField("setByte").getGenericType());
        typeValidator.validateAllowedTypes(aptUtils, typeName, typeName);
        verify(aptUtils, times(2)).validateTrue(eq(true), anyString(), anyVararg());
    }

    @Test
    public void should_validate_mapByteArray() throws Exception {
        final TypeName typeName = TypeName.get(TestTypes.class.getDeclaredField("mapByteArray").getGenericType());
        typeValidator.validateAllowedTypes(aptUtils, typeName, typeName);
        verify(aptUtils, times(3)).validateTrue(eq(true), anyString(), anyVararg());
    }

    @Test
    public void should_validate_tuple1() throws Exception {
        final TypeName typeName = TypeName.get(TestTypes.class.getDeclaredField("tuple1").getGenericType());
        typeValidator.validateAllowedTypes(aptUtils, typeName, typeName);
        verify(aptUtils, times(2)).validateTrue(eq(true), anyString(), anyVararg());
    }

    @Test
    public void should_validate_tuple2() throws Exception {
        final TypeName typeName = TypeName.get(TestTypes.class.getDeclaredField("tuple2").getGenericType());
        typeValidator.validateAllowedTypes(aptUtils, typeName, typeName);
        verify(aptUtils, times(4)).validateTrue(eq(true), anyString(), anyVararg());
    }

    @Test
    public void should_validate_tuple3() throws Exception {
        final TypeName typeName = TypeName.get(TestTypes.class.getDeclaredField("tuple3").getGenericType());
        typeValidator.validateAllowedTypes(aptUtils, typeName, typeName);
        verify(aptUtils, times(4)).validateTrue(eq(true), anyString(), anyVararg());
    }

    @Test
    public void should_validate_tupleValue() throws Exception {
        final TypeName typeName = TypeName.get(TestTypes.class.getDeclaredField("tupleValue").getGenericType());
        typeValidator.validateAllowedTypes(aptUtils, typeName, typeName);
        verify(aptUtils, times(1)).validateTrue(eq(true), anyString(), anyVararg());
    }

    @Test
    public void should_fail_on_invalidType() throws Exception {
        final TypeName typeName = TypeName.get(TestTypes.class.getDeclaredField("invalidType").getGenericType());
        typeValidator.validateAllowedTypes(aptUtils, typeName, typeName);
        verify(aptUtils, times(1)).validateTrue(eq(false), messageCaptor.capture(), objectCaptor.capture(), objectCaptor.capture());

        assertThat(messageCaptor.getValue()).isEqualTo("Type '%s' in '%s' is not a valid type for CQL");
        assertThat(objectCaptor.getAllValues()).containsExactly(typeName.toString(), typeName.toString());
    }

    @Test
    public void should_fail_on_invalidTypeNestedType() throws Exception {
        String consistencyLevel = ConsistencyLevel.class.getCanonicalName();
        final TypeName typeName = TypeName.get(TestTypes.class.getDeclaredField("invalidTypeNestedType").getGenericType());
        typeValidator.validateAllowedTypes(aptUtils, typeName, typeName);
        verify(aptUtils, times(3)).validateTrue(eq(true), anyString(), anyVararg());
        verify(aptUtils, times(1)).validateTrue(eq(false), messageCaptor.capture(), objectCaptor.capture(), objectCaptor.capture());

        assertThat(messageCaptor.getValue()).isEqualTo("Type '%s' in '%s' is not a valid type for CQL");
        assertThat(objectCaptor.getAllValues()).containsExactly(consistencyLevel, typeName.toString());
    }

    @Test
    public void should_fail_on_invalidUpperBound() throws Exception {
        String consistencyLevel = ConsistencyLevel.class.getCanonicalName();
        final TypeName typeName = TypeName.get(TestTypes.class.getDeclaredField("invalidUpperBound").getGenericType());
        typeValidator.validateAllowedTypes(aptUtils, typeName, typeName);
        verify(aptUtils, times(1)).validateTrue(eq(true), anyString(), anyVararg());
        verify(aptUtils, times(1)).validateTrue(eq(false), messageCaptor.capture(), objectCaptor.capture(), objectCaptor.capture());

        assertThat(messageCaptor.getValue()).isEqualTo("Type '%s' in '%s' is not a valid type for CQL");
        assertThat(objectCaptor.getAllValues()).containsExactly(consistencyLevel, typeName.toString());
    }

    @Test
    public void should_fail_on_invalidLowerBound() throws Exception {
        String object = Object.class.getCanonicalName();
        final TypeName typeName = TypeName.get(TestTypes.class.getDeclaredField("invalidLowerBound").getGenericType());
        typeValidator.validateAllowedTypes(aptUtils, typeName, typeName);
        verify(aptUtils, times(1)).validateTrue(eq(true), anyString(), anyVararg());
        verify(aptUtils, times(1)).validateTrue(eq(false), messageCaptor.capture(), objectCaptor.capture(), objectCaptor.capture());

        assertThat(messageCaptor.getValue()).isEqualTo("Type '%s' in '%s' is not a valid type for CQL");
        assertThat(objectCaptor.getAllValues()).containsExactly(object, typeName.toString());
    }

    @Test
    public void should_fail_on_invalidWildCard() throws Exception {
        String object = Object.class.getCanonicalName();
        final TypeName typeName = TypeName.get(TestTypes.class.getDeclaredField("invalidWildCard").getGenericType());
        typeValidator.validateAllowedTypes(aptUtils, typeName, typeName);
        verify(aptUtils, times(1)).validateTrue(eq(true), anyString(), anyVararg());
        verify(aptUtils, times(1)).validateTrue(eq(false), messageCaptor.capture(), objectCaptor.capture(), objectCaptor.capture());

        assertThat(messageCaptor.getValue()).isEqualTo("Type '%s' in '%s' is not a valid type for CQL");
        assertThat(objectCaptor.getAllValues()).containsExactly(object, typeName.toString());
    }

    public static class TestTypes {

        private byte primitiveByte;
        private Byte objectByte;
        private byte[] primitiveByteArray;
        private Byte[] objectByteArray;
        private List<String> listString;
        private Set<Byte> setByte;
        private Map<Integer, Byte[]> mapByteArray;
        private Tuple1<String> tuple1;
        private Tuple2<List<Integer>, Byte[]> tuple2;
        private Tuple3<? extends Date, Integer, Byte> tuple3;
        private TupleValue tupleValue;

        private ConsistencyLevel invalidType;
        private List<Map<Integer, ConsistencyLevel>> invalidTypeNestedType;
        private List<? extends ConsistencyLevel> invalidUpperBound;
        private List<? super String> invalidLowerBound;
        private List<?> invalidWildCard;
    }

}