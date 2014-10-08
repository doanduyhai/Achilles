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
package info.archinnov.achilles.query.cql;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Row;
import info.archinnov.achilles.internal.context.DaoContext;
import info.archinnov.achilles.internal.statement.wrapper.NativeStatementWrapper;
import info.archinnov.achilles.type.Options;
import info.archinnov.achilles.type.OptionsBuilder;
import info.archinnov.achilles.type.TypedMap;

@RunWith(MockitoJUnitRunner.class)
public class NativeQueryTest {

    private NativeQuery query;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private DaoContext daoContext;

    @Mock
    private NativeQueryMapper mapper;

    @Mock
    private Row row;

    @Mock
    private RegularStatement regularStatement;

    private Object[] boundValues = new Object[] { 1 };

    @Captor
    private ArgumentCaptor<NativeStatementWrapper> simpleStatementCaptor;

    @Before
    public void setUp() {

        query = new NativeQuery(daoContext, regularStatement, OptionsBuilder.noOptions(), boundValues);
        Whitebox.setInternalState(query, NativeQueryMapper.class, mapper);
    }

    @Test
    public void should_get() throws Exception {
        List<Row> rows = Arrays.asList(row);
        when(daoContext.execute(any(NativeStatementWrapper.class)).all()).thenReturn(rows);

        List<TypedMap> result = new ArrayList<>();
        when(mapper.mapRows(rows)).thenReturn(result);

        List<TypedMap> actual = query.get();

        assertThat(actual).isSameAs(result);
    }

    @Test
    public void should_get_one() throws Exception {

        List<Row> rows = Arrays.asList(row);
        when(daoContext.execute(any(NativeStatementWrapper.class)).all()).thenReturn(rows);

        List<TypedMap> result = new ArrayList<>();
        TypedMap line = new TypedMap();
        result.add(line);
        when(mapper.mapRows(rows)).thenReturn(result);

        TypedMap actual = query.first();
        assertThat(actual).isSameAs(line);
    }

    @Test
    public void should_return_null_when_no_row() throws Exception {

        List<Row> rows = Arrays.asList(row);
        when(daoContext.execute(any(NativeStatementWrapper.class)).all()).thenReturn(rows);

        List<TypedMap> result = new ArrayList<>();
        when(mapper.mapRows(rows)).thenReturn(result);

        Map<String, Object> actual = query.first();
        assertThat(actual).isNull();
    }

    @Test
    public void should_execute_upserts() throws Exception {
        //Given
        final Options options = OptionsBuilder.ifNotExists();
        query.boundValues = boundValues;
        query.options = options;
        when(regularStatement.getQueryString()).thenReturn("queryString");

        //When
        query.execute();

        //Then
        verify(daoContext).execute(simpleStatementCaptor.capture());

        final NativeStatementWrapper actual = simpleStatementCaptor.getValue();
        assertThat(actual.getStatement().toString()).isEqualTo("queryString");
        assertThat(actual.getValues()).isEqualTo(boundValues);

    }

}
