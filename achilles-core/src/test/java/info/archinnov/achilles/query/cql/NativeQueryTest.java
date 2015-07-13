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

import static info.archinnov.achilles.internal.async.AsyncUtils.RESULTSET_TO_ROWS;
import static info.archinnov.achilles.internal.async.AsyncUtils.RESULTSET_TO_ROWS_WITH_EXECUTION_INFO;
import static org.fest.assertions.api.Assertions.anyOf;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;

import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.SimpleStatement;
import com.google.common.base.Optional;
import info.archinnov.achilles.internal.async.RowsWithExecutionInfo;
import info.archinnov.achilles.internal.persistence.operations.TypedMapIterator;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Row;
import com.google.common.base.Function;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import info.archinnov.achilles.async.AchillesFuture;
import info.archinnov.achilles.internal.async.AsyncUtils;
import info.archinnov.achilles.type.Empty;
import info.archinnov.achilles.internal.context.ConfigurationContext;
import info.archinnov.achilles.internal.context.DaoContext;
import info.archinnov.achilles.internal.persistence.operations.NativeQueryMapper;
import info.archinnov.achilles.internal.statement.wrapper.NativeStatementWrapper;
import info.archinnov.achilles.options.Options;
import info.archinnov.achilles.options.OptionsBuilder;
import info.archinnov.achilles.type.TypedMap;

@RunWith(MockitoJUnitRunner.class)
public class NativeQueryTest {

    private NativeQuery query;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private DaoContext daoContext;

    @Mock
    private ConfigurationContext configContext;

    @Mock
    private NativeQueryMapper mapper;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private AsyncUtils asyncUtils;

    @Mock
    private ExecutorService executorService;

    @Mock
    private Row row;

    @Mock
    private ListenableFuture<ResultSet> futureResultSet;

    @Mock
    private ListenableFuture<List<Row>> futureRows;

    @Mock
    private ListenableFuture<RowsWithExecutionInfo> futureRowsWithPaging;

    @Mock
    private ListenableFuture<TypedMapsWithPagingState> futureTypedMaps;

    @Mock
    private ListenableFuture<TypedMap> futureTypedMap;

    @Mock
    private ListenableFuture<Empty> futureEmpty;

    @Mock
    private AchillesFuture<TypedMapsWithPagingState> typedMapsWithPagingFuture;

    @Mock
    private AchillesFuture<TypedMap> achillesFutureTypedMap;

    @Mock
    private AchillesFuture<Empty> achillesFutureEmpty;

    @Captor
    private ArgumentCaptor<Function<RowsWithExecutionInfo, TypedMapsWithPagingState>> rowsToTypedMapsCaptor;

    @Captor
    private ArgumentCaptor<Function<List<Row>, TypedMap>> rowsToTypedMapCaptor;

    private FutureCallback<Object>[] asyncListeners = new FutureCallback[] { };

	 @Mock
    private RegularStatement regularStatement;
	
    @Captor
    private ArgumentCaptor<NativeStatementWrapper> nativeStatementCaptor;

    private Object[] boundValues = new Object[]{1};


    @Before
    public void setUp() {

        when(configContext.getExecutorService()).thenReturn(executorService);
        query = new NativeQuery(daoContext, configContext,regularStatement, OptionsBuilder.noOptions(), boundValues);
        query.asyncUtils = asyncUtils;
        query.mapper = mapper;
    }

    @Test
    public void should_get_async() throws Exception {
        // Given
        List<Row> rows = Arrays.asList(row);
        List<TypedMap> typedMaps = new ArrayList<>();

        when(daoContext.execute(any(NativeStatementWrapper.class))).thenReturn(futureResultSet);
        when(asyncUtils.transformFuture(futureResultSet, RESULTSET_TO_ROWS_WITH_EXECUTION_INFO)).thenReturn(futureRowsWithPaging);
        when(asyncUtils.transformFuture(eq(futureRowsWithPaging), rowsToTypedMapsCaptor.capture())).thenReturn(futureTypedMaps);

        when(mapper.mapRows(rows)).thenReturn(typedMaps);

        // When
        final ListenableFuture<TypedMapsWithPagingState> actual = query.asyncGetInternal(asyncListeners);

        // Then
        assertThat(actual).isSameAs(futureTypedMaps);
        verify(asyncUtils).maybeAddAsyncListeners(futureTypedMaps, asyncListeners);

        final Function<RowsWithExecutionInfo, TypedMapsWithPagingState> function = rowsToTypedMapsCaptor.getValue();
        final TypedMapsWithPagingState actualTypedMaps = function.apply(new RowsWithExecutionInfo(rows, mock(ExecutionInfo.class)));

        assertThat(actualTypedMaps.getTypedMaps()).isSameAs(typedMaps);
    }

    @Test
    public void should_get_first_async() throws Exception {
        // Given
        List<Row> rows = Arrays.asList(row);
        TypedMap typedMap = new TypedMap();
        List<TypedMap> typedMaps = new ArrayList<>();
        typedMaps.add(typedMap);

        when(daoContext.execute(any(NativeStatementWrapper.class))).thenReturn(futureResultSet);
        when(asyncUtils.transformFuture(futureResultSet, RESULTSET_TO_ROWS)).thenReturn(futureRows);
        when(asyncUtils.transformFuture(eq(futureRows), rowsToTypedMapCaptor.capture())).thenReturn(futureTypedMap);
        when(asyncUtils.buildInterruptible(futureTypedMap)).thenReturn(achillesFutureTypedMap);

        when(mapper.mapRows(rows)).thenReturn(typedMaps);

        // When
        final AchillesFuture<TypedMap> actual = query.asyncGetFirstInternal(asyncListeners);

        // Then
        assertThat(actual).isSameAs(achillesFutureTypedMap);
        verify(asyncUtils).maybeAddAsyncListeners(futureTypedMap, asyncListeners);

        final Function<List<Row>, TypedMap> function = rowsToTypedMapCaptor.getValue();
        final TypedMap actualTypedMap = function.apply(rows);
        assertThat(actualTypedMap).isSameAs(typedMap);

    }

    @Test
    public void should_return_null_when_no_row() throws Exception {
        // Given
        List<Row> rows = Arrays.asList(row);
        List<TypedMap> typedMaps = new ArrayList<>();

        when(daoContext.execute(any(NativeStatementWrapper.class))).thenReturn(futureResultSet);
        when(asyncUtils.transformFuture(futureResultSet, RESULTSET_TO_ROWS)).thenReturn(futureRows);
        when(asyncUtils.transformFuture(eq(futureRows), rowsToTypedMapCaptor.capture())).thenReturn(futureTypedMap);
        when(asyncUtils.buildInterruptible(futureTypedMap)).thenReturn(achillesFutureTypedMap);

        when(mapper.mapRows(rows)).thenReturn(typedMaps);

        // When
        final AchillesFuture<TypedMap> actual = query.asyncGetFirstInternal(asyncListeners);

        // Then
        assertThat(actual).isSameAs(achillesFutureTypedMap);
        verify(asyncUtils).maybeAddAsyncListeners(futureTypedMap, asyncListeners);

        final Function<List<Row>, TypedMap> function = rowsToTypedMapCaptor.getValue();
        final TypedMap actualTypedMap = function.apply(rows);
        assertThat(actualTypedMap).isNull();
    }

    @Test
    public void should_execute_async() throws Exception {
        //Given
        final Object[] boundValues = { "test" };
        final Options options = OptionsBuilder.ifNotExists();
        query = new NativeQuery(daoContext, configContext, regularStatement, options, boundValues);
        query.asyncUtils = asyncUtils;

        when(daoContext.execute(nativeStatementCaptor.capture())).thenReturn(futureResultSet);
        when(asyncUtils.transformFutureToEmpty(futureResultSet, executorService)).thenReturn(futureEmpty);
        when(asyncUtils.buildInterruptible(futureEmpty)).thenReturn(achillesFutureEmpty);
        when(regularStatement.getQueryString()).thenReturn("query");

        //When
        final AchillesFuture<Empty> actual = query.asyncExecuteInternal(asyncListeners);

        //Then
        assertThat(actual).isSameAs(achillesFutureEmpty);

        verify(asyncUtils).maybeAddAsyncListeners(futureEmpty, asyncListeners);

        final NativeStatementWrapper statementWrapper = nativeStatementCaptor.getValue();
        assertThat(statementWrapper.getStatement()).isInstanceOf(SimpleStatement.class);
        assertThat(((SimpleStatement) statementWrapper.getStatement()).getQueryString()).isEqualTo("query");
        assertThat(statementWrapper.getValues()).isEqualTo(boundValues);
    }

    @Test
    public void should_get_iterator() throws Exception {
        //Given
        ResultSet resultSet = mock(ResultSet.class);
        Iterator<Row> iterator = mock(Iterator.class);

        when(daoContext.execute(nativeStatementCaptor.capture())).thenReturn(futureResultSet);
        when(asyncUtils.buildInterruptible(futureResultSet).getImmediately()).thenReturn(resultSet);
        when(resultSet.iterator()).thenReturn(Arrays.asList(row).iterator());
        when(iterator.hasNext()).thenReturn(true);

        //When
        final Iterator<TypedMap> typedMapIterator = query.iterator();

        //Then
        assertThat(typedMapIterator.hasNext()).isTrue();
    }

    @Test
    public void should_get_async_iterator_without_fetch_size() throws Exception {
        //Given
        ListenableFuture<Iterator<TypedMap>> futureTypedMapIterator = mock(ListenableFuture.class);
        AchillesFuture<Iterator<TypedMap>> achillesFuture = mock(AchillesFuture.class);
        Iterator<TypedMap> typedMapIterator = mock(TypedMapIterator.class);

        when(daoContext.execute(nativeStatementCaptor.capture())).thenReturn(futureResultSet);
        when(asyncUtils.transformFuture(eq(futureResultSet), any(Function.class), eq(executorService))).thenReturn(futureTypedMapIterator);
        when(asyncUtils.buildInterruptible(futureTypedMapIterator)).thenReturn(achillesFuture);

        when(achillesFuture.get()).thenReturn(typedMapIterator);


        //When
        final AchillesFuture<Iterator<TypedMap>> actual = query.asyncIterator(Optional.<Integer>absent());

        //Then
        assertThat(actual.get()).isSameAs(typedMapIterator);
    }
}
