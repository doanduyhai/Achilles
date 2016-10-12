package info.archinnov.achilles.query.cql;

import com.datastax.driver.core.PagingState;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import info.archinnov.achilles.async.AchillesFuture;
import info.archinnov.achilles.internal.async.AsyncUtils;
import info.archinnov.achilles.internal.async.RowsWithExecutionInfo;
import info.archinnov.achilles.internal.context.ConfigurationContext;
import info.archinnov.achilles.internal.context.DaoContext;
import info.archinnov.achilles.internal.persistence.operations.NativeQueryMapper;
import info.archinnov.achilles.internal.persistence.operations.TypedMapIterator;
import info.archinnov.achilles.internal.statement.wrapper.NativeQueryLog;
import info.archinnov.achilles.internal.statement.wrapper.NativeStatementWrapper;
import info.archinnov.achilles.type.Empty;
import info.archinnov.achilles.options.Options;
import info.archinnov.achilles.type.TypedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static info.archinnov.achilles.internal.async.AsyncUtils.RESULTSET_TO_ROWS;
import static info.archinnov.achilles.internal.async.AsyncUtils.RESULTSET_TO_ROWS_WITH_EXECUTION_INFO;

public abstract class AbstractNativeQuery<T extends AbstractNativeQuery<T>> {

    private static final Logger log = LoggerFactory.getLogger(AbstractNativeQuery.class);

    protected NativeStatementWrapper nativeStatementWrapper;

    protected DaoContext daoContext;
    protected AsyncUtils asyncUtils = AsyncUtils.Singleton.INSTANCE.get();
    protected NativeQueryMapper mapper = NativeQueryMapper.Singleton.INSTANCE.get();
    protected Optional<PagingState> pagingStateO = Optional.absent();
    protected ExecutorService executorService;

    protected AbstractNativeQuery(DaoContext daoContext, ConfigurationContext configContext, Statement statement, Options options, Object... boundValues) {
        this.daoContext = daoContext;
        this.nativeStatementWrapper = new NativeStatementWrapper(daoContext, NativeQueryLog.class, statement, boundValues, options.getLWTResultListener());
        this.executorService = configContext.getExecutorService();
    }

    protected abstract T getThis();

    protected ListenableFuture<TypedMapsWithPagingState> asyncGetInternal(FutureCallback<Object>... asyncListeners) {
        log.debug("Get results for native query '{}' asynchronously", nativeStatementWrapper.getStatement());

        if (pagingStateO.isPresent()) {
            nativeStatementWrapper.setPagingState(pagingStateO.get());
        }

        final ListenableFuture<ResultSet> resultSetFuture = daoContext.execute(nativeStatementWrapper);

        final ListenableFuture<RowsWithExecutionInfo> futureRows = asyncUtils.transformFuture(resultSetFuture, RESULTSET_TO_ROWS_WITH_EXECUTION_INFO);

        Function<RowsWithExecutionInfo, TypedMapsWithPagingState> rowsToTypedMaps = new Function<RowsWithExecutionInfo, TypedMapsWithPagingState>() {
            @Override
            public TypedMapsWithPagingState apply(RowsWithExecutionInfo rows) {
                final List<TypedMap> typedMaps = mapper.mapRows(rows.getRows());
                return new TypedMapsWithPagingState(typedMaps, rows.getExecutionInfo().getPagingState());
            }
        };

        final ListenableFuture<TypedMapsWithPagingState> futureTypedMap = asyncUtils.transformFuture(futureRows, rowsToTypedMaps);

        asyncUtils.maybeAddAsyncListeners(futureTypedMap, asyncListeners);

        return futureTypedMap;
    }

    protected AchillesFuture<TypedMap> asyncGetFirstInternal(FutureCallback<Object>... asyncListeners) {
        log.debug("Get first result for native query '{}' asynchronously", nativeStatementWrapper.getStatement());
        final ListenableFuture<ResultSet> resultSetFuture = daoContext.execute(nativeStatementWrapper);
        final ListenableFuture<List<Row>> futureRows = asyncUtils.transformFuture(resultSetFuture, RESULTSET_TO_ROWS);

        Function<List<Row>, TypedMap> rowsToTypedMap = new Function<List<Row>, TypedMap>() {
            @Override
            public TypedMap apply(List<Row> rows) {
                List<TypedMap> result = mapper.mapRows(rows);
                if (result.isEmpty()) {
                    return null;
                } else {
                    return result.get(0);
                }
            }
        };
        final ListenableFuture<TypedMap> futureTypedMap = asyncUtils.transformFuture(futureRows, rowsToTypedMap);

        asyncUtils.maybeAddAsyncListeners(futureTypedMap, asyncListeners);

        return asyncUtils.buildInterruptible(futureTypedMap);
    }

    protected AchillesFuture<Empty> asyncExecuteInternal(FutureCallback<Object>... asyncListeners) {
        log.debug("Execute native query '{}' asynchronously", nativeStatementWrapper.getStatement());
        final ListenableFuture<ResultSet> resultSetFuture = daoContext.execute(nativeStatementWrapper);
        final ListenableFuture<Empty> futureEmpty = asyncUtils.transformFutureToEmpty(resultSetFuture, executorService);

        asyncUtils.maybeAddAsyncListeners(futureEmpty, asyncListeners);
        return asyncUtils.buildInterruptible(futureEmpty);
    }

    protected AchillesFuture<Iterator<TypedMap>> asyncIterator(Optional<Integer> fetchSizeO, FutureCallback<Object>... asyncListeners) {
        final Statement statement = nativeStatementWrapper.getStatement();
        log.debug("Execute native query {} and return iterator", statement);

        if (fetchSizeO.isPresent()) {
            statement.setFetchSize(fetchSizeO.get());
        }

        final ListenableFuture<ResultSet> futureResultSet = daoContext.execute(nativeStatementWrapper);

        final Function<ResultSet, Iterator<TypedMap>> toTypedMap = new Function<ResultSet, Iterator<TypedMap>>() {
            @Override
            public Iterator<TypedMap> apply(ResultSet resultSet) {
                return new TypedMapIterator(resultSet.iterator());
            }
        };

        final ListenableFuture<Iterator<TypedMap>> futureTypedMapIterator = asyncUtils.transformFuture(futureResultSet, toTypedMap, executorService);
        asyncUtils.maybeAddAsyncListeners(futureTypedMapIterator, asyncListeners);
        return asyncUtils.buildInterruptible(futureTypedMapIterator);
    }

    /**
     * Set paging state
     * @param pagingState paging state
     * @return this native query
     */
    public T withPagingState(PagingState pagingState) {
        pagingStateO = Optional.fromNullable(pagingState);
        return getThis();
    }

    /**
     * Set paging state as string
     * @param pagingState paging state as String
     * @return this native query
     */
    public T withPagingState(String pagingState) {
        pagingStateO = Optional.fromNullable(PagingState.fromString(pagingState));
        return getThis();
    }

    /**
     * Set paging state as byte array
     * @param pagingState paging state as byte array
     * @return this native query
     */
    public T withPagingState(byte[] pagingState) {
        pagingStateO = Optional.fromNullable(PagingState.fromBytes(pagingState));
        return getThis();
    }
}
