package info.archinnov.achilles.async;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;
import com.google.common.base.Function;
import com.google.common.util.concurrent.ListenableFuture;

public class SynchronousTransformingFuture<I, O> implements ListenableFuture<O> {

    private final Future<I> delegate;

    private final Function<I, O> function;

    /**
     * As the value is created by transforming the underlying result, which may be mutable (e.g. a ResultSet will be consumed), we must cache the
     * value once transformed.
     */
    private final ReentrantLock valueLock = new ReentrantLock();
    private boolean valueAlreadyTransformed = false;
    private O value;

    public SynchronousTransformingFuture(Future<I> delegate, Function<I, O> function) {
        this.delegate = delegate;
        this.function = function;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return delegate.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
        return delegate.isCancelled();
    }

    @Override
    public boolean isDone() {
        return delegate.isDone();
    }

    @Override
    public O get() throws InterruptedException, ExecutionException {
        valueLock.lock();
        if(valueAlreadyTransformed) {
            return value;
        }
        try {
            I i  = delegate.get();
            value = function.apply(i);
            valueAlreadyTransformed = true;
            return value;
        } finally {
            valueLock.unlock();
        }
    }

    @Override
    public O get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        valueLock.lock();
        if(valueAlreadyTransformed) {
            return value;
        }
        try {
            I i  = delegate.get(timeout, unit);
            value = function.apply(i);
            valueAlreadyTransformed = true;
            return value;
        } finally {
            valueLock.unlock();
        }
    }

    @Override
    public void addListener(Runnable listener, Executor executor) {
        executor.execute(listener);
    }
}
