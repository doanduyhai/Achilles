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

package info.archinnov.achilles.async;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.DriverInternalError;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import info.archinnov.achilles.exception.AchillesBeanMappingException;
import info.archinnov.achilles.exception.AchillesBeanValidationException;
import info.archinnov.achilles.exception.AchillesLightWeightTransactionException;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.exception.AchillesInvalidTableException;
import info.archinnov.achilles.exception.AchillesStaleObjectStateException;

public class AchillesFuture<V> implements ListenableFuture<V> {

    private final ListenableFuture<V> delegate;

    public AchillesFuture(ListenableFuture<V> delegate) {
        this.delegate = delegate;
    }

    public V getImmediately() {
        try {
            return Uninterruptibles.getUninterruptibly(delegate);
        } catch (ExecutionException e) {
            throw extractCauseFromExecutionException(e);
        }
    }

    @Override
    public void addListener(Runnable listener, Executor executor) {
        delegate.addListener(listener, executor);
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
    public V get() throws InterruptedException, ExecutionException {
        return delegate.get();
    }

    @Override
    public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return delegate.get(timeout, unit);
    }


    private RuntimeException extractCauseFromExecutionException(ExecutionException e) {
        final Throwable cause = e.getCause();
        if (cause instanceof DriverException) {
            return ((DriverException) cause).copy();
        } else if (cause instanceof DriverInternalError) {
            return (DriverInternalError) cause;
        } else if (cause instanceof AchillesLightWeightTransactionException) {
            return (AchillesLightWeightTransactionException) cause;
        } else if (cause instanceof AchillesBeanMappingException) {
            return (AchillesBeanMappingException) cause;
        } else if (cause instanceof AchillesBeanMappingException) {
            return (AchillesBeanMappingException) cause;
        } else if (cause instanceof AchillesBeanValidationException) {
            return (AchillesBeanValidationException) cause;
        } else if (cause instanceof AchillesInvalidTableException) {
            return (AchillesInvalidTableException) cause;
        } else if (cause instanceof AchillesStaleObjectStateException) {
            return (AchillesStaleObjectStateException) cause;
        } else if (cause instanceof AchillesException) {
            return (AchillesException) cause;
        } else {
            return new AchillesException(cause);
        }
    }
}
