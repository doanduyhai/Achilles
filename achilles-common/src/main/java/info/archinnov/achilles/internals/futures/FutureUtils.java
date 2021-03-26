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

package info.archinnov.achilles.internals.futures;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Copy from original class at https://github.com/lukas-krecan/future-converter/blob/master/java8-guava/src/main/java/net/javacrumbs/futureconverter/java8guava/FutureConverter.java
 * Licenced under Apache 2.0
 */
public class FutureUtils {

    public static <T> CompletableFuture<T> toCompletableFuture(ListenableFuture<T> listenableFuture, ExecutorService executor) {
        CompletableFuture<T> completable = new CompletableListenableFuture<>(listenableFuture);

        Futures.addCallback(listenableFuture, new FutureCallback<T>() {
            @Override
            public void onSuccess(T result) {
                completable.complete(result);
            }

            @Override
            public void onFailure(Throwable t) {
                completable.completeExceptionally(t);
            }
        }, executor);

        return completable;
    }


    private static final class CompletableListenableFuture<T> extends CompletableFuture<T> {
        private final ListenableFuture<T> listenableFuture;

        public CompletableListenableFuture(ListenableFuture<T> listenableFuture) {
            this.listenableFuture = listenableFuture;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            boolean result = listenableFuture.cancel(mayInterruptIfRunning);
            super.cancel(mayInterruptIfRunning);
            return result;
        }
    }
}
