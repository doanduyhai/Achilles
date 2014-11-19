package info.archinnov.achilles.test.integration.tests;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Function;
import com.google.common.util.concurrent.*;

public class TestFuture {
    public static void main(String[] args) throws Exception {
        ListeningScheduledExecutorService completer = MoreExecutors.listeningDecorator(
                Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder().setNameFormat("completer").build()));

        final SettableFuture<Integer> future = SettableFuture.create();
        completer.schedule(new Runnable() {
            @Override public void run() {
                future.set(1);
            }
        }, 5, TimeUnit.SECONDS);

        ListeningExecutorService transformer = MoreExecutors.listeningDecorator(
                Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("transformer").build())
        );
        Function<Integer, Integer> inc = new Function<Integer, Integer>() {
            @Override public Integer apply(Integer n) {
                System.out.println("I'm getting executed by " + Thread.currentThread());
                return n + 1;
            }
        };
        ListenableFuture<Integer> future1 = Futures.transform(future, inc, transformer);
        ListenableFuture<Integer> future2 = Futures.transform(future1, inc);
        ListenableFuture<Integer> future3 = Futures.transform(future2, inc);

        future3.get();
    }
}