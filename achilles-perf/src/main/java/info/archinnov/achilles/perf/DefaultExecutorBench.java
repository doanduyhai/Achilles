package info.archinnov.achilles.perf;

import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@State(Scope.Benchmark)
public class DefaultExecutorBench {

    public static final int TASKS = 10000;

    public static final int BOUNDED_QUEUE_CAPACITY = 1000;

    public static final int TASK_WEIGHT = 1000000;

    @Benchmark
    public void unlimitedThreadsAndSynchronousQueue(final Blackhole blackhole) throws NoSuchFieldException, InterruptedException {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();

        submitTasks(blackhole, executor);

        awaitTermination(executor);
    }

    @Benchmark
    public void cappedThreadsAndSynchronousQueue(final Blackhole blackhole) throws NoSuchFieldException, InterruptedException {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(1,10,
                60L, TimeUnit.DAYS,
                new SynchronousQueue<Runnable>(), new ThreadPoolExecutor.CallerRunsPolicy());

        submitTasks(blackhole, executor);

        awaitTermination(executor);
    }

    @Benchmark
    public void unlimitedThreadsAndUnboundedLBQ(final Blackhole blackhole) throws NoSuchFieldException, InterruptedException {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(0, Integer.MAX_VALUE,
                60L, TimeUnit.DAYS,
                new LinkedBlockingQueue<Runnable>());

        submitTasks(blackhole, executor);

        awaitTermination(executor);
    }

    @Benchmark
    public void unlimitedAndBoundedLBQ(final Blackhole blackhole) throws NoSuchFieldException, InterruptedException {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(0, Integer.MAX_VALUE,
                60L, TimeUnit.DAYS,
                new LinkedBlockingQueue<Runnable>(BOUNDED_QUEUE_CAPACITY), new ThreadPoolExecutor.CallerRunsPolicy());

        submitTasks(blackhole, executor);

        awaitTermination(executor);
    }

    @Benchmark
    public void cappedThreadsAndBoundedLBQ(final Blackhole blackhole) throws NoSuchFieldException, InterruptedException {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 10,
                60L, TimeUnit.DAYS,
                new LinkedBlockingQueue<Runnable>(BOUNDED_QUEUE_CAPACITY), new ThreadPoolExecutor.CallerRunsPolicy());

        submitTasks(blackhole, executor);
        awaitTermination(executor);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(".*" + DefaultExecutorBench.class.getSimpleName() + ".*")
                .warmupIterations(5)
                .measurementIterations(5)
                .shouldDoGC(true)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    private void awaitTermination(ThreadPoolExecutor executor) throws InterruptedException {
//        printThreadCount(executor);
        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
    }

    private void printThreadCount(ThreadPoolExecutor executor) {
        System.err.println(executor.getPoolSize());
    }

    private void submitTasks(final Blackhole blackhole, ThreadPoolExecutor executor) {
        for (int i = 0; i < TASKS; i++) {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    blackhole.consume(TASK_WEIGHT);
                }
            });
        }
    }
}