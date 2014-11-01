package info.archinnov.achilles.perf;

import java.lang.reflect.Field;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;

@State(Scope.Benchmark)
public class LoggerBench {

    /*
      Some code called a huge number of times is using slf4j's logger with
      3 or more args, which is the vararg version.
      To get an idea of the impact of the implicit array allocation,
      here are a few benches.

      Note : we just want to measure the impact of the array instantiation and methods calls
      The logger configuration should be made so that the logged line is skipped.

      Conclusions :
      - the impact of the array allocation is huge.
      - the impact of the method calls is negligible (or may even be optimized away).
      - for method called a lot, guarding the logger call with an if is an easy performance win.
     */

    private static final Logger logger = LoggerFactory.getLogger(LoggerBench.class);

    String entity = "";
    PropertyMeta idMeta = new PropertyMeta();
    Field field;

    public LoggerBench() {
        try {
            field = String.class.getDeclaredField("hash");
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    @Benchmark
    public void log_with_no_guard() throws NoSuchFieldException {
        // code fragment taken from Reflection invoker
        logger.trace("Get primary key {} from instance {} of class {}", idMeta.getPropertyName(), entity, field
                .getDeclaringClass().getCanonicalName());
    }

    @Benchmark
    public void log_with_guard() throws NoSuchFieldException {
        if (logger.isDebugEnabled()) {
            logger.trace("Get primary key {} from instance {} of class {}", idMeta.getPropertyName(), entity, field
                    .getDeclaringClass().getCanonicalName());
        }
    }

    @Benchmark
    public void log_with_unguarded_method_calls() throws NoSuchFieldException {
        String propertyName = idMeta.getPropertyName();
        String canonicalName = field.getDeclaringClass().getCanonicalName();

        if (logger.isDebugEnabled()) {
            logger.trace("Get primary key {} from instance {} of class {}", propertyName, entity, canonicalName);
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(".*" + LoggerBench.class.getSimpleName() + ".*")
                .warmupIterations(5)
                .measurementIterations(10)
                .forks(1)
                .build();

        new Runner(opt).run();
    }
}