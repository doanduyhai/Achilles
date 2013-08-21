package info.archinnov.achilles.type;

public class OptionsBuilder {

    private static final NoOptions noOptions = new NoOptions();

    public static NoOptions noOptions()
    {
        return noOptions;
    }

    public static WithTtlAndTimestamp withConsistency(ConsistencyLevel consistencyLevel)
    {
        return new WithTtlAndTimestamp(consistencyLevel);
    }

    public static WithConsistencyAndTimestamp withTtl(Integer ttl)
    {
        return new WithConsistencyAndTimestamp(ttl);
    }

    public static WithConsistencyAndTtl withTimestamp(Long timestamp)
    {
        return new WithConsistencyAndTtl(timestamp);
    }

    public static class NoOptions extends Options
    {
        protected NoOptions() {
        }

        @Override
        public NoOptions duplicateWithoutTtlAndTimestamp()
        {
            return this;
        }
    }

    public static class WithTtlAndTimestamp extends Options
    {

        protected WithTtlAndTimestamp(ConsistencyLevel consistencyLevel) {
            super.consistency = consistencyLevel;
        }

        public WithTimestamp ttl(Integer ttl)
        {
            return new WithTimestamp(super.consistency, ttl);
        }

        public WithTtl timestamp(Long timestamp)
        {
            return new WithTtl(super.consistency, timestamp);
        }
    }

    public static class WithConsistencyAndTimestamp extends Options
    {

        protected WithConsistencyAndTimestamp(Integer ttl) {
            super.ttl = ttl;
        }

        public WithTimestamp consistency(ConsistencyLevel consistency)
        {
            return new WithTimestamp(consistency, super.ttl);
        }

        public WithConsistency timestamp(Long timestamp)
        {
            return new WithConsistency(super.ttl, timestamp);
        }
    }

    public static class WithConsistencyAndTtl extends Options
    {

        protected WithConsistencyAndTtl(Long timestamp) {
            super.timestamp = timestamp;
        }

        public WithTtl consistency(ConsistencyLevel consistency)
        {
            return new WithTtl(consistency, super.timestamp);
        }

        public WithConsistency ttl(Integer ttl)
        {
            return new WithConsistency(ttl, super.timestamp);
        }
    }

    public static class WithConsistency extends Options
    {

        protected WithConsistency(Integer ttl, Long timestamp) {
            super.ttl = ttl;
            super.timestamp = timestamp;
        }

        public ReadOnlyOptions consistency(ConsistencyLevel consistencyLevel)
        {
            super.consistency = consistencyLevel;
            return new ReadOnlyOptions(super.consistency, super.ttl, super.timestamp);
        }

    }

    public static class WithTtl extends Options
    {

        protected WithTtl(ConsistencyLevel consistencyLevel, Long timestamp) {
            super.consistency = consistencyLevel;
            super.timestamp = timestamp;
        }

        public ReadOnlyOptions ttl(Integer ttl)
        {
            super.ttl = ttl;
            return new ReadOnlyOptions(super.consistency, super.ttl, super.timestamp);
        }

    }

    public static class WithTimestamp extends Options
    {

        protected WithTimestamp(ConsistencyLevel consistencyLevel, Integer ttl) {
            super.consistency = consistencyLevel;
            super.ttl = ttl;
        }

        public ReadOnlyOptions timestamp(Long timestamp)
        {
            super.timestamp = timestamp;
            return new ReadOnlyOptions(super.consistency, super.ttl, super.timestamp);
        }
    }

    public static class ReadOnlyOptions extends Options
    {

        public ReadOnlyOptions(ConsistencyLevel consistencyLevel, Integer ttl, Long timestamp) {
            super.consistency = consistencyLevel;
            super.ttl = ttl;
            super.timestamp = timestamp;
        }

    }
}
