package info.archinnov.achilles.type;

import com.google.common.base.Optional;

public class Options {

    ConsistencyLevel consistency;

    Integer ttl;

    Long timestamp;

    Options() {
    }

    public Optional<ConsistencyLevel> getConsistencyLevel() {
        return Optional.fromNullable(consistency);
    }

    public Optional<Integer> getTtl() {
        return Optional.fromNullable(ttl);
    }

    public Optional<Long> getTimestamp() {
        return Optional.fromNullable(timestamp);
    }

    @Override
    public String toString() {
        return "Options [consistency=" + consistency + ", ttl=" + ttl + ", timestamp=" + timestamp + "]";
    }

    public Options duplicateWithoutTtlAndTimestamp()
    {
        return OptionsBuilder
                .withConsistency(consistency);
    }

    public Options duplicateWithNewConsistencyLevel(ConsistencyLevel consistencyLevel)
    {
        return OptionsBuilder
                .withConsistency(consistencyLevel)
                .ttl(ttl)
                .timestamp(timestamp);
    }
}
