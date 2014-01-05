package info.archinnov.achilles.utils;

import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.UUID;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class UUIDGenTest {

    @Test
    public void should_generate_distinct_microseconds() throws Exception {
        Long timestampInMicros = UUIDGen.increasingMicroTimestamp();
        for(int i=0;i<10000;i++) {
            Long newTimestampInMicros = UUIDGen.increasingMicroTimestamp();
            assertThat(newTimestampInMicros).isGreaterThan(timestampInMicros);
            timestampInMicros = newTimestampInMicros;
        }
    }
}
