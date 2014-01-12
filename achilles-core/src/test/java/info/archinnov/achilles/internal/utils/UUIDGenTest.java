package info.archinnov.achilles.internal.utils;

import static org.fest.assertions.api.Assertions.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class UUIDGenTest {

	@Test
	public void should_generate_distinct_microseconds() throws Exception {
		Long timestampInMicros = UUIDGen.increasingMicroTimestamp();
		for (int i = 0; i < 1000; i++) {
			Long newTimestampInMicros = UUIDGen.increasingMicroTimestamp();
			assertThat(newTimestampInMicros).isGreaterThan(timestampInMicros);
			timestampInMicros = newTimestampInMicros;
		}
	}
}
