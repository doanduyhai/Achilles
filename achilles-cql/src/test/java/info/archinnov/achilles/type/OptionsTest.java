/**
 *
 * Copyright (C) 2012-2013 DuyHai DOAN
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
package info.archinnov.achilles.type;

import static com.datastax.driver.core.ConsistencyLevel.EACH_QUORUM;
import static com.datastax.driver.core.ConsistencyLevel.LOCAL_QUORUM;
import static org.fest.assertions.api.Assertions.assertThat;

import org.junit.Test;

public class OptionsTest {

	@Test
	public void should_duplicate_without_ttl_and_timestamp() throws Exception {
		Options options = OptionsBuilder.withConsistency(EACH_QUORUM).withTtl(10).withTimestamp(100L);

		Options newOptions = options.duplicateWithoutTtlAndTimestamp();

		assertThat(newOptions.getConsistencyLevel().get()).isSameAs(EACH_QUORUM);
		assertThat(newOptions.getTimestamp().isPresent()).isFalse();
		assertThat(newOptions.getTtl().isPresent()).isFalse();
	}

	@Test
	public void should_duplicate_with_new_consistency_level() throws Exception {
		Options options = OptionsBuilder.withConsistency(EACH_QUORUM).withTtl(10).withTimestamp(100L);

		Options newOptions = options.duplicateWithNewConsistencyLevel(LOCAL_QUORUM);

		assertThat(newOptions.getConsistencyLevel().get()).isSameAs(LOCAL_QUORUM);
		assertThat(newOptions.getTimestamp().get()).isEqualTo(100L);
		assertThat(newOptions.getTtl().get()).isEqualTo(10);
	}
}
