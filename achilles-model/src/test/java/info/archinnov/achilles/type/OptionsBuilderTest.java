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

import static info.archinnov.achilles.type.ConsistencyLevel.*;
import static org.fest.assertions.api.Assertions.assertThat;

import org.junit.Test;

public class OptionsBuilderTest {

	@Test
	public void should_create_options_with_all_parameters() throws Exception {
		Options options = OptionsBuilder.withConsistency(ALL).ttl(10)
				.timestamp(100L);

		assertThat(options.getConsistencyLevel().get()).isSameAs(ALL);
		assertThat(options.getTtl().get()).isEqualTo(10);
		assertThat(options.getTimestamp().get()).isEqualTo(100L);

		options = OptionsBuilder.withConsistency(ALL).timestamp(100L).ttl(10);

		assertThat(options.getConsistencyLevel().get()).isSameAs(ALL);
		assertThat(options.getTtl().get()).isEqualTo(10);
		assertThat(options.getTimestamp().get()).isEqualTo(100L);

		options = OptionsBuilder.withTtl(11).consistency(ANY).timestamp(111L);

		assertThat(options.getConsistencyLevel().get()).isSameAs(ANY);
		assertThat(options.getTtl().get()).isEqualTo(11);
		assertThat(options.getTimestamp().get()).isEqualTo(111L);

		options = OptionsBuilder.withTtl(11).timestamp(111L).consistency(ANY);

		assertThat(options.getConsistencyLevel().get()).isSameAs(ANY);
		assertThat(options.getTtl().get()).isEqualTo(11);
		assertThat(options.getTimestamp().get()).isEqualTo(111L);

		options = OptionsBuilder.withTimestamp(122L).consistency(ONE).ttl(12);

		assertThat(options.getConsistencyLevel().get()).isSameAs(ONE);
		assertThat(options.getTtl().get()).isEqualTo(12);
		assertThat(options.getTimestamp().get()).isEqualTo(122L);

		options = OptionsBuilder.withTimestamp(122L).ttl(12).consistency(ONE);

		assertThat(options.getConsistencyLevel().get()).isSameAs(ONE);
		assertThat(options.getTtl().get()).isEqualTo(12);
		assertThat(options.getTimestamp().get()).isEqualTo(122L);
	}

	@Test
	public void should_create_no_options() throws Exception {
		Options options = OptionsBuilder.noOptions();

		assertThat(options.getConsistencyLevel().isPresent()).isFalse();
		assertThat(options.getTtl().isPresent()).isFalse();
		assertThat(options.getTimestamp().isPresent()).isFalse();
		Options duplicate = options.duplicateWithoutTtlAndTimestamp();
		assertThat(duplicate.getConsistencyLevel().isPresent()).isFalse();
		assertThat(duplicate.getTtl().isPresent()).isFalse();
		assertThat(duplicate.getTimestamp().isPresent()).isFalse();
	}
}
