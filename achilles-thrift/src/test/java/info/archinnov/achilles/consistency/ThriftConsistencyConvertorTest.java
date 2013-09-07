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
package info.archinnov.achilles.consistency;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.type.ConsistencyLevel;
import me.prettyprint.hector.api.HConsistencyLevel;

import org.junit.Test;

public class ThriftConsistencyConvertorTest {
	@Test
	public void should_get_hector_level_from_achilles_level() throws Exception {
		assertThat(
				ThriftConsistencyConvertor
						.getHectorLevel(ConsistencyLevel.EACH_QUORUM))
				.isEqualTo(HConsistencyLevel.EACH_QUORUM);
	}

}
