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

package info.archinnov.achilles.proxy.wrapper;

import java.util.Arrays;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ValueCollectionWrapperTest {

	@Test(expected = UnsupportedOperationException.class)
	public void should_exception_on_add() throws Exception {
		ValueCollectionWrapper wrapper = new ValueCollectionWrapper(Arrays.<Object> asList("a"));

		wrapper.add("");
	}

	@Test(expected = UnsupportedOperationException.class)
	public void should_exception_on_add_all() throws Exception {
		ValueCollectionWrapper wrapper = new ValueCollectionWrapper(Arrays.<Object> asList("a"));

		wrapper.addAll(Arrays.asList("a", "b"));
	}
}
