/*
 * Copyright (C) 2012-2021 DuyHai DOAN
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

package info.archinnov.achilles.embedded;

import static org.assertj.core.api.Assertions.assertThat;
import java.net.ServerSocket;
import org.junit.Test;

public class PortFinderTest {

    @Test
    public void try_to_find_first_port() throws Exception {
        int start = 7050;
        int result = PortFinder.findFirstAvailableBetween(start, start + 10);
        assertThat(result).isEqualTo(start);

        try(ServerSocket ss = new ServerSocket(start)) {
            ss.setReuseAddress(true);
            int secondResult = PortFinder.findFirstAvailableBetween(start, start + 10);
            assertThat(secondResult).isEqualTo(start + 1);
        }

        int thirdResult = PortFinder.findFirstAvailableBetween(start, start + 10);
        assertThat(thirdResult).isEqualTo(start);
    }
}