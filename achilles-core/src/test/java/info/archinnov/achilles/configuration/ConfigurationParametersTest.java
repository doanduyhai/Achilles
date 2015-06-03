/*
 * Copyright (C) 2012-2014 DuyHai DOAN
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package info.archinnov.achilles.configuration;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for parsing configuration parameters.
 */
public class ConfigurationParametersTest {

    @Test
    public void testProperlyParsed() {
        String key = "achilles.entity.packages";
        ConfigurationParameters result = ConfigurationParameters.fromLabel(key);
        Assert.assertEquals(ConfigurationParameters.ENTITY_PACKAGES, result);
    }

    @Test
    public void testWhenNull() {
        String key = "xx.null";
        ConfigurationParameters result = ConfigurationParameters.fromLabel(key);
        Assert.assertNull(result);
    }
}
