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

package info.archinnov.achilles.internal.metadata.holder;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import info.archinnov.achilles.schemabuilder.Create;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.internal.metadata.holder.PartitionComponents;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class PartitionComponentsTest {

	@Rule
	public ExpectedException exception = ExpectedException.none();

    @Mock
    private PropertyMeta meta1;
    @Mock
    private PropertyMeta meta2;
    @Mock
    private PropertyMeta meta3;
    @Mock
    private PropertyMeta meta4;

	private PartitionComponents partitionComponents;

    @Before
    public void setUp() {
        when(meta1.<Long>getValueClass()).thenReturn(Long.class);
        when(meta2.<Integer>getValueClass()).thenReturn(int.class);
        when(meta3.<ByteBuffer>getValueClass()).thenReturn(ByteBuffer.class);
        when(meta4.<String>getValueClass()).thenReturn(String.class);
        partitionComponents = new PartitionComponents(asList(meta1, meta2, meta3, meta4));
    }

    @Test
    public void should_validate_partition_components() throws Exception {
        partitionComponents.validatePartitionComponents("entity", 10L, 2, ByteBuffer.wrap("test".getBytes()));
    }

    @Test
    public void should_exception_when_no_partition_components() throws Exception {
        exception.expect(AchillesException.class);
        exception.expectMessage(format("There should be at least one partition key component provided for querying on entity '%s'", "entity"));

        partitionComponents.validatePartitionComponents("entity");
    }

    @Test
    public void should_exception_when_too_many_partition_components() throws Exception {
        exception.expect(AchillesException.class);
        exception.expectMessage(format("The partition key components count should be less or equal to '%s' for querying on entity '%s'", 4, "entity"));

        partitionComponents.validatePartitionComponents("entity", 10L, 2, ByteBuffer.wrap("test".getBytes()), "test", new Date());
    }

    @Test
    public void should_exception_when_null_partition_components() throws Exception {
        exception.expect(AchillesException.class);
        exception.expectMessage(format("The '%sth' partition key component should not be null", 2));

        partitionComponents.validatePartitionComponents("entity", 10L, null, ByteBuffer.wrap("test".getBytes()));
    }

    @Test
    public void should_exception_when_partition_components_has_incorrect_type() throws Exception {
        exception.expect(AchillesException.class);
        exception.expectMessage(format("The type '%s' of partition key component '%s' for querying on entity '%s' is not valid. It should be '%s'",
                String.class.getCanonicalName(), "wrong_type", "entity", int.class.getCanonicalName()));

        partitionComponents.validatePartitionComponents("entity", 10L, "wrong_type", ByteBuffer.wrap("test".getBytes()));
    }

    @Test
    public void should_validate_partition_components_IN() throws Exception {
        partitionComponents.validatePartitionComponentsIn("entity", "first", "second", "third");
    }

    @Test
    public void should_exception_when_no_partition_components_IN() throws Exception {
        exception.expect(AchillesException.class);
        exception.expectMessage(format("There should be at least one partition key component IN provided for querying on entity '%s'", "entity"));

        partitionComponents.validatePartitionComponentsIn("entity");
    }

    @Test
    public void should_exception_when_null_partition_components_IN() throws Exception {
        exception.expect(AchillesException.class);
        exception.expectMessage(format("The '%sth' partition key component IN should not be null", 2));

        partitionComponents.validatePartitionComponentsIn("entity", "one", null, "three");
    }

    @Test
    public void should_exception_when_partition_components_IN_incorrect_type() throws Exception {
        exception.expect(AchillesException.class);
        exception.expectMessage(format("The type '%s' of partition key component '%s' for querying on entity '%s' is not valid. It should be '%s'",
                Long.class.getCanonicalName(), 10L, "entity", String.class.getCanonicalName()));

        partitionComponents.validatePartitionComponentsIn("entity", "one", 10L, "three");
    }
}
