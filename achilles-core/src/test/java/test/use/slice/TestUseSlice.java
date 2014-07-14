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

package test.use.slice;

import static info.archinnov.achilles.schemabuilder.Create.Options.ClusteringOrder;
import static info.archinnov.achilles.schemabuilder.Create.Options.ClusteringOrder.Sorting;
import static info.archinnov.achilles.type.ConsistencyLevel.ALL;
import static org.mockito.Mockito.when;
import java.util.Arrays;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.persistence.operations.SliceQueryExecutor;
import info.archinnov.achilles.query.slice.DeleteDSL;
import info.archinnov.achilles.query.slice.IterateDSL;
import info.archinnov.achilles.query.slice.SelectDSL;
import info.archinnov.achilles.query.slice.SliceQueryBuilder;

@RunWith(MockitoJUnitRunner.class)
public class TestUseSlice {

    @Mock
    private SliceQueryExecutor executor;

    @Mock
    private EntityMeta entityMeta;

    @Before
    public void setUp() {
        when(entityMeta.getClusteringOrders()).thenReturn(Arrays.asList(new ClusteringOrder("col", Sorting.ASC)));
        when(entityMeta.getPartitionKeysSize()).thenReturn(2);
        when(entityMeta.getClusteringKeysSize()).thenReturn(3);

    }

    @Test
    public void should_test_select() throws Exception {

        SelectDSL<String> builder = new SliceQueryBuilder<>(executor, String.class, entityMeta).forSelect();

        builder.withPartitionComponents("5").andPartitionComponentsIN("A", "B").limit(3)
                .fromClusterings("a", "b")
                .limit(5)
                .toClusterings("c", "d").withConsistency(ALL);

        System.out.println("\n**************\n");

        builder = new SliceQueryBuilder<>(executor, String.class, entityMeta).forSelect();
        builder.withPartitionComponents("5").andPartitionComponentsIN("A", "B")
                .limit(3).withClusterings("a", "b")
                .limit(5)
                .andClusteringsIN("c", "d").withConsistency(ALL)
                .orderByDescending();
    }


    @Test
    public void should_test_iterate() throws Exception {
        //Given
        final IterateDSL<String> builder = new SliceQueryBuilder<>(executor, String.class, entityMeta).forIteration();

        builder.withPartitionComponents("a")
                .andPartitionComponentsIN("A", "B")
                .limit(3)
                .fromClusterings("a", "b")
                .limit(5)
                .toClusterings("c", "d")
                .withConsistency(ALL);

        System.out.println("\n**************\n");

        builder.withPartitionComponents("a")
                .limit(3)
                .withClusterings("a", "b").andClusteringsIN("c", "d")
                .withConsistency(ALL)
                .orderByDescending();
    }

    @Test
    public void should_test_remove() throws Exception {

        final DeleteDSL<String> builder = new SliceQueryBuilder<>(executor, String.class, entityMeta).forDelete();

        builder.withPartitionComponents("a").andPartitionComponentsIN("A", "B");
    }

    @Test
    public void should_test() throws Exception {

        final Child<String> child = new Child(String.class);

        child.limit(3).doSomething().limit(2).thenSomething();

    }


}
