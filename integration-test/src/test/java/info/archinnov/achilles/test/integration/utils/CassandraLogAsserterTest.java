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

package info.archinnov.achilles.test.integration.utils;

import static info.archinnov.achilles.type.ConsistencyLevel.EACH_QUORUM;
import static info.archinnov.achilles.type.ConsistencyLevel.LOCAL_QUORUM;
import static info.archinnov.achilles.type.ConsistencyLevel.ONE;
import static info.archinnov.achilles.type.ConsistencyLevel.THREE;
import static info.archinnov.achilles.type.ConsistencyLevel.TWO;
import static org.mockito.Mockito.*;

import java.io.ByteArrayOutputStream;
import org.junit.ComparisonFailure;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CassandraLogAsserterTest {

    @InjectMocks
    private CassandraLogAsserter logAsserter;

    @Mock
    private ByteArrayOutputStream logStream;


    @Test
    public void should_check_consistencies_in_order() throws Exception {
        //Given //When
        logAsserter.logStream = logStream;
        when(logStream.toString()).thenReturn("writing request EXECUTE [cl=ONE]\n writing request QUERY\n writing request EXECUTE [cl=EACH_QUORUM]");

        //Then
        logAsserter.assertConsistencyLevels(ONE, EACH_QUORUM);
    }

    @Test
    public void should_check_batch_consistency() throws Exception {
        //Given //When
        logAsserter.logStream = logStream;
        when(logStream.toString()).thenReturn("writing request EXECUTE [cl=ONE]\n writing request Batch at consistency EACH_QUORUM");

        //Then
        logAsserter.assertConsistencyLevels(ONE, EACH_QUORUM);
    }

    @Test(expected = ComparisonFailure.class)
    public void should_fail_when_consistency_not_in_order() throws Exception {
        //Given //When
        logAsserter.logStream = logStream;
        when(logStream.toString()).thenReturn("writing request EXECUTE [cl=ONE]\n writing request QUERY\n writing request EXECUTE [cl=EACH_QUORUM]");

        //Then
        logAsserter.assertConsistencyLevels(EACH_QUORUM,ONE);
    }

    @Test(expected = ComparisonFailure.class)
    public void should_fail_if_more_expected_consistencies_than_found() throws Exception {
        //Given //When
        logAsserter.logStream = logStream;
        when(logStream.toString()).thenReturn("writing request EXECUTE [cl=ONE]\n writing request QUERY\n writing request EXECUTE [cl=LOCAL_QUORUM]");

        //Then
        logAsserter.assertConsistencyLevels(ONE,EACH_QUORUM);
    }

    @Test
    public void should_succeed_if_more_found_consistencies_than_expected() throws Exception {
        //Given //When
        logAsserter.logStream = logStream;
        when(logStream.toString()).thenReturn("writing request EXECUTE [cl=ONE]\n writing request QUERY [cl=THREE]\n writing request EXECUTE [cl=LOCAL_QUORUM]");

        //Then
        logAsserter.assertConsistencyLevels(ONE,LOCAL_QUORUM);
    }

    @Test
    public void should_find_serial_consistency_level_only() throws Exception {
        //Given //When
        logAsserter.logStream = logStream;
        when(logStream.toString()).thenReturn("writing request EXECUTE [cl=TWO,...,serialCl=ONE]\n writing request QUERY [cl=THREE]\n writing request EXECUTE [cl=LOCAL_QUORUM]");


        //Then
        logAsserter.assertSerialConsistencyLevels(ONE);
    }

    @Test
    public void should_find_serial_and_normal_consistency_levels() throws Exception {
        //Given //When
        logAsserter.logStream = logStream;
        when(logStream.toString()).thenReturn("writing request EXECUTE [cl=TWO,...,serialCl=ONE]\n writing request QUERY [cl=THREE]\n writing request EXECUTE [cl=LOCAL_QUORUM]");

        //Then
        logAsserter.assertSerialConsistencyLevels(ONE,TWO,THREE,LOCAL_QUORUM);
    }

    @Test(expected = ComparisonFailure.class)
    public void should_fail_if_serial_consistency_level_not_found_alone() throws Exception {
        //Given //When
        logAsserter.logStream = logStream;
        when(logStream.toString()).thenReturn("writing request EXECUTE [cl=TWO,...,serialCl=TWO]\n writing request QUERY [cl=THREE]\n writing request EXECUTE [cl=LOCAL_QUORUM]");

        //Then
        logAsserter.assertSerialConsistencyLevels(ONE);
    }

    @Test(expected = ComparisonFailure.class)
    public void should_fail_if_serial_consistency_level_not_found_but_normal_consistencies_found() throws Exception {
        //Given //When
        logAsserter.logStream = logStream;
        when(logStream.toString()).thenReturn("writing request EXECUTE [cl=TWO,...,serialCl=TWO]\n writing request QUERY [cl=THREE]\n writing request EXECUTE [cl=LOCAL_QUORUM]");

        //Then
        logAsserter.assertSerialConsistencyLevels(ONE,TWO,THREE,LOCAL_QUORUM);
    }

    @Test(expected = ComparisonFailure.class)
    public void should_fail_if_found_serial_but_not_normal_consistency_levels() throws Exception {
        //Given //When
        logAsserter.logStream = logStream;
        when(logStream.toString()).thenReturn("writing request EXECUTE [cl=TWO,...,serialCl=TWO]\n writing request QUERY [cl=ONE]\n writing request EXECUTE [cl=LOCAL_QUORUM]");

        //Then
        logAsserter.assertSerialConsistencyLevels(ONE,TWO,THREE,LOCAL_QUORUM);
    }
}
