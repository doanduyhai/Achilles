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
package info.archinnov.achilles.type;

/**
 * <p>
 *     List all available consistency levels
 *     <br>
 *     <br>
 *     Please note that <strong>SERIAL</strong> and <strong>LOCAL_SERIAL</strong> levels only
 *     apply to LWT operations
 *     <br>
 *     <br>
 *     All levels having <strong>LOCAL</strong> in their name apply to a multi-datacenters cluster
 * </p>
 * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Consistency-Level" target="_blank">ConsistencyLevel</a>
 */
public enum ConsistencyLevel {
    ANY, ONE, TWO, THREE, QUORUM, EACH_QUORUM, LOCAL_QUORUM, ALL, SERIAL, LOCAL_SERIAL, LOCAL_ONE;
}
