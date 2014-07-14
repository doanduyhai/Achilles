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

package test.slice;

import info.archinnov.achilles.type.ConsistencyLevel;

public interface CommonProperties<T> {

    public T inclusiveBounds();

    public T exclusiveBounds();

    public T fromInclusiveToExclusiveBounds();

    public T fromExclusiveToInclusiveBounds();

    public T orderByAscending();

    public T orderByDescending();

    public T limit(int limit);

    public T withConsistency(ConsistencyLevel consistencyLevel);
}
