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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * <p>
 *     Abstraction representing a distributed counter in Cassandra
 *     <br>
 *     <br>
 *     This counter interface only exposes operations to increment/decrement and read counter values
 *     and is consistent with the way counters should be accessed in Cassandra
 *     <br>
 *     <br>
 *     Counter <strong>deletion</strong> is not exposed because it is officially not recommended and is subject
 *     to subtle bugs
 * </p>
 * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Counters" target="_blank">Counter</a>
 */
@JsonDeserialize(using = CounterDeserializer.class)
@JsonSerialize(using = CounterSerializer.class)
public interface Counter {
    public Long get();

    public void incr();

    public void incr(long increment);

    public void decr();

    public void decr(long decrement);


}
