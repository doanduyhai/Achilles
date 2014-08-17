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

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * <p>
 *  Utility class to assign an initial counter value to a transient entity. Example
 *
 * <pre class="code"><code class="java">
 *
 *  {@literal @}Entity
 *  public class UserEntity
 *  {
 *      {@literal @}Id
 *      private Long userId;
 *
 *      ...
 *
 *      {@literal @}Column
 *      private Counter
 *
 *      private UserEntity(Long userId,.....,Counter popularity)
 *      {
 *          this.userId = userId;
 *          ...
 *          this.popularity = popularity;
 *      }
 *      ...
 *  }
 *
 *   // Creating a new user with initial popularity value set to 100
 *   manager.persist(new UserEntity(10L,....,CounterBuilder.incr(100L));
 *
 * </code></pre>
 *
 * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Achilles-Custom-Types#counterbuilder" target="_blank">CounterBuilder</a>
 */
public class CounterBuilder {

    public static Counter incr() {
        return new CounterImpl(1L);
    }

    public static Counter incr(long incr) {
        return new CounterImpl(incr);
    }

    public static Counter decr() {
        return new CounterImpl(-1L);
    }

    public static Counter decr(long decr) {
        return new CounterImpl(-1L * decr);
    }


    public static class CounterImpl implements Counter {

        @JsonProperty
        private long counterValue;

        private CounterImpl(long counterValue) {
            this.counterValue = counterValue;
        }

        @Override
        public Long get() {
            return counterValue;
        }

        @Override
        public void incr() {
            counterValue++;
        }

        @Override
        public void incr(long increment) {
            counterValue += increment;
        }

        @Override
        public void decr() {
            counterValue--;
        }

        @Override
        public void decr(long decrement) {
            counterValue -= decrement;
        }
    }
}
