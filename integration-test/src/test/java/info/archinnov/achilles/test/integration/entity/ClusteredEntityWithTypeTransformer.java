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
package info.archinnov.achilles.test.integration.entity;

import info.archinnov.achilles.annotations.*;
import info.archinnov.achilles.codec.Codec;
import info.archinnov.achilles.exception.AchillesTranscodingException;

import static info.archinnov.achilles.test.integration.entity.ClusteredEntityWithTypeTransformer.TABLE_NAME;

@Entity(table = TABLE_NAME)
public class ClusteredEntityWithTypeTransformer {

    public static final String TABLE_NAME = "clustered_type_transformer";

    @EmbeddedId
    private ClusteredKey id;

    @Column
    private String value;

    public ClusteredEntityWithTypeTransformer() {
    }

    public ClusteredEntityWithTypeTransformer(Long id, MyCount count, String value) {
        this.id = new ClusteredKey(id, count);
        this.value = value;
    }


    public ClusteredKey getId() {
        return id;
    }

    public void setId(ClusteredKey id) {
        this.id = id;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }


    public static class MyCount {
        private int count;

        public MyCount(int count) {
            this.count = count;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            MyCount myCount = (MyCount) o;

            if (count != myCount.count) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return count;
        }
    }

    public static class ClusteredKey {
        @Column
        @Order(1)
        private Long id;

        @Column
        @Order(2)
        @TypeTransformer(valueCodecClass = CountToInt.class)
        private MyCount count;

        public ClusteredKey() {
        }

        public ClusteredKey(Long id, MyCount count) {
            this.id = id;
            this.count = count;
        }

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public MyCount getCount() {
            return count;
        }

        public void setCount(MyCount count) {
            this.count = count;
        }

    }

    public static class CountToInt implements Codec<MyCount, Integer> {

        @Override
        public Class<MyCount> sourceType() {
            return MyCount.class;
        }

        @Override
        public Class<Integer> targetType() {
            return Integer.class;
        }

        @Override
        public Integer encode(MyCount fromJava) throws AchillesTranscodingException {
            return fromJava.getCount();
        }

        @Override
        public MyCount decode(Integer fromCassandra) throws AchillesTranscodingException {
            return new MyCount(fromCassandra);
        }
    }

}
