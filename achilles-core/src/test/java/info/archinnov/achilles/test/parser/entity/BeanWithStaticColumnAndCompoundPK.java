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
package info.archinnov.achilles.test.parser.entity;

import info.archinnov.achilles.annotations.*;

@Entity
public class BeanWithStaticColumnAndCompoundPK {

    @CompoundPrimaryKey
    private CompoundPK id;

    @Column(staticColumn = true)
    private String name;

    public static class CompoundPK {

        @PartitionKey(1)
        private Long id;

        @PartitionKey(2)
        private int date;

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public int getDate() {
            return date;
        }

        public void setDate(int date) {
            this.date = date;
        }
    }

    public CompoundPK getId() {
        return id;
    }

    public void setId(CompoundPK id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
