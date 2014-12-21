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

import static info.archinnov.achilles.test.integration.entity.ClusteredEntity.TABLE_NAME;
import java.util.Comparator;
import java.util.Objects;

import info.archinnov.achilles.annotations.*;

@Entity(table = TABLE_NAME)
public class ClusteredEntity {

    public static final String TABLE_NAME = "clustered";

    @CompoundPrimaryKey
    private CompoundPK id;

    @Column
    private String value;

    public ClusteredEntity() {
    }

    public ClusteredEntity(Long id, Integer count, String name, String value) {
        this.id = new CompoundPK(id, count, name);
        this.value = value;
    }

    public ClusteredEntity(CompoundPK id, String value) {
        this.id = id;
        this.value = value;
    }

    public CompoundPK getId() {
        return id;
    }

    public void setId(CompoundPK id) {
        this.id = id;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.id, this.value);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ClusteredEntity other = (ClusteredEntity) obj;

        return Objects.equals(this.id, other.id)
                && Objects.equals(this.value, other.value);
    }

    @Override
    public String toString() {
        return "ClusteredEntity{" +
                "id=" + id +
                ", value='" + value + '\'' +
                '}';
    }

    public static class CompoundPK {
        @PartitionKey
        private Long id;

        @ClusteringColumn(1)
        private Integer count;

        @ClusteringColumn(2)
        private String name;

        public CompoundPK() {
        }

        public CompoundPK(Long id, Integer count, String name) {
            this.id = id;
            this.count = count;
            this.name = name;
        }

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public Integer getCount() {
            return count;
        }

        public void setCount(Integer count) {
            this.count = count;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return "ClusteredKey{" +
                    "id=" + id +
                    ", count=" + count +
                    ", name='" + name + '\'' +
                    '}';
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.id, this.count, this.name);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            CompoundPK other = (CompoundPK) obj;

            return Objects.equals(this.id, other.id) &&
                    Objects.equals(this.count, other.count) &&
                    Objects.equals(this.name, other.name);
        }

    }

    public static class ClusteredEntityComparator implements Comparator<ClusteredEntity> {

        @Override
        public int compare(ClusteredEntity o1, ClusteredEntity o2) {
            return o1.id.id.compareTo(o2.id.id);
        }
    }

}
