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

import static info.archinnov.achilles.test.integration.entity.CompositeClusteredEntity.TABLE_NAME;
import java.util.Comparator;
import java.util.Objects;

import info.archinnov.achilles.annotations.*;

@Entity(table = TABLE_NAME)
public class CompositeClusteredEntity {

	public static final String TABLE_NAME = "composite_clustered";

	@CompoundPrimaryKey
	private CompoundPK id;

	@Column
	private String value;

	public CompositeClusteredEntity() {
	}

	public CompositeClusteredEntity(Long id, String bucket, Integer count, String name, String value) {
		this.id = new CompoundPK(id, bucket, count,name);
		this.value = value;
	}

	public CompositeClusteredEntity(CompoundPK id, String value) {
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
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CompositeClusteredEntity that = (CompositeClusteredEntity) o;

        return Objects.equals(this.id,that.id) &&
                Objects.equals(this.value,that.value);
    }

    @Override
    public String toString() {
        return "CompositeClusteredEntity{" +
                "id=" + id +
                ", value='" + value + '\'' +
                '}';
    }

    @Override
    public int hashCode() {
       return Objects.hash(this.id,this.value);
    }

    public static class CompoundPK {
        @PartitionKey(1)
		private Long id;

        @PartitionKey(2)
        private String bucket;

        @ClusteringColumn(1)
		private Integer count;

        @ClusteringColumn(2)
		private String name;

		public CompoundPK() {
		}

		public CompoundPK(Long id, String bucket, Integer count, String name) {
			this.id = id;
            this.bucket = bucket;
            this.count = count;
			this.name = name;
		}

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getBucket() {
            return bucket;
        }

        public void setBucket(String bucket) {
            this.bucket = bucket;
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
                    ", bucket='" + bucket + '\'' +
                    ", count=" + count +
                    ", name='" + name + '\'' +
                    '}';
        }
    }

    public static class CompositeClusteredEntityComparator implements Comparator<CompositeClusteredEntity> {

        @Override
        public int compare(CompositeClusteredEntity o1, CompositeClusteredEntity o2) {
            int result = 0;
            if (o1.id.id.equals(o2.id.id)) {
                result = o1.id.bucket.compareTo(o2.id.bucket);
            } else {
                result = o1.id.id.compareTo(o2.id.id);
            }
            return result;
        }
    }

}
