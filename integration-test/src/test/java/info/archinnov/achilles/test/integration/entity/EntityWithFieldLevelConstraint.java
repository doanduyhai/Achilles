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

import static info.archinnov.achilles.test.integration.entity.EntityWithFieldLevelConstraint.TABLE_NAME;
import info.archinnov.achilles.annotations.Column;
import info.archinnov.achilles.annotations.Entity;
import info.archinnov.achilles.annotations.PartitionKey;

import javax.validation.constraints.NotNull;

@Entity(table = TABLE_NAME)
public class EntityWithFieldLevelConstraint {
	public static final String TABLE_NAME = "entity_field_constrained";

	@PartitionKey
	private Long id;

	@Column
	@NotNull
	private String name;

    @Column
    private int age;

    public EntityWithFieldLevelConstraint() {
    }

    public EntityWithFieldLevelConstraint(Long id, String name, int age) {
        this.id = id;
        this.name = name;
        this.age = age;
    }

    public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }
}
