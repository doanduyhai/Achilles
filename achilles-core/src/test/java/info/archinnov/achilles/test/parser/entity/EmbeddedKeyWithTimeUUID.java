/**
 *
 * Copyright (C) 2012-2013 DuyHai DOAN
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package info.archinnov.achilles.test.parser.entity;

import info.archinnov.achilles.annotations.Order;
import info.archinnov.achilles.annotations.TimeUUID;

import java.util.UUID;

import javax.persistence.Column;

public class EmbeddedKeyWithTimeUUID {

	@TimeUUID
	@Order(1)
	@Column
	private UUID date;

	@Order(2)
	@Column(name = "ranking")
	private int rank;

	public UUID getDate() {
		return date;
	}

	public void setDate(UUID date) {
		this.date = date;
	}

	public int getRank() {
		return rank;
	}

	public void setRank(int rank) {
		this.rank = rank;
	}
}
