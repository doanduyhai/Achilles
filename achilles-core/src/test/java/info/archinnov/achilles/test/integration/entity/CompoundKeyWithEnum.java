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
package info.archinnov.achilles.test.integration.entity;

import info.archinnov.achilles.annotations.Order;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

public class CompoundKeyWithEnum {

	private Long index;

	private Type type;

	@JsonCreator
	public CompoundKeyWithEnum(@Order(1) @JsonProperty("index") Long index,
			@Order(2) @JsonProperty("type") Type type) {
		this.index = index;
		this.type = type;
	}

	public Long getIndex() {
		return index;
	}

	public Type getType() {
		return type;
	}

	public static enum Type {
		AUDIO, IMAGE, FILE;
	}
}
