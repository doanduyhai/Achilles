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
package info.archinnov.achilles.test.builders;

import java.util.Arrays;
import java.util.List;

import me.prettyprint.cassandra.serializers.SerializerTypeInferer;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.Composite;

import org.apache.commons.collections.CollectionUtils;

public class CompositeTestBuilder {

	private List<?> values;
	private ComponentEquality equality;

	public static CompositeTestBuilder builder() {
		return new CompositeTestBuilder();
	}

	public Composite buildForQuery() {
		Composite built = new Composite();
		if (!CollectionUtils.isEmpty(values)) {
			for (int i = 0; i < values.size(); i++) {
				ComponentEquality eq;
				if (i != values.size() - 1) {
					eq = ComponentEquality.EQUAL;
				} else {
					eq = equality;

				}
				built.addComponent(i, values.get(i), eq);
			}
		}
		return built;
	}

	public Composite buildSimple() {
		Composite built = new Composite();
		if (!CollectionUtils.isEmpty(values)) {
			for (int i = 0; i < values.size(); i++) {
				Object value = values.get(i);
				built.setComponent(i, value,
						SerializerTypeInferer.getSerializer(value));
			}
		}
		return built;
	}

	public CompositeTestBuilder values(Object... values) {
		this.values = Arrays.asList(values);
		return this;
	}

	public CompositeTestBuilder equality(ComponentEquality equality) {
		this.equality = equality;
		return this;
	}

	public CompositeTestBuilder gt() {
		this.equality = ComponentEquality.GREATER_THAN_EQUAL;
		return this;
	}

	public CompositeTestBuilder lt() {
		this.equality = ComponentEquality.LESS_THAN_EQUAL;
		return this;
	}
}
