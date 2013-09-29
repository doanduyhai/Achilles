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
package info.archinnov.achilles.proxy.wrapper.builder;

import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.proxy.wrapper.ValueCollectionWrapper;

import java.util.Collection;

public class ValueCollectionWrapperBuilder extends AbstractWrapperBuilder<ValueCollectionWrapperBuilder> {
	private Collection<Object> target;

	public ValueCollectionWrapperBuilder(PersistenceContext context, Collection<Object> target) {
		super.context = context;
		this.target = target;
	}

	public static ValueCollectionWrapperBuilder builder(PersistenceContext context, Collection<Object> target) {
		return new ValueCollectionWrapperBuilder(context, target);
	}

	public ValueCollectionWrapper build() {
		ValueCollectionWrapper valueCollectionWrapper = new ValueCollectionWrapper(this.target);
		super.build(valueCollectionWrapper);
		return valueCollectionWrapper;
	}

}
