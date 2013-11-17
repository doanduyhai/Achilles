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

import java.util.Iterator;
import info.archinnov.achilles.context.CQLPersistenceContext;
import info.archinnov.achilles.proxy.wrapper.IteratorWrapper;

public class IteratorWrapperBuilder extends AbstractWrapperBuilder<IteratorWrapperBuilder> {
	private Iterator<Object> target;

	public static IteratorWrapperBuilder builder(CQLPersistenceContext context, Iterator<Object> target) {
		return new IteratorWrapperBuilder(context, target);
	}

	public IteratorWrapperBuilder(CQLPersistenceContext context, Iterator<Object> target) {
		super.context = context;
		this.target = target;
	}

	public IteratorWrapper build() {
		IteratorWrapper iteratorWrapper = new IteratorWrapper(this.target);
		super.build(iteratorWrapper);
		return iteratorWrapper;
	}
}
