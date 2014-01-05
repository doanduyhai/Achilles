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
import java.util.Map.Entry;
import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.proxy.wrapper.EntryIteratorWrapper;

public class EntryIteratorWrapperBuilder extends AbstractWrapperBuilder<EntryIteratorWrapperBuilder> {
	private Iterator<Entry<Object, Object>> target;

	public static EntryIteratorWrapperBuilder builder(PersistenceContext context, Iterator<Entry<Object, Object>> target) {
		return new EntryIteratorWrapperBuilder(context, target);
	}

	public EntryIteratorWrapperBuilder(PersistenceContext context, Iterator<Entry<Object, Object>> target) {
		super.context = context;
		this.target = target;
	}

	public EntryIteratorWrapper build() {
		EntryIteratorWrapper iteratorWrapper = new EntryIteratorWrapper(this.target);
		super.build(iteratorWrapper);
		return iteratorWrapper;
	}
}
