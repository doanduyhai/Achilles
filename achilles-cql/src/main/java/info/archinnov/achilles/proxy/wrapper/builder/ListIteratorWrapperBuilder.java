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

import java.util.ListIterator;
import info.archinnov.achilles.context.CQLPersistenceContext;
import info.archinnov.achilles.proxy.wrapper.ListIteratorWrapper;

public class ListIteratorWrapperBuilder extends AbstractWrapperBuilder<ListIteratorWrapperBuilder> {
	private ListIterator<Object> target;

	public static ListIteratorWrapperBuilder builder(CQLPersistenceContext context, ListIterator<Object> target) {
		return new ListIteratorWrapperBuilder(context, target);
	}

	public ListIteratorWrapperBuilder(CQLPersistenceContext context, ListIterator<Object> target) {
		super.context = context;
		this.target = target;
	}

	public ListIteratorWrapper build() {
		ListIteratorWrapper listIteratorWrapper = new ListIteratorWrapper(this.target);
		super.build(listIteratorWrapper);
		return listIteratorWrapper;
	}

}
