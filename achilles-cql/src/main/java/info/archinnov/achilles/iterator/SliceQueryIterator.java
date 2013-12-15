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
package info.archinnov.achilles.iterator;

import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.entity.EntityMapper;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.operations.EntityProxifier;
import info.archinnov.achilles.proxy.RowMethodInvoker;
import info.archinnov.achilles.query.slice.CQLSliceQuery;

import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;

public class SliceQueryIterator<T> implements Iterator<T> {

    private static final Logger log  = LoggerFactory.getLogger(SliceQueryIterator.class);

    private PersistenceContext context;
	private Iterator<Row> iterator;
	private EntityMeta meta;

	private EntityMapper mapper = new EntityMapper();
	private EntityProxifier proxifier = new EntityProxifier();

	public SliceQueryIterator(CQLSliceQuery<T> sliceQuery, PersistenceContext context, Iterator<Row> iterator) {
		this.context = context;
		this.iterator = iterator;
		this.meta = sliceQuery.getMeta();
	}

	@Override
	public boolean hasNext() {
        final boolean hasNext = iterator.hasNext();
        log.trace("Does iterator has more element ? {}",hasNext);
        return hasNext;
	}

	@Override
	public T next() {
        log.trace("Fetch iterator next element");
        T clusteredEntity = null;
        Row row = iterator.next();
        if(row != null) {
            clusteredEntity = meta.instanciate();
            mapper.setEagerPropertiesToEntity(row, meta, clusteredEntity);
            clusteredEntity = proxify(clusteredEntity);
        }
		return clusteredEntity;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException("Cannot remove clustered entity with iterator");
	}

	private T proxify(T clusteredEntity) {
		PersistenceContext duplicate = context.duplicate(clusteredEntity);
		return proxifier.buildProxy(clusteredEntity, duplicate);
	}

}
