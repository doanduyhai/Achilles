/*
 * Copyright (C) 2012-2021 DuyHai DOAN
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

package info.archinnov.achilles.internals.types;

import java.util.Iterator;
import java.util.LinkedList;

public class ResultLoggingIteratorWrapper<E> implements Iterator<E> {

    private final LinkedList<E> values;
    private final Iterator<E> delegate;

    public ResultLoggingIteratorWrapper(LinkedList<E> values, Iterator<E> delegate) {
        this.values = values;
        this.delegate = delegate;
    }

    @Override
    public boolean hasNext() {
        return values.size() > 0 || delegate.hasNext();
    }

    @Override
    public E next() {
        if (values.size() > 0) {
            return values.poll();
        } else {
            return delegate.next();
        }
    }
}
