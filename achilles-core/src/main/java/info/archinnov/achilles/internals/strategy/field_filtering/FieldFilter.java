/*
 * Copyright (C) 2012-2015 DuyHai DOAN
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

package info.archinnov.achilles.internals.strategy.field_filtering;

import java.util.function.Predicate;
import javax.lang.model.element.VariableElement;

import info.archinnov.achilles.annotations.ClusteringColumn;
import info.archinnov.achilles.annotations.Column;
import info.archinnov.achilles.annotations.PartitionKey;
import info.archinnov.achilles.annotations.Transient;

@FunctionalInterface
public interface FieldFilter extends Predicate<VariableElement> {

    FieldFilter EXPLICIT_ENTITY_FIELD_FILTER = elm ->
            (elm.getAnnotation(Column.class) != null ||
                    elm.getAnnotation(PartitionKey.class) != null ||
                    elm.getAnnotation(ClusteringColumn.class) != null)
                    && (elm.getAnnotation(Transient.class) == null);
    FieldFilter IMPLICIT_ENTITY_FIELD_FILTER = elm ->
            elm.getAnnotation(Transient.class) == null;
    FieldFilter EXPLICIT_UDT_FIELD_FILTER = elm ->
            (elm.getAnnotation(Column.class) != null
                    && elm.getAnnotation(Transient.class) == null);
    FieldFilter IMPLICIT_UDT_FIELD_FILTER = elm ->
            (elm.getAnnotation(Column.class) != null
                    && elm.getAnnotation(Transient.class) == null);

    @Override
    boolean test(VariableElement element);
}
