/*
 * Copyright (C) 2012-2014 DuyHai DOAN
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package info.archinnov.achilles.internal.statement.cache;

public enum CacheType {
    ASSIGN_VALUE_TO_LIST,
    ASSIGN_VALUE_TO_SET,
    ASSIGN_VALUE_TO_MAP,
    REMOVE_COLLECTION_OR_MAP,
    ADD_TO_SET,
    REMOVE_FROM_SET,
    APPEND_TO_LIST,
    PREPEND_TO_LIST,
    REMOVE_FROM_LIST,
    SET_TO_LIST_AT_INDEX,
    REMOVE_FROM_LIST_AT_INDEX,
    ADD_TO_MAP,
    REMOVE_FROM_MAP,
    SELECT_FIELD,
    UPDATE_FIELDS,
    INSERT,
    SLICE_QUERY_SELECT,
    SLICE_QUERY_DELETE,
    DELETE_PARTITION;
}
