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

package info.archinnov.achilles.internals.utils;

import java.util.*;

public class CollectionsHelper {

    public static <T> List<T> appendAll(List<T>... lists) {
        final List<T> newList = new ArrayList<>();
        Arrays.asList(lists).forEach(newList::addAll);
        return newList;
    }

    public static <T> Set<T> appendAll(Set<T> ... sets) {
        final Set<T> newSet = new HashSet<>();
        Arrays.asList(sets).forEach(newSet::addAll);
        return newSet;
    }
}
