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

public class NamingHelper {

    public static String upperCaseFirst(String name) {
        return name.substring(0, 1).toUpperCase() + name.substring(1);
    }

    public static String maybeQuote(String rawName) {
        String toLower = rawName.toLowerCase();
        return toLower.equals(rawName)
                && !rawName.trim().startsWith("\"")
                && !rawName.trim().endsWith("\"")
                ? rawName
                : "\"" + rawName + "\"";
    }
}
