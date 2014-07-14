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
package info.archinnov.achilles.type;

public enum BoundingMode {

	INCLUSIVE_BOUNDS(true, true),
    EXCLUSIVE_BOUNDS(false, false),
    INCLUSIVE_START_BOUND_ONLY(true, false),
    INCLUSIVE_END_BOUND_ONLY(false, true);

    private final boolean inclusiveStart;
    private final boolean inclusiveEnd;

    BoundingMode(boolean inclusiveStart, boolean inclusiveEnd) {
        this.inclusiveStart = inclusiveStart;
        this.inclusiveEnd = inclusiveEnd;
    }

    public boolean isInclusiveStart() {
        return inclusiveStart;
    }

    public boolean isInclusiveEnd() {
        return inclusiveEnd;
    }

}
