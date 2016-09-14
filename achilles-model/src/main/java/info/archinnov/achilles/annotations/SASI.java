/*
 * Copyright (C) 2012-2016 DuyHai DOAN
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

package info.archinnov.achilles.annotations;

import java.lang.annotation.*;
import java.util.Locale;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.TYPE_USE})
@Documented
public @interface SASI {

    IndexMode indexMode() default IndexMode.PREFIX;

    boolean analyzed() default false;

    Analyzer analyzerClass() default Analyzer.NON_TOKENIZING_ANALYZER;

    int maxCompactionFlushMemoryInMb() default 1024;

    Normalization normanization() default Normalization.NONE;

    String locale() default "en";

    boolean enableStemming() default false;

    boolean skipStopWords() default false;

    enum IndexMode {
        PREFIX,
        CONTAINS,
        SPARSE
    }

    enum Analyzer {
        NON_TOKENIZING_ANALYZER,
        STANDARD_ANALYZER
    }

    enum Normalization {
        LOWERCASE,
        UPPERCASE,
        NONE
    }
}
