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

package info.archinnov.achilles.annotations;

import java.lang.annotation.*;

/**
 * Annotation for SASI index
 * <br/>
 * <br/>
 * The following combinations are allowed for index options:
 * <br/>
 * <table>
 *     <thead>
 *         <tr>
 *             <th>Data type</th>
 *             <th>Index Mode</th>
 *             <th>Analyzer Class</th>
 *             <th>Possible option values</th>
 *         </tr>
 *     </thead>
 *     <tbody>
 *         <tr>
 *             <td>Text or Ascii</td>
 *             <td>PREFIX or CONTAINS</td>
 *             <td>NoOpAnalyzer</td>
 *             <td>
 *                 <ul>
 *                     <li>analyzed = <strong>false</strong> (DEFAULT)</li>
 *                     <li>normalization = <strong>NONE</strong> (DEFAULT)</li>
 *                     <li>locale is <strong>ignored</strong></li>
 *                     <li>maxCompactionFlushMemoryInMb (OPTIONAL)</li>
 *                     <li>enableStemming = <strong>false</strong> (DEFAULT)</li>
 *                     <li>skipStopWords = <strong>false</strong> (DEFAULT)</li>
 *                 </ul>
 *             </td>
 *         </tr>
 *         <tr>
 *             <td>Text or Ascii</td>
 *             <td>PREFIX or CONTAINS</td>
 *             <td>NonTokenizingAnalyzer</td>
 *             <td>
 *                 <ul>
 *                     <li>analyzed = <strong>true</strong> (MANDATORY)</li>
 *                     <li>normalization (OPTIONAL)</li>
 *                     <li>locale (OPTIONAL)</li>
 *                     <li>maxCompactionFlushMemoryInMb (OPTIONAL)</li>
 *                     <li>enableStemming = <strong>false</strong> (DEFAULT)</li>
 *                     <li>skipStopWords = <strong>false</strong> (DEFAULT)</li>
 *                 </ul>
 *             </td>
 *         </tr>
 *         <tr>
 *             <td>Text or Ascii</td>
 *             <td>PREFIX or CONTAINS</td>
 *             <td>StandardAnalyzer</td>
 *             <td>
 *                 <ul>
 *                     <li>analyzed = <strong>true</strong> (MANDATORY)</li>
 *                     <li>normalization (OPTIONAL)</li>
 *                     <li>locale (OPTIONAL)</li>
 *                     <li>maxCompactionFlushMemoryInMb (OPTIONAL)</li>
 *                     <li>enableStemming = <strong>false</strong> (DEFAULT)</li>
 *                     <li>skipStopWords  = <strong>false</strong> (DEFAULT)</li>
 *                 </ul>
 *             </td>
 *         </tr>
 *         <tr>
 *             <td>Non Text</td>
 *             <td>PREFIX OR SPARSE</td>
 *             <td>NoOpAnalyzer</td>
 *             <td>
 *                 <ul>
 *                     <li>analyzed = <strong>false</strong> (DEFAULT)</li>
 *                     <li>normalization = <strong>NONE</strong> (DEFAULT)</li>
 *                     <li>locale is <strong>ignored</strong></li>
 *                     <li>maxCompactionFlushMemoryInMb (OPTIONAL)</li>
 *                     <li>enableStemming = <strong>false</strong> (DEFAULT)</li>
 *                     <li>skipStopWords = <strong>false</strong> (DEFAULT)</li>
 *                 </ul>
 *             </td>
 *         </tr>
 *     </tbody>
 * </table>
 *<br/>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
@Documented
public @interface SASI {

    /**
     * <strong>Optional</strong>.
     * Define the name of the SASI index. If not set, defaults to <strong>table name_field name_index</strong>
     * <pre class="code"><code class="java">
     * {@literal @}Table
     * public class User {
     * ...
     * {@literal @}SASI(<strong>name = "country_code_index"</strong>)
     * {@literal @}Column
     * private String countryCode;
     * ...
     * }
     * </code></pre>
     * </p>
     * If the index name was not set above, it would default to <string>user_countrycode_index</string>
     */
    String name() default "";

    /**
     * SASI index mode. Allowed values are:
     * <ul>
     *     <li><strong>PREFIX</strong> (DEFAULT): allows search on prefix for text/ascii data types.
     *     Default and only valid index mode for non-text data types</li>
     *     <li><strong>CONTAINS</strong>: allows search on prefix, suffix and substring for text/ascii data types.
     *     Invalid for non-text data types</li>
     *     <li><strong>SPARSE</strong>: only valid for non-text data types.
     *     SPARSE mode is optimized for low-cardinality e.g. for indexed values having
     *     <strong>5 or less</strong> corresponding rows. If there are more than 5 CQL rows having
     *     this index value, SASI will complain by throwing an exception</li>
     * </ul>
     *
     */
    IndexMode indexMode() default IndexMode.PREFIX;

    /**
     * Indicates whether the data should be analyzed or not.
     * <br/>
     * <br/>
     * Setting 'analyzed' = true is only valid for text/ascii data types.
     * <br/>
     * <br/>
     * Setting 'analyzed' = true is <strong>mandatory</strong> if 'analyzerClass' is set to:
     * <ul>
     *     <li><strong>NON_TOKENIZING_ANALYZER</strong></li>
     *     <li><strong>STANDARD_ANALYZER</strong></li>
     * </ul>
     */
    boolean analyzed() default false;

    /**
     * Defines the analyzer class. Available values are:
     * <ul>
     *     <li><strong>NO_OP_ANALYSER</strong> (DEFAULT): do not analyze the input</li>
     *     <li><strong>NON_TOKENIZING_ANALYZER</strong>: only valid for text/ascii data types.
     *     Do not tokenize the input.
     *     Normalization by lowercase/uppercase is allowed</li>
     *     <li><strong>STANDARD_ANALYZER</strong>: only valid for text/ascii data types.
     *     Split the input text into tokens, using the locale defined by attribute 'locale'
     *     Normalization by lowercase/uppercase is allowed</li>
     * </ul>
     * <br/>
     * <br/>
     * <strong>
     * Please note that setting 'analyzerClass' to NON_TOKENIZING_ANALYZER or STANDARD_ANALYZER
     * also requires setting 'analyzed' to true
     * </strong>
     */
    Analyzer analyzerClass() default Analyzer.NO_OP_ANALYZER;

    /**
     * Maximum size of SASI data to keep in memory during compaction process.
     * <br/>
     * <br/>
     * Default = 1024 e.g. 1Gb
     * <br/>
     * <br/>
     * If there are more than 'maxCompactionFlushMemoryInMb' worth of index data, SASI
     * will flush them on temporary files on disk before merging all the temp files into
     * a single one. Of course it will add up to compaction duration. No free lunch, sorry
     */
    int maxCompactionFlushMemoryInMb() default 1024;

    /**
     * Defines the normalization to be applied to the input. Available values are:
     * <ul>
     *     <li><strong>NONE</strong> (DEFAULT): no normalization</li>
     *     <li><strong>LOWERCASE</strong>: normalize input text and search term to lower case</li>
     *     <li><strong>UPPERCASE</strong>: normalize input text and search term to upper case</li>
     * </ul>
     */
    Normalization normalization() default Normalization.NONE;

    /**
     * Defines the locale for tokenization. This attribute is only used when
     * 'analyzerClass' == STANDARD_ANALYZER otherwise it is ignored
     */
    String locale() default "en";

    /**
     * Enable stemming of input text. This attribute is only used when
     * 'analyzerClass' == STANDARD_ANALYZER
     */
    boolean enableStemming() default false;

    /**
     * Enable stemming of input text. This attribute is only used when
     * 'analyzerClass' == STANDARD_ANALYZER
     */
    boolean skipStopWords() default false;

    enum IndexMode {
        PREFIX,
        CONTAINS,
        SPARSE
    }

    enum Analyzer {
        NO_OP_ANALYZER("org.apache.cassandra.index.sasi.analyzer.NoOpAnalyzer"),
        NON_TOKENIZING_ANALYZER("org.apache.cassandra.index.sasi.analyzer.NonTokenizingAnalyzer"),
        STANDARD_ANALYZER("org.apache.cassandra.index.sasi.analyzer.StandardAnalyzer");

        private String analyzerClass;

        Analyzer(String analyzerClass) {
            this.analyzerClass = analyzerClass;
        }

        public String analyzerClass() {
            return analyzerClass;
        }
    }

    enum Normalization {
        LOWERCASE {
            @Override
            public String forStandardAnalyzer() {
                return "tokenization_normalize_lowercase";
            }

            @Override
            public String forNonTokenizingAnalyzer() {
                return "normalize_lowercase";
            }
        },
        UPPERCASE {
            @Override
            public String forStandardAnalyzer() {
                return "tokenization_normalize_uppercase";
            }

            @Override
            public String forNonTokenizingAnalyzer() {
                return "normalize_uppercase";
            }
        },
        NONE {
            @Override
            public String forStandardAnalyzer() {
                return "";
            }

            @Override
            public String forNonTokenizingAnalyzer() {
                return "";
            }
        };

        abstract public String forStandardAnalyzer();
        abstract public String forNonTokenizingAnalyzer();
    }
}
