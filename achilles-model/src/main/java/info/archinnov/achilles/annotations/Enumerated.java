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

 * Annotation for enum type column. Example
 * <pre class="code"><code class="java">

 * {@literal @}Column
 * <strong>{@literal @}Enumerated(Encoding.ORDINAL)</strong>
 * private Pricing pricing;
 * </code></pre>

 * It can be also applied to <strong>nested</strong> enums too:
 * <pre class="code"><code class="java">

 * {@literal @}Column
 * private List&lt;{@literal @}Enumerated(<strong>Encoding.ORDINAL</strong>) Pricing&gt; pricingTypes;
 * </code></pre>
 * </p>
 *
 * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Achilles-Annotations#enumerated" target="_blank">@Column</a>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.TYPE_USE, ElementType.TYPE_PARAMETER})
@Documented
public @interface Enumerated {

    /**
     * (<strong>Optional</strong>) The encoding type for this value. Default value is <strong>Encoding.NAME</strong> e.g. enum value will be converted
     * to string by calling  the <em>.name()</em> method.
     * <br/>
     * <br/>
     * Alternatively it is possible to encode using <strong>Encoding.ORDINAL</strong>. In this case enum value will be saved using the ordinal value
     * (order in which each enum is declared in the source class). Please note that the ordinal is <strong>0-based</strong>

     * <pre class="code"><code class="java">

     * {@literal @}Enumerated(<strong>Encoding.ORDINAL</strong>)
     * private Pricing pricingType;
     * </code></pre>
     */
    Encoding value() default Encoding.NAME;

    enum Encoding {
        NAME, ORDINAL
    }
}
