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

package info.archinnov.achilles.annotations;

import java.lang.annotation.*;

/**
 * <p>
 * Annotation for enum type column. Example
 *
 * <pre class="code"><code class="java">
 *
 *   <strong>{@literal @}Enumerated(Encoding.ORDINAL)</strong>
 *   private Pricing pricing;
 *
 * </code></pre>
 *
 * If used on a collection type ({@code java.util.List} or {@code java.util.Set}), the encoding type applies to the type of the collection. Example:
 *
 * <pre class="code"><code class="java">
 *
 *   {@literal @}Enumerated(<strong>Encoding.ORDINAL</strong>)
 *   private List&lt;Pricing&gt; pricingTypes;
 *
 * </code></pre>
 *
 * If used on a map type, the <em>value()</em> attribute applies to the map value type and the <em>key()</em> attribute applies to map key type. Example:
 *
 * <pre class="code"><code class="java">
 *
 *   // Country encoded to Integer, Pricing encoded to String
 *   {@literal @}Enumerated(<strong>key = Encoding.ORDINAL</strong>, <strong>value = Encoding.NAME</strong>)
 *   private Map&lt;Country,Pricing&gt; pricingByCountry;
 *
 * </code></pre>
 *
 * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Achilles-Annotations#enumerated" target="_blank">@Column</a>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
@Documented
public @interface Enumerated {

	/**
	 * (<strong>Optional</strong>) The encoding type for this value. Default value is <strong>Encoding.NAME</strong> e.g. enum value will be converted
     * to string by calling  the <em>.name()</em> method.
     *
     * <br/>
     * <br/>
     * Alternatively it is possible to encode using <strong>Encoding.ORDINAL</strong>. In this case enum value will be saved using the ordinal value
     * (order in which each enum is declared in the source class). Please note that the ordinal is <strong>0-based</strong>
     *
     * <pre class="code"><code class="java">
     *
     *   {@literal @}Enumerated(<strong>Encoding.ORDINAL</strong>)
     *   private Pricing pricingType;
     *
     * </code></pre>
     *
     * If used on a collection type ({@code java.util.List} or {@code java.util.Set}), the encoding type applies to the type of the collection. Example:
     *
     * <pre class="code"><code class="java">
     *
     *   {@literal @}Enumerated(<strong>Encoding.ORDINAL</strong>)
     *   private List&lt;Pricing&gt; pricingTypes;
     *
     * </code></pre>
     *
	 */
	Encoding value() default Encoding.NAME;

    /**
     * (<strong>Optional</strong>) The encoding type for this key (applies only to map types). Default value is <strong>Encoding.NAME</strong> e.g. enum value will be converted
     * to string by calling  the <em>.name()</em> method.
     *
     * <br/>
     * <br/>
     * Alternatively it is possible to encode using <strong>Encoding.ORDINAL</strong>. In this case enum value will be saved using the ordinal value
     * (order in which each enum is declared in the source class). Please note that the ordinal is <strong>0-based</strong>
     *
     * <pre class="code"><code class="java">
     *
     *   // Country encoded to Integer, Pricing encoded to String
     *   {@literal @}Enumerated(<strong>key = Encoding.ORDINAL</strong>, <strong>value = Encoding.NAME</strong>)
     *   private Map&lt;Country,Pricing&gt; pricingByCountry;
     *
     * </code></pre>
     *
     */
    Encoding key() default  Encoding.NAME;

    /**
     * Encoding method for enum value. Possible choices:
     *
     * <ul>
     *     <li>NAME: encode by enum name (type = <strong>String</strong>)</li>
     *     <li>ORDINAL: encode by enum ordinal (type = <strong>Integer</strong>)</li>
     * </ul>
     */
    public static enum Encoding {
        NAME, ORDINAL;
    }
}
