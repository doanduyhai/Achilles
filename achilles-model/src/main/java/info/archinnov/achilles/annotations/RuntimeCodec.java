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
 *
 * Transform a custom Java type into one of native types supported by the Java driver.
 * Normally you'll use the {@literal @}Codec annotation and provide a codec class
 * but if your codec class is stateful or its construction needs some external dependencies
 * and cannot be instantiated using the default no-args constructor, you can register
 * the codec using this annotation and build it at runtime before injecting it into Achilles
 *
 * <br/>
 * Ex:
 * <pre class="code"><code class="java">
 * //Compile time
 * {@literal @}Column
 * <strong>{@literal @}RuntimeCodec(cqlClass = String.class)</strong>
 * private MyBean bean;
 *
 * //Runtime
 * final Cluster cluster = .... // Create Java driver cluster object
 * <strong>final Codec&lt;MyBean, String&gt; statefulCodec = new .... // Create your codec with initialization logic here</strong>
 * final CodecSignature&lt;MyBean, String&gt; codecSignature = new CodecSignature(MyBean.class, String.class);
 *
 * ManagerFactory factory = ManagerFactoryBuilder
 *                              .builder(cluster)
 *                              ...
 *                              <strong>.withRuntimeCodec(codecSignature, codec)</strong>
 *                              .build();
 *
 * </code></pre>
 *
 * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Codec-System#runtime-codec" target="_blank">Runtime Codec</a>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.TYPE_USE, ElementType.TYPE_PARAMETER})
@Documented
public @interface RuntimeCodec {

    /**
     * To help Achilles look up for your codec at bootstrap time and distinguish it from
     * other codecs having same source and target types, you can provide an unique name here
     */
    String codecName() default "";

    /**
     * <strong>Mandatory</strong>. To help Achilles determine the CQL type at compile time, you have to specify
     * the target Cassandra Java data type. This type should be a CQL-compatible type.
     */
    Class<?> cqlClass();
}
