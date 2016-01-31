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

package info.archinnov.achilles.type.codec;

import java.util.Optional;

/**
 * Class to define a signature for a {@literal @}RuntimeCodec
 *
 * Usage:
 *
 * <pre class="code"><code class="java">
 * //Compile time
 * {@literal @}Column
 * {@literal @}RuntimeCodec(cqlClass = String.class)
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
 *                              .withRuntimeCodec(codecSignature, codec)
 *                              .build();
 *
 * </code></pre>
 *
 * @param <FROM> Java source class
 * @param <TO>  CQL-compatible target class
 */
public class CodecSignature<FROM, TO> {

    private final FROM sourceClass;
    private final TO targetClass;
    private Optional<String> name = Optional.empty();

    public CodecSignature(FROM sourceClass, TO targetClass) {
        this.sourceClass = sourceClass;
        this.targetClass = targetClass;
    }

    public CodecSignature(FROM sourceClass, TO targetClass, Optional<String> name) {
        this(sourceClass, targetClass);
        this.name = name;
    }
}
