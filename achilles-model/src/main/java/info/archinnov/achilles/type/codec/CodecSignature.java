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

package info.archinnov.achilles.type.codec;

import java.util.Objects;
import java.util.Optional;

import info.archinnov.achilles.validation.Validator;

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

    private final Class<FROM> sourceClass;
    private final Class<TO> targetClass;
    private Optional<String> codecName = Optional.empty();

    public CodecSignature(Class<FROM> sourceClass, Class<TO> targetClass) {
        this.sourceClass = sourceClass;
        this.targetClass = targetClass;
    }

    public CodecSignature(Class<FROM> sourceClass, Class<TO> targetClass, Optional<String> codecName) {
        this(sourceClass, targetClass);
        this.codecName = codecName;
    }

    public CodecSignature(Class<FROM> sourceClass, Class<TO> targetClass, String codecName) {
        this(sourceClass, targetClass);
        Validator.validateNotBlank(codecName, "codecName for CodecSignature should not be blank");
        this.codecName = Optional.of(codecName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CodecSignature<?, ?> that = (CodecSignature<?, ?>) o;
        return Objects.equals(sourceClass, that.sourceClass) &&
                Objects.equals(targetClass, that.targetClass) &&
                Objects.equals(codecName, that.codecName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceClass, targetClass, codecName);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CodecSignature{");
        sb.append("sourceClass=").append(sourceClass);
        sb.append(", targetClass=").append(targetClass);
        sb.append(", codecName=").append(codecName);
        sb.append('}');
        return sb.toString();
    }
}
