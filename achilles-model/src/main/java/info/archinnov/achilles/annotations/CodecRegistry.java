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
 * Marker annotation to be used on configuration class
 * for compile-time code generation. The type (class,
 * abstract class or interface) having this
 * annotation will expose a list of codecs to be used by
 * Achilles during source code parsing.
 * <br/>
 * Usage:
 * <br/>
 * <pre class="code"><code class="java">
 *
 * public class MyOwnType {...}
 * <strong>{@literal @}CodecRegistry</strong>
 * public [class | abstract class | interface] MyCodecRegistry {
 *
 *      //Source type = int, target type = String (according to IntToStringCodec codec)
 *      {@literal @}Codec(IntToStringCodec.class)
 *      private int intToString;
 *
 *      //Source type = MyOwnType, target type = String (according to MyOwnTypeToStringCodec codec)
 *      {@literal @}Codec(MyOwnTypeToStringCodec.class)
 *      private MyOwnType myOwnTypeToString;
 *
 *      //Source type = AnotherBean, target type = String (because of {@literal @}JSON)
 *      {@literal @}JSON
 *      private AnotherBean beanToJSON;
 *
 *      //Source type = MyEnum, target type = int (because of Encoding.ORDINAL)
 *      {@literal @}Enumerated(Encoding.ORDINAL)
 *      private MyEnum enumToOrdinal;
 * }
 * </code></pre>
 * <br/>
 *
 * <em>Note: it is possible to declare several codec registries in your source code,
 * just annotate them with {@literal @}CodecRegistry</em>
 * <br/><br/>
 * <strong>Warning: it is not possible to declare 2 different codecs for the same source type
 * for all registered codec registries.
 * Achilles will raise a compilation error when encountering such case.</strong> Ex:
 * <br/>
 * <br/>
 * <pre class="code"><code class="java">
 * <strong>{@literal @}CodecRegistry</strong>
 * public class MyCodecRegistry {
 *
 *      {@literal @}Codec(MyOwnTypeToStringCodec.class)
 *      private MyOwnType myOwnTypeToString;
 *
 *      <strong>// ERROR, not possible to have a 2nd codec for the same source type MyOwnType</strong>
 *      {@literal @}Codec(MyOwnTypeToBytesCodec.class)
 *      private MyOwnType myOwnTypeToBytes;
 * }
 * </code></pre>
 *
 * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Codec-System#codec-registry" target="_blank">Codec Registry</a>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
@Documented
public @interface CodecRegistry {
}
