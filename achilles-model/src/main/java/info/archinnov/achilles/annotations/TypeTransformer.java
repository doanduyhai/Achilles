package info.archinnov.achilles.annotations;


import info.archinnov.achilles.codec.IdentityCodec;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * <p>
 * Transform a custom Java type into one of native types supported by the Java driver
 * <br/>
 * This annotation defines 2 attributes:
 * <ul>
 *     <li>keyCodecClass: class of Codec for Map key type transformation. <strong>Only useful for Map types</strong></li>
 *     <li>valueCodecClass: class of Codec for simple, List and Set type transformation. Please note that in case of List & Set, the transformation applies to the values inside the List/Set, not to the List/Set itself</li>
 * </ul>
 *
 * The Codec class provided should implement the {@link info.archinnov.achilles.codec.Codec} interface.
 *
 * <br/>
 * <br/>
 * Let's consider the following codec transforming a <strong>Long</strong> to a <strong>String</strong>
 * <pre class="code"><code class="java">
 *
 * public class LongToString implements Codec&lt;Long,String&gt; {
 *  {@literal @}Override
 *  public Class<Long> sourceType() {
 *      return Long.class;
 *  }
 *
 *  {@literal @}Override
 *  public Class<String> targetType() {
 *      return String.class;
 *  }
 *
 *  {@literal @}Override
 *  public String encode(Long fromJava) throws AchillesTranscodingException {
 *      return fromJava.toString();
 *  }
 *
 *  {@literal @}Override
 *  public Long decode(String fromCassandra) throws AchillesTranscodingException {
 *      return Long.parseLong(fromCassandra);
 *  }
 * }
 * </code></pre>
 * <br/>
 * Example of <strong>simple Long</strong> type to <strong>String</strong> type transformation
 * <pre class="code"><code class="java">
 *
 *   {@literal @}Column
 *   <strong>{@literal @}TypeTransformer(valueCodecClass = LongToString.class)</strong>
 *   private Long longToString;
 *
 * </code></pre>
 * <br/>
 * Example of <strong>List&lt;Long&gt;</strong> to <strong>List&lt;String&gt;</strong> transformation
 * <pre class="code"><code class="java">
 *
 *   {@literal @}Column
 *   <strong>{@literal @}TypeTransformer(valueCodecClass = LongToString.class)</strong>
 *   private List&lt;Long&gt; listOfLong;
 *
 * </code></pre>
 *
 * <br/>
 * Example of <strong>Set&lt;Long&gt;</strong> to <strong>Set&lt;String&gt;</strong> transformation
 * <pre class="code"><code class="java">
 *
 *   {@literal @}Column
 *   <strong>{@literal @}TypeTransformer(valueCodecClass = LongToString.class)</strong>
 *   private Set&lt;Long&gt; setOfLong;
 *
 * </code></pre>
 *
 * <br/>
 * Example of key Map transformation: <strong>Map&lt;Long,Double&gt;</strong> to <strong>Map&lt;String,Double&gt;</strong>
 * <pre class="code"><code class="java">
 *
 *   {@literal @}Column
 *   <strong>{@literal @}TypeTransformer(keyCodecClass = LongToString.class)</strong>
 *   private Map&lt;Long,Double&gt; mapKeyTransformation;
 *
 * </code></pre>
 *
 * <br/>
 * Example of value Map transformation: <strong>Map&lt;Integer,Long&gt;</strong> to <strong>Map&lt;Integer,String&gt;</strong>
 * <pre class="code"><code class="java">
 *
 *   {@literal @}Column
 *   <strong>{@literal @}TypeTransformer(valueCodecClass = LongToString.class)</strong>
 *   private Map&lt;Integer,Long&gt; mapValueTransformation;
 *
 * </code></pre>
 *
 * <br/>
 * Example of key/value Map transformation: <strong>Map&lt;Long,Long&gt;</strong> to <strong>Map&lt;String,String&gt;</strong>
 * <pre class="code"><code class="java">
 *
 *   {@literal @}Column
 *   <strong>{@literal @}TypeTransformer(keyCodecClass = LongToString.class, valueCodecClass = LongToString.class)</strong>
 *   private Map&lt;Long,Long&gt; mapKeyValueTransformation;
 *
 * </code></pre>
 * </p>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.FIELD })
@Documented
public @interface TypeTransformer {

    /**
     * Key Codec Impl class.The Codec class provided should implement the {@link info.archinnov.achilles.codec.Codec} interface. <br/>
     * <strong>This attribute should be only used on Map types and will be ignored if set on simple, list or set types</strong>
     *
     *  <br/>
     * <br/>
     * Let's consider the following codec transforming a <strong>Long</strong> to a <strong>String</strong>
     * <pre class="code"><code class="java">
     *
     * public class LongToString implements Codec&lt;Long,String&gt; {
     *  {@literal @}Override
     *  public Class<Long> sourceType() {
     *      return Long.class;
     *  }
     *
     *  {@literal @}Override
     *  public Class<String> targetType() {
     *      return String.class;
     *  }
     *
     *  {@literal @}Override
     *  public String encode(Long fromJava) throws AchillesTranscodingException {
     *      return fromJava.toString();
     *  }
     *
     *  {@literal @}Override
     *  public Long decode(String fromCassandra) throws AchillesTranscodingException {
     *      return Long.parseLong(fromCassandra);
     *  }
     * }
     * </code></pre>
     *
     *
     * <br/>
     * Example of key Map transformation: <strong>Map&lt;Long,Double&gt;</strong> to <strong>Map&lt;String,Double&gt;</strong>
     * <pre class="code"><code class="java">
     *
     *   {@literal @}Column
     *   <strong>{@literal @}TypeTransformer(keyCodecClass = LongToString.class)</strong>
     *   private Map<&lt;Long,Double&gt mapKeyTransformation;
     *
     * </code></pre>
     */
    Class<?> keyCodecClass() default IdentityCodec.class;

    /**
     * Value Codec Impl class. The Codec class provided should implement the {@link info.archinnov.achilles.codec.Codec} interface.
     *
     * <br/>
     * <br/>
     * Let's consider the following codec transforming a <strong>Long</strong> to a <strong>String</strong>
     * <pre class="code"><code class="java">
     *
     * public class LongToString implements Codec&lt;Long,String&gt; {
     *  {@literal @}Override
     *  public Class<Long> sourceType() {
     *      return Long.class;
     *  }
     *
     *  {@literal @}Override
     *  public Class<String> targetType() {
     *      return String.class;
     *  }
     *
     *  {@literal @}Override
     *  public String encode(Long fromJava) throws AchillesTranscodingException {
     *      return fromJava.toString();
     *  }
     *
     *  {@literal @}Override
     *  public Long decode(String fromCassandra) throws AchillesTranscodingException {
     *      return Long.parseLong(fromCassandra);
     *  }
     * }
     * </code></pre>
     * <br/>
     * Example of <strong>simple Long</strong> type to <strong>String</strong> type transformation
     * <pre class="code"><code class="java">
     *
     *   {@literal @}Column
     *   <strong>{@literal @}TypeTransformer(valueCodecClass = LongToString.class)</strong>
     *   private Long longToString;
     *
     * </code></pre>
     * <br/>
     * Example of <strong>List&lt;Long&gt;</strong> to <strong>List&lt;String&gt;</strong> transformation
     * <pre class="code"><code class="java">
     *
     *   {@literal @}Column
     *   <strong>{@literal @}TypeTransformer(valueCodecClass = LongToString.class)</strong>
     *   private List&lt;Long&gt listOfLong;
     *
     * </code></pre>
     *
     * <br/>
     * Example of <strong>Set&lt;Long&gt;</strong> to <strong>Set&lt;String&gt;</strong> transformation
     * <pre class="code"><code class="java">
     *
     *   {@literal @}Column
     *   <strong>{@literal @}TypeTransformer(valueCodecClass = LongToString.class)</strong>
     *   private Set&lt;Long&gt setOfLong;
     *
     * </code></pre>
     *
     * <br/>
     * Example of value Map transformation: <strong>Map&lt;Integer,Long&gt;</strong> to <strong>Map&lt;Integer,String&gt;</strong>
     * <pre class="code"><code class="java">
     *
     *   {@literal @}Column
     *   <strong>{@literal @}TypeTransformer(valueCodecClass = LongToString.class)</strong>
     *   private Map<&lt;Integer,Long&gt mapValueTransformation;
     *
     * </code></pre>
     */
    Class<?> valueCodecClass() default IdentityCodec.class;
}
