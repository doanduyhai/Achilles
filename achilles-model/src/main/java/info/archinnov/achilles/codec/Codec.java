package info.archinnov.achilles.codec;

import info.archinnov.achilles.exception.AchillesTranscodingException;

/**
 * Define a codec to transform a source type into a target type. <br/>
 * The source type can be any Java type. The target type should be a Java type supported by Cassandra <br/>
 * <br/>
 * <br/>
 *
 * Example of <strong>LongToString</strong> codec
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
 * @param <FROM> sourceType
 * @param <TO> targetType compatible with Cassandra
 */
public interface Codec<FROM, TO> {

    Class<FROM> sourceType();

    Class<TO> targetType();

    TO encode(FROM fromJava) throws AchillesTranscodingException;

    FROM decode(TO fromCassandra) throws AchillesTranscodingException;
}
