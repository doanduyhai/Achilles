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
package info.archinnov.achilles.json;


import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Default Jackson object mapper factory if none is configured with parameter {@link info.archinnov.achilles.configuration.ConfigurationParameters}.JACKSON_MAPPER_FACTORY

 * The object mapper is configured by default as follow:

 * <pre class="code"><code class="java">

 * ObjectMapper mapper = new ObjectMapper();
 * <strong>

 * mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
 * mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
 * AnnotationIntrospector primary = new JacksonAnnotationIntrospector();
 * AnnotationIntrospector secondary = new JaxbAnnotationIntrospector(TypeFactory.defaultInstance());
 * mapper.setAnnotationIntrospector(AnnotationIntrospector.pair(primary, secondary));
 * </strong>
 * </code></pre>
 */
public class DefaultJacksonMapperFactory implements JacksonMapperFactory {

    @Override
    public <T> ObjectMapper getMapper(Class<T> type) {
        return DefaultJacksonMapper.DEFAULT.get();
    }

}
