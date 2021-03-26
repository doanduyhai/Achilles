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
package info.archinnov.achilles.type.interceptor;

import java.util.List;

/**

 * Interface to define an entity interceptor.
 * <br>
 * The "<em>boolean acceptEntity(Class&lt;?&gt; entityClass)</em>" method decides on which entity this interceptor
 * should trigger
 * <br>
 * <br>
 * The "<em>List&lt;Event&gt; interceptOnEvents()</em>" method returns a list of {@link Event}
 * in the entity lifecycle to be intercepted.
 * <br>
 * <br>
 * The "<em>void onEvent(T entity, Event event)</em>" method is called upon interception. This is where interception logic takes place.
 * A <em>raw</em> instance of the entity is injected as parameter for this method as well as the current Event. The raw instance state
 * may be modified according to some functional logic
 * <br>
 * <br>
 * <strong>Please note that during interception, the interceptor should NOT remove the primary key.
 * A verification is performed by Achilles after entity interception to guarantee that the primary key
 * and/or all its components are not null.
 * <br>
 * <br>
 * It is still possible to modify the primary key for POST_LOAD/POST_INSERT/POST_DELETE events, although it is strongly discouraged to do so
 * </strong>
 * <br>
 * <br>
 * Below is an example of an interceptor on the <em>User</em> entity to set the <em>biography</em>
 * field with "TO DO" when it is not set. This interception occurs before <strong>insert only</strong> operations
 * <br>
 * <br>
 * <pre class="code"><code class="java">
 * public class UserInterceptor extends Interceptor&lt;User&gt;
 * {
 * public boolean acceptEntity(Class&lt;?&gt; entityClass) {
 * return entityClass.equals(User.class);
 * }

 * public void onEvent(User entity, Event event) {
 * if(event == Event.PRE_INSERT && entity.getBiography() == null) {
 * entity.setBiography("TO DO");
 * }
 * }

 * public List&lt;Event&gt; interceptOnEvents() {
 * return Arrays.asList(PRE_INSERT,PRE_UPDATE);
 * }
 * }
 * </code></pre>
 *
 * @param <T> : type of entity to which this interceptor applies
 * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Interceptors" target="_blank">Interceptors</a>
 */
public interface Interceptor<T> {

    boolean acceptEntity(Class<?> entityClass);

    void onEvent(T entity, Event event);

    List<Event> interceptOnEvents();

}
