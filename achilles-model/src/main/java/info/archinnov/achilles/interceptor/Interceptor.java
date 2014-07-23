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
package info.archinnov.achilles.interceptor;

import java.util.List;

/**
 * <p/>
 * Interface to define an entity interceptor.
 * <br>
 * The "<em>List&lt;Event&gt; events()</em>" method returns a list of {@link info.archinnov.achilles.interceptor.Event}
 * in the entity lifecycle to be intercepted.
 * <br>
 * <br>
 * The "<em>void onEvent(T entity)</em>" method is called upon interception. This is where interception logic takes place.
 * A <em>raw</em> instance of the entity is injected as parameter for this method and its state
 * may be modified according to some functional logic
 * <br>
 * <br>
 * <strong>Please note that during interception, the interceptor should NOT remove the primary key.
 * A verification is performed by Achilles after entity interception to guarantee that the primary key
 * and/or all its components are not null.
 * <br>
 * <br>
 * However it is possible to modify the primary key, although it is strongly discouraged to do so. It may
 * result in unexpected error and/or data corruption in Cassandra
 * </strong>
 * <br>
 * <br>
 * Below is an example of an interceptor on the <em>User</em> entity to set the <em>biography</em>
 * field with "TO DO" when it is not set. This interception occurs before persist and update operations
 *
 * <pre class="code"><code class="java">
 *
 *   public class UserInterceptor extends Interceptor&lt;User&gt;
 *   {
 *
 *       public void onEvent(User entity) {
 *          if(entity.getBiography() == null) {
 *              entity.setBiography("TO DO");
 *          }
 *       }
 *
 *       public List&lt;Event&gt; events() {
 *          return Arrays.asList(PRE_PERSIST,PRE_UPDATE);
 *          }
 *    }
 *
 * </code></pre>
 *
 * <p/>

 *
 * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Interceptors" target="_blank">Interceptors</a>
 *
 * @param <T> : type of entity to which this interceptor applies
 */
public interface Interceptor<T> {

	void onEvent(T entity);

	List<Event> events();

}
