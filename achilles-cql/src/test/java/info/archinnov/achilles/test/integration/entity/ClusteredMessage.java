/**
 *
 * Copyright (C) 2012-2013 DuyHai DOAN
 *
 * Licensimport javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
ce with the License.
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
package info.archinnov.achilles.test.integration.entity;

import info.archinnov.achilles.annotations.Column;
import info.archinnov.achilles.annotations.EmbeddedId;
import info.archinnov.achilles.annotations.Entity;

@Entity
public class ClusteredMessage {

	@EmbeddedId
	private ClusteredMessageId id;

	@Column
	private String label;

	public ClusteredMessage() {
	}

	public ClusteredMessage(ClusteredMessageId id, String label) {
		this.id = id;
		this.label = label;
	}

	public ClusteredMessageId getId() {
		return id;
	}

	public void setId(ClusteredMessageId id) {
		this.id = id;
	}

	public String getLabel() {
		return label;
	}

	public void setLabel(String label) {
		this.label = label;
	}
}
