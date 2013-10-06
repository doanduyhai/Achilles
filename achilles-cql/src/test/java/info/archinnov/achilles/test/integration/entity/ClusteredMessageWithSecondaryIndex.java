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

import info.archinnov.achilles.annotations.Index;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;

@Entity
public class ClusteredMessageWithSecondaryIndex {

	@EmbeddedId
	private ClusteredMessageId id;
	
	@Column
	private String description;

	@Column
	@Index
	private String label;

	@Column
	@Index
	private Integer number;

	public ClusteredMessageWithSecondaryIndex() {
	}

	public ClusteredMessageWithSecondaryIndex(ClusteredMessageId id, String description, String label, Integer number) {
		this.id = id;
		this.description = description;
		this.label = label;
		this.number = number;
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

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public Integer getNumber() {
		return number;
	}

	public void setNumber(Integer number) {
		this.number = number;
	}
}
