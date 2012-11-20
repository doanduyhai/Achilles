package parser.entity;

import java.io.Serializable;

import javax.persistence.Id;
import javax.persistence.Table;

@Table
public class BeanWithNotSerializableId implements Serializable
{
	class NotSerializableId
	{}

	public static final long serialVersionUID = 1L;

	@Id
	private NotSerializableId id;
}
