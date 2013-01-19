package parser.entity;

import java.io.Serializable;

import javax.persistence.Entity;
import javax.persistence.Id;

/**
 * BeanWithNotSerializableId
 * 
 * @author DuyHai DOAN
 * 
 */
@Entity
public class BeanWithNotSerializableId implements Serializable
{
	class NotSerializableId
	{}

	public static final long serialVersionUID = 1L;

	@Id
	private NotSerializableId id;
}
