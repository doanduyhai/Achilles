package parser.entity;

import java.io.Serializable;

import javax.persistence.Table;

@Table
public class BeanWithNonPublicSerialVersionUID implements Serializable
{
	static final long serialVersionUID = 1L;
}
