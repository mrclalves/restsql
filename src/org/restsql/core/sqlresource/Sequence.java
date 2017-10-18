package org.restsql.core.sqlresource;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for Sequence complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * 	&lt;complexType name="Sequence">
 *		&lt;attribute name="name" type="string" use="required">
 *			&lt;annotation>
 *				&lt;documentation>Name of squence when used.</documentation>
 *			&lt;/annotation>
 *		&lt;/attribute>
 *	&lt;/complexType>
 * </pre>
 */



@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "Sequence")
public class Sequence {
	
    @XmlAttribute(name = "name", required = true)
	protected String name;

	public Sequence() {
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	@Override
	public String toString() {
		return "Sequence [name=" + name + "]";
	}

}
