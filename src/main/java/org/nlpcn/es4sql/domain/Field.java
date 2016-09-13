package org.nlpcn.es4sql.domain;

/**
 * 搜索域
 * 
 * @author ansj
 *
 */
public class Field implements  Cloneable{

	protected String name;
	private String alias;

	public Field(String name, String alias) {
		this.name = name;
		this.alias = alias;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getAlias() {
		return alias;
	}

	public void setAlias(String alias) {
		this.alias = alias;
	}

	@Override
	public String toString() {
		return this.name;
	}

    @Override
    public boolean equals(Object obj) {
        if(obj == null) return false;
        if(obj.getClass() != this.getClass()) return false;
        Field other = (Field) obj;
        boolean namesAreEqual = (other.getName() == null && this.name == null )
                || other.getName().equals(this.name) ;
        if(!namesAreEqual) return false;
        return (other.getAlias() == null && this.alias == null )
                || other.getAlias().equals(this.alias) ;
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        return new Field(new String(this.name),new String(this.alias));
    }
}
