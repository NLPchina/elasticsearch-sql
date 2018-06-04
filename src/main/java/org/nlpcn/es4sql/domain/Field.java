package org.nlpcn.es4sql.domain;

import org.nlpcn.es4sql.parse.ChildrenType;
import org.nlpcn.es4sql.parse.NestedType;

/**
 * 搜索域
 * 
 * @author ansj
 *
 */
public class Field implements Cloneable{

	protected String name; //zhongshu-comment 在这里拼上script(....)
	private String alias;
    private NestedType nested;
    private ChildrenType children;

	public Field(String name, String alias) {
		this.name = name;
		this.alias = alias;
        this.nested = null;
        this.children = null;
	}

    public Field(String name, String alias, NestedType nested, ChildrenType children) {
        this.name = name;
        this.alias = alias;
        this.nested = nested;
        this.children = children;
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

    public boolean isNested() {
        return this.nested != null;
    }

    public boolean isReverseNested() {
        return this.nested != null && this.nested.isReverse();
    }

    public void setNested(NestedType nested){
        this.nested = nested;
    }

    public String getNestedPath() {
        if(this.nested == null ) return null;
        return this.nested.path;
    }

    public boolean isChildren() {
        return this.children != null;
    }

    public void setChildren(ChildrenType children){
        this.children = children;
    }

    public String getChildType() {
        if(this.children == null ) return null;
        return this.children.childType;
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
        return new Field(new String(this.name), new String(this.alias));
    }
}
