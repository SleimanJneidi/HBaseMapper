package com.rolonews.hbasemapper.mapping;

import com.google.common.base.Objects;

/**
* Created by Sleiman on 03/02/2015.
*/
public class HCellDescriptor{

    private final String family;
    private final String qualifier;
    private final boolean isCollection;

    HCellDescriptor(String family, String qualifier, boolean isCollection){
        this.family = family;
        this.qualifier = qualifier;
        this.isCollection = isCollection;
    }
    
    public String family() {
        return this.family;
    }

    public String qualifier() {
        return this.qualifier;
    }
    
    public boolean isCollection() {
    	return isCollection;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
        	return true;
        }
        if(o instanceof HCellDescriptor){
        	HCellDescriptor that = (HCellDescriptor) o;
            return Objects.equal(family, that.family) && Objects.equal(qualifier, that.qualifier) && 
            		isCollection == that.isCollection;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(family, qualifier, isCollection);
    }
}
