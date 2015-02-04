package com.rolonews.hbasemapper.mapping;

/**
* Created by Sleiman on 03/02/2015.
*/
class HCellDescriptor implements CellDescriptor{

    private final String family;
    private final String qualifier;

    HCellDescriptor(String family, String qualifier){
        this.family = family;
        this.qualifier = qualifier;
    }
    @Override
    public String family() {
        return this.family;
    }

    @Override
    public String qualifier() {
        return this.qualifier;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HCellDescriptor that = (HCellDescriptor) o;

        if (family != null ? !family.equals(that.family) : that.family != null) return false;
        if (qualifier != null ? !qualifier.equals(that.qualifier) : that.qualifier != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = family != null ? family.hashCode() : 0;
        result = 31 * result + (qualifier != null ? qualifier.hashCode() : 0);
        return result;
    }
}
