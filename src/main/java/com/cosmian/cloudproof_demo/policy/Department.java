package com.cosmian.cloudproof_demo.policy;

import java.util.Map;
import java.util.Set;

import com.cosmian.CosmianException;
import com.cosmian.rest.cover_crypt.acccess_policy.Attr;

public enum Department {

    Marketing("Marketing"),
    HR("HR"),
    Security("Security"),
    Other("Other");

    public final static String AXIS_NAME = "Department";

    private final String name;

    private Department(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    public Attr getAttribute() throws CosmianException {
        return new Attr(AXIS_NAME, name);
    }

    @Override
    public String toString() {
        return EnumUtils.to_string(this);
    }

    static Map<String, Department> ENUM_MAP = EnumUtils.to_map(Department.values());

    public static Department from(String name) throws IllegalArgumentException {
        Department o = ENUM_MAP.get(name);
        if (o == null) {
            throw new IllegalArgumentException("No department with name: " + name);
        }
        return o;
    }

    /**
     * The list of departments as strings
     * 
     * @return the String values
     */
    public static String[] stringValues() {
        Set<String> keys = ENUM_MAP.keySet();
        return keys.toArray(new String[keys.size()]);
    }

}
