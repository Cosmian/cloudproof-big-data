package com.cosmian.cloudproof_demo.policy;

import java.util.Map;
import java.util.Set;

import com.cosmian.CosmianException;
import com.cosmian.rest.cover_crypt.acccess_policy.Attr;

public enum Country {

    France("France"),
    Spain("Spain"),
    Germany("Germany"),
    Italy("Italy"),
    Other("Other");

    public final static String AXIS_NAME = "Country";

    private final String name;

    private Country(String name) {
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

    static Map<String, Country> ENUM_MAP = EnumUtils.to_map(Country.values());

    public static Country from(String name) throws IllegalArgumentException {
        Country o = ENUM_MAP.get(name);
        if (o == null) {
            throw new IllegalArgumentException("No country with name: " + name);
        }
        return o;
    }

    /**
     * The list of countries as strings
     * 
     * @return the String values
     */
    public static String[] stringValues() {
        Set<String> keys = ENUM_MAP.keySet();
        return keys.toArray(new String[keys.size()]);
    }
}
