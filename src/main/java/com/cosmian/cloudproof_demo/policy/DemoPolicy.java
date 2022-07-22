package com.cosmian.cloudproof_demo.policy;

import com.cosmian.CosmianException;
import com.cosmian.rest.cover_crypt.policy.Policy;

public final class DemoPolicy extends Policy {

    public final static String DEPARTMENT_AXIS = Department.AXIS_NAME;

    public final static String COUNTRY_AXIS = Country.AXIS_NAME;

    private static volatile DemoPolicy INSTANCE = null;

    private static final Object lock = new Object();

    protected DemoPolicy() throws CosmianException {
        super(Integer.MAX_VALUE);
        this.addAxis(DEPARTMENT_AXIS, Department.stringValues(), false).addAxis(COUNTRY_AXIS, Country.stringValues(),
            false);
    }

    public static DemoPolicy getInstance() throws CosmianException {
        if (INSTANCE == null) {
            synchronized (lock) {
                if (INSTANCE == null) {
                    INSTANCE = new DemoPolicy();
                }
            }
        }
        return INSTANCE;
    }

}
