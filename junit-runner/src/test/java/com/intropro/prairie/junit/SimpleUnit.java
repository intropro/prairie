package com.intropro.prairie.junit;

import com.intropro.prairie.unit.common.Unit;
import com.intropro.prairie.unit.common.exception.DestroyUnitException;
import com.intropro.prairie.unit.common.exception.InitUnitException;

/**
 * Created by presidentio on 11/10/15.
 */
public class SimpleUnit implements Unit {

    private boolean inited;

    @Override
    public void start() throws InitUnitException {
        inited = true;
    }

    @Override
    public void stop() throws DestroyUnitException {
        inited = false;
    }

    public boolean isInited() {
        return inited;
    }
}
