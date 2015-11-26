package com.intropro.prairie.unit.cmd;

/**
 * Created by presidentio on 11/27/15.
 */
public class TestIn {

    public static void main(String[] args) throws InterruptedException {
        StreamRedirect streamRedirect = new StreamRedirect(System.in, System.out);
        streamRedirect.start();
        streamRedirect.join();
    }

}
