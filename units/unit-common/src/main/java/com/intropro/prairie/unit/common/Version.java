package com.intropro.prairie.unit.common;

/**
 * Created by presidentio on 12/8/15.
 */
public class Version implements Comparable {

    private String version;

    public Version(String version) {
        this.version = version;
    }

    public String getVersion() {
        return version;
    }

    @Override
    public int compareTo(Object o) {
        if (!(o instanceof Version)) {
            throw new IllegalArgumentException("Expected: " + getClass().getName());
        }
        String version2 = ((Version) o).getVersion();
        String[] vals1 = version.split("\\.");
        String[] vals2 = version2.split("\\.");
        int i = 0;
        // set index to first non-equal ordinal or length of shortest version string
        while (i < vals1.length && i < vals2.length && vals1[i].equals(vals2[i])) {
            i++;
        }
        // compare first non-equal ordinal number
        if (i < vals1.length && i < vals2.length) {
            int diff = Integer.valueOf(vals1[i]).compareTo(Integer.valueOf(vals2[i]));
            return Integer.signum(diff);
        }
        // the strings are equal or one string is a substring of the other
        // e.g. "1.2.3" = "1.2.3" or "1.2.3" < "1.2.3.4"
        else {
            return Integer.signum(vals1.length - vals2.length);
        }
    }
}
