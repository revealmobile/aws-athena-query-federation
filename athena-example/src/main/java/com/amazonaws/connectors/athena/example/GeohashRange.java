/*-
 * #%L
 * athena-example
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.connectors.athena.example;

public class GeohashRange implements java.io.Serializable {
    private static final long serialVersionUID = 1L;
    private final String lower;
    private final String upper;
    public GeohashRange(final String lower, final String upper) {
        this.lower = lower;
        this.upper = upper;
    }
    public String getLower() {
        return lower;
    }
    public String getUpper() {
        return upper;
    }
    public boolean overlaps(final String otherLower, final String otherUpper) {
        return upper.compareTo(otherLower) >= 0 && lower.compareTo(otherUpper) < 0;
    }
    public static GeohashRange parse(final String rangeString) {
        if (rangeString == null) {
            return null;
        }
        final String[] parts = rangeString.split("-");
        return new GeohashRange(parts[0], parts[1]);
    }
    public static String rightPad(final String src, final String pad, final int length) {
        return src + new String(new char[length - src.length()]).replace("\0", pad);
    }
}
