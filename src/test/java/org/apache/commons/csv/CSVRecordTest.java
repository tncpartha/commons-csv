/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.commons.csv;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CSVRecordTest {

    private enum EnumFixture { UNKNOWN_COLUMN }

    private String[] values;
    private CSVRecord record, recordWithHeader;
    private Map<String, Integer> header;

    @Before
    public void setUp() throws Exception {
        System.out.println("Entering setUP");
        values = new String[] { "A", "B", "C" };
        record = new CSVRecord(values, null, null, 0, -1);
        header = new HashMap<String, Integer>();
        header.put("first", Integer.valueOf(0));
        header.put("second", Integer.valueOf(1));
        header.put("third", Integer.valueOf(2));
        recordWithHeader = new CSVRecord(values, header, null, 0, -1);
        System.out.println("Exiting setUP");
    }

    @Test
    public void testGetInt() {
        System.out.println("Entering testGetInt");
        assertEquals(values[0], record.get(0));
        assertEquals(values[1], record.get(1));
        assertEquals(values[2], record.get(2));
        System.out.println("Exiting testGetInt");
    }

    @Test
    public void testGetString() {
         System.out.println("Entering tetsGetString");
        assertEquals(values[0], recordWithHeader.get("first"));
        assertEquals(values[1], recordWithHeader.get("second"));
        assertEquals(values[2], recordWithHeader.get("third"));
         System.out.println("Exiting testGetString");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetStringInconsistentRecord() {
         System.out.println("Entering testGetStringIncosistentRecord");
        header.put("fourth", Integer.valueOf(4));
        recordWithHeader.get("fourth");
         System.out.println("Exiting testGetStringIncosistentRecord");
    }

    @Test(expected = IllegalStateException.class)
    public void testGetStringNoHeader() {
         System.out.println("Entering testGetStringNoHeader");
        record.get("first");
         System.out.println("Exiting testGetStringNoHeader");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetUnmappedEnum() {
         System.out.println("Entering testGetUnmappedEnum");
        assertNull(recordWithHeader.get(EnumFixture.UNKNOWN_COLUMN));
         System.out.println("Exiting testGetUnmappedEnum");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetUnmappedName() {
         System.out.println("Entering testGetUnmappedName");
        assertNull(recordWithHeader.get("fourth"));
         System.out.println("Exiting testGetUnmappedName");
    }

    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void testGetUnmappedNegativeInt() {
         System.out.println("Entering ArryIndexOutOfBoundsException");
        assertNull(recordWithHeader.get(Integer.MIN_VALUE));
         System.out.println("Exiting ArrayIndexOutOfBoundsException");
    }

    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void testGetUnmappedPositiveInt() {
         System.out.println("Entering recordWithHeader");
        assertNull(recordWithHeader.get(Integer.MAX_VALUE));
         System.out.println("Exiting recordWithHeader");
    }

    @Test
    public void testIsConsistent() {
         System.out.println("Entering testIsConsistent");
        assertTrue(record.isConsistent());
        assertTrue(recordWithHeader.isConsistent());

        header.put("fourth", Integer.valueOf(4));
        assertFalse(recordWithHeader.isConsistent());
         System.out.println("Exiting testIsConsistent");
    }

    @Test
    public void testIsMapped() {
         System.out.println("Entering testIsMapped");
        assertFalse(record.isMapped("first"));
        assertTrue(recordWithHeader.isMapped("first"));
        assertFalse(recordWithHeader.isMapped("fourth"));
         System.out.println("Exiting testIsMapped");
    }

    @Test
    public void testIsSet() {
         System.out.println("Entering testIsSet");
        assertFalse(record.isSet("first"));
        assertTrue(recordWithHeader.isSet("first"));
        assertFalse(recordWithHeader.isSet("fourth"));
         System.out.println("Exiting testIsSet");
    }

    @Test
    public void testIterator() {
         System.out.println("Entering testIterator");
        int i = 0;
        for (final String value : record) {
            assertEquals(values[i], value);
            i++;
            }
         System.out.println("Exiting testIterator");
    }

    @Test
    public void testPutInMap() {
         System.out.println("Entering testPutInMap");
        final Map<String, String> map = new ConcurrentHashMap<String, String>();
        this.recordWithHeader.putIn(map);
        this.validateMap(map, false);
        // Test that we can compile with assigment to the same map as the param.
        final TreeMap<String, String> map2 = recordWithHeader.putIn(new TreeMap<String, String>());
        this.validateMap(map2, false);
         System.out.println("Exiting testPutInMap");
    }

    @Test
    public void testRemoveAndAddColumns() throws IOException {
         System.out.println("Entering testRemoveAndAddColumns");
        // do:
        final CSVPrinter printer = new CSVPrinter(new StringBuilder(), CSVFormat.DEFAULT);
        final Map<String, String> map = recordWithHeader.toMap();
        map.remove("OldColumn");
        map.put("ZColumn", "NewValue");
        // check:
        final ArrayList<String> list = new ArrayList<String>(map.values());
        Collections.sort(list);
        printer.printRecord(list);
        Assert.assertEquals("A,B,C,NewValue" + CSVFormat.DEFAULT.getRecordSeparator(), printer.getOut().toString());
        printer.close();
         System.out.println("Exiting testRmoveAndAddColumns");
    }

    @Test
    public void testToMap() {
         System.out.println("Entering testToMap");
        final Map<String, String> map = this.recordWithHeader.toMap();
        this.validateMap(map, true);
         System.out.println("Exiting testToMap");
    }

    @Test
    public void testToMapWithShortRecord() throws Exception {
         System.out.println("Entering testToMapWithShortRecord");
       final CSVParser parser =  CSVParser.parse("a,b", CSVFormat.DEFAULT.withHeader("A", "B", "C"));
       final CSVRecord shortRec = parser.iterator().next();
       shortRec.toMap();
         System.out.println("Exiting testToMapWithShortRecord");
    }

    @Test
    public void testToMapWithNoHeader() throws Exception {
         System.out.println("Entering testToMapWithNoHeader");
       final CSVParser parser =  CSVParser.parse("a,b", CSVFormat.newFormat(','));
       final CSVRecord shortRec = parser.iterator().next();
       final Map<String, String> map = shortRec.toMap();
       assertNotNull("Map is not null.", map);
       assertTrue("Map is empty.", map.isEmpty());
         System.out.println("Exiting testToMapWithNoHeader");
    }

    private void validateMap(final Map<String, String> map, final boolean allowsNulls) {
         System.out.println("Entering validateMap");
        assertTrue(map.containsKey("first"));
        assertTrue(map.containsKey("second"));
        assertTrue(map.containsKey("third"));
        assertFalse(map.containsKey("fourth"));
        if (allowsNulls) {
            assertFalse(map.containsKey(null));
        }
        assertEquals("A", map.get("first"));
        assertEquals("B", map.get("second"));
        assertEquals("C", map.get("third"));
        assertEquals(null, map.get("fourth"));
         System.out.println("Exiting validateMap");
    }

}
