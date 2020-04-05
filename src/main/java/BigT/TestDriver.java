package BigT;

import java.io.IOException;
import iterator.MapUtils;
import iterator.TupleUtilsException;
import iterator.UnknowAttrType;

public class TestDriver {
	
	public static void runTests() {
		runTest1();
		runTest2();
	}
	
	/*
	 * Test 1 focuses on the Map class and ensuring all methods are functioning as expected
	 */
	public static void runTest1() {
		System.out.println("---------------------------------");
		System.out.println("Starting test 1 - Map.java");
		System.out.println("---------------------------------");
		
		System.out.println("Creating a map, setting all fields and printing");
		//create map and print it out
		Map m = new Map();
		try {
			m.setRowLabel("Dominica");
			m.setColumnLabel("Zebra");
			m.setTimeStamp(46067);
			m.setValue("1");
			m.print();
		} catch (IOException e) {
			System.out.println("IOException while creating a map");
			e.printStackTrace();
		}
		
		System.out.println("");
		
		//create a map from another map using Map(byte[] amap, int offset)
		//I believe offset should be 0
		System.out.println("Creating a map from the map above using\n"
				+ "bytearray and modifying the column label");
		Map m2 = new Map(m.getMapByteArray(), 0);
		try {
			m2.setColumnLabel("Giraffe");
			m2.print();
		} catch (IOException e) {
			System.out.println("IOException while creating a map from a bytearray");
			e.printStackTrace();
		}

		System.out.println("");
		
		//create a map from another map using Map(Map fromMap)
		//I believe offset should be 0
		System.out.println("Creating a map from the first map above using\n"
				+ "the map object and modifying the TimeStamp");
		Map m3 = new Map(m);
		try {
			m3.setTimeStamp(51073);
			m3.print();
		} catch (IOException e) {
			System.out.println("IOException while creating a map with another map");
			e.printStackTrace();
		}

		System.out.println("");
		
		//Test all the get methods
		System.out.println("Print out a map field by field using the get methods");
		try {
			System.out.println(m.getRowLabel());
			System.out.println(m.getColumnLabel());
			System.out.println(m.getTimeStamp());
			System.out.println(m.getValue());
		} catch (IOException e) {
			System.out.println("IOException while testing the get methods");
			e.printStackTrace();
		}

		System.out.println("");
		
		System.out.println("Get the sizes of the maps (Should always be the same)");
		System.out.println(m.size() + " " +  m2.size() + " " +  m3.size());

		System.out.println("");
		
		//Test all the 'used when you don't want to use the constructor' methods
		System.out.println("Testing mapCopy, mapInit, and mapSet using the\n"
				+ "three maps from above");
		Map m4 = new Map();
		Map m5 = new Map();
		Map m6 = new Map(m2);
		
		m4.mapCopy(m);
		m5.mapInit(m2.getMapByteArray(), 0);
		m6.mapSet(m3.getMapByteArray(), 0);
		
		try {
			m4.print();
			m5.print();
			m6.print();
		} catch (IOException e) {
			System.out.println("IOException while testing the constructor replacement methods");
			e.printStackTrace();
		}
		
		System.out.println("\n------------------------------------------");
		System.out.println("Finished Test 1");
		System.out.println("------------------------------------------");
	}
	
	/*
	 * Test 2 focuses on the MapUtils class ensuring that comparing two maps is functional
	 */
	public static void runTest2() {
		System.out.println("---------------------------------");
		System.out.println("Starting test 2 - MapUtils.java");
		System.out.println("---------------------------------");
		
		Map m1 = new Map();
		Map m2 = new Map();
		
		try {
			m1.setRowLabel("Kevin");
			m1.setColumnLabel("Cranmer");
			m1.setTimeStamp(12345);
			m1.setValue("CSE510");
			m2.mapCopy(m1);
		} catch (IOException e) {
			System.out.println("Error setting values of maps");
			e.printStackTrace();
		}
		
		System.out.println("Comparing two equal Maps");
		try {
			boolean isEqual = MapUtils.Equal(m1, m2);
			int rowEqual = MapUtils.CompareMapWithMap(m1, m2, 0);
			int columnEqual = MapUtils.CompareMapWithMap(m1, m2, 1);
			int timestampEqual = MapUtils.CompareMapWithMap(m1, m2, 2);
			int valueEqual = MapUtils.CompareMapWithMap(m1, m2, 3);
			
			if(!isEqual) System.out.println("MapUtils.Equal() incorrectly return False");
			if(rowEqual != 0) System.out.println("MapUtils.CompareMapWithMap() incorrectly did not return 0 for Row");
			if(columnEqual != 0) System.out.println("MapUtils.CompareMapWithMap() incorrectly did not return 0 for Column");
			if(timestampEqual != 0) System.out.println("MapUtils.CompareMapWithMap() incorrectly did not return 0 for TimeStamp");
			if(valueEqual != 0) System.out.println("MapUtils.CompareMapWithMap() incorrectly did not return 0 for Value");
			
		} catch (UnknowAttrType e) {
			e.printStackTrace();
		} catch (TupleUtilsException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		System.out.println("Comparing two unequal Maps");
		try {
			m2.setColumnLabel("Thomas");
			m2.setTimeStamp(12301);
			
			boolean isEqual = MapUtils.Equal(m1, m2);
			int rowEqual = MapUtils.CompareMapWithMap(m1, m2, 0);
			int columnEqual = MapUtils.CompareMapWithMap(m1, m2, 1);
			int timestampEqual = MapUtils.CompareMapWithMap(m1, m2, 2);
			int valueEqual = MapUtils.CompareMapWithMap(m1, m2, 3);
			
			if(isEqual) System.out.println("MapUtils.Equal() incorrectly return True");
			if(rowEqual != 0) System.out.println("MapUtils.CompareMapWithMap() incorrectly did not return 0 for Row");
			if(columnEqual != -1) System.out.println("MapUtils.CompareMapWithMap() incorrectly did not return 1 for Column");
			if(timestampEqual != 1) System.out.println("MapUtils.CompareMapWithMap() incorrectly did not return -1 for TimeStamp");
			if(valueEqual != 0) System.out.println("MapUtils.CompareMapWithMap() incorrectly did not return 0 for Value");
			
		} catch (UnknowAttrType e) {
			e.printStackTrace();
		} catch (TupleUtilsException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		System.out.println("If no incorrect statements shown then test passes.");
		System.out.println("------------------------------------------");
		System.out.println("Finished Test 2");
		System.out.println("------------------------------------------");
		
	}
	
	public static void main(String [] args) {
		runTests();
	}
}
