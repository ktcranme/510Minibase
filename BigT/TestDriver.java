package BigT;

import java.io.IOException;

public class TestDriver {
	
	public static void runTests() {
		runTest1();
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
	
	
	public static void main(String [] args) {
		runTests();
	}
}
