package BigT;

import java.io.*;
import java.lang.*;
import global.*;

//The GlobalConst gives us a bunch of int sizes like MINIBASE_PAGESIZE
//Not sure why its an interface and not just a static class tho


//This class is very similar to heap.Tuple "but having a fixed structure (and thus a fixed header)"
//core concept for us to understand this class is what having the fixed structure means for
//I BELIEVE it means we create the header as in heap.Tuple.setHdr we know that it is String,String,Int,String for Row,Column,Timestamp,Value
//as for other things, I'm not sure yet

public class PhysicalMap implements GlobalConst{
	//public static final int max_size = MINIBASE_PAGESIZE; //Maximum size of any map		NOTE: I assume this will be the same
	public static final int map_size = MAXROWLABELSIZE + MAXCOLUMNLABELSIZE + 4 + MAXVALUESIZE + 4 + MAXVALUESIZE + 4 + MAXVALUESIZE;		//NOTE: this could change if the map_offset is actually not always 0
	private byte [] data; //a byte array to hold data
	private int map_offset; //start position of this tuple in data[]		NOTE: I don't understand what this is. Wouldn't the tuple be at the start of the byte array everytime???
	private int map_length; //length of this tuple
	private short fldCnt = 8; //Number of fields in this tuple		NOTE: I believe this will always be 4
	private short [] fldOffset; //Array of offsets of the fields

    public static final short[] PhysicalMapOffsets = {
        0,
        (short) (MAXROWLABELSIZE), 
        (short) (MAXROWLABELSIZE + MAXCOLUMNLABELSIZE),
        (short) (MAXROWLABELSIZE + MAXCOLUMNLABELSIZE + 4),
        (short) (MAXROWLABELSIZE + MAXCOLUMNLABELSIZE + 4 + MAXVALUESIZE),
        (short) (MAXROWLABELSIZE + MAXCOLUMNLABELSIZE + 4 + MAXVALUESIZE + 4),
        (short) (MAXROWLABELSIZE + MAXCOLUMNLABELSIZE + 4 + MAXVALUESIZE + 4 + MAXVALUESIZE),
        (short) (MAXROWLABELSIZE + MAXCOLUMNLABELSIZE + 4 + MAXVALUESIZE + 4 + MAXVALUESIZE + 4),
        (short) (MAXROWLABELSIZE + MAXCOLUMNLABELSIZE + 4 + MAXVALUESIZE + 4 + MAXVALUESIZE + 4 + MAXVALUESIZE),
    };
	
	private short[] getFldOffsetArray()
	{
		return new short[] {
            (short) map_offset,
            (short) (map_offset + MAXROWLABELSIZE), 
            (short) (map_offset + MAXROWLABELSIZE + MAXCOLUMNLABELSIZE),
            (short) (map_offset + MAXROWLABELSIZE + MAXCOLUMNLABELSIZE + 4),
            (short) (map_offset + MAXROWLABELSIZE + MAXCOLUMNLABELSIZE + 4 + MAXVALUESIZE),
            (short) (map_offset + MAXROWLABELSIZE + MAXCOLUMNLABELSIZE + 4 + MAXVALUESIZE + 4),
            (short) (map_offset + MAXROWLABELSIZE + MAXCOLUMNLABELSIZE + 4 + MAXVALUESIZE + 4 + MAXVALUESIZE),
            (short) (map_offset + MAXROWLABELSIZE + MAXCOLUMNLABELSIZE + 4 + MAXVALUESIZE + 4 + MAXVALUESIZE + 4),
            (short) (map_offset + MAXROWLABELSIZE + MAXCOLUMNLABELSIZE + 4 + MAXVALUESIZE + 4 + MAXVALUESIZE + 4 + MAXVALUESIZE),
        };
	}
	
	public String getFirstVer() throws IOException {
	  String val;
	  val = Convert.getStrValue(fldOffset[7], data, fldOffset[8] - fldOffset[7]);
	  return val;
	}
	
	public String getSecondVer() throws IOException {
	  String val;
	  val = Convert.getStrValue(fldOffset[5], data, fldOffset[6] - fldOffset[5]);
	  return val;
	}

	public String getThirdVer() throws IOException {
	  String val;
	  val = Convert.getStrValue(fldOffset[3], data, fldOffset[4] - fldOffset[3]);
	  return val;
	}

	public int getVersionCount() {
		for (int i = fldOffset[8] - 1; i >= fldOffset[4]; i--) {
			if (data[i] == 0) continue;
			else if (i > fldOffset[6]) return 3;
			else if (i > fldOffset[4]) return 2;
		}
		return 1;
	}

	public String getVersion(int i) throws IOException {
		int pos = 2 * i + 3;
		String val = Convert.getStrValue(fldOffset[pos], data, fldOffset[pos + 1] - fldOffset[pos]);
		return val;
	}
	
	public int updateMap(int timestamp, String value) throws IOException  {
		// System.out.println("New ts: " + timestamp + ", New value:" + value);
		int firstTs = Convert.getIntValue(fldOffset[2], data);
		int secondTs = Convert.getIntValue(fldOffset[4], data);
		int thirdTs = Convert.getIntValue(fldOffset[6], data);

		// String fv = Convert.getStrValue(fldOffset[3], data, fldOffset[4] - fldOffset[3]);
		String sv = Convert.getStrValue(fldOffset[5], data, fldOffset[6] - fldOffset[5]);
		String tv = Convert.getStrValue(fldOffset[7], data, fldOffset[8] - fldOffset[7]);

		// System.out.println("CURRENTLY: " + firstTs + ": " + fv + ", " + secondTs + ": " + sv + ", " + thirdTs + ": " + tv);
		if (sv.isEmpty()) {
			Convert.setIntValue(timestamp, fldOffset[4], data);
			Convert.setStrValue(value, fldOffset[5], data);
			// secondTs = Convert.getIntValue(fldOffset[4], data);
			// sv = Convert.getStrValue(fldOffset[5], data, fldOffset[6] - fldOffset[5]);
			// System.out.println("SECOND POS WAS EMPTY: " + firstTs + ": " + fv + ", " + secondTs + ": " + sv + ", " + thirdTs + ": " + tv);
			return 1;
		} else if (tv.isEmpty()) {
			Convert.setIntValue(timestamp, fldOffset[6], data);
			Convert.setStrValue(value, fldOffset[7], data);
			// // thirdTs = Convert.getIntValue(fldOffset[6], data);
			// // tv = Convert.getStrValue(fldOffset[7], data, fldOffset[8] - fldOffset[7]);
			// System.out.println("THIRD POS WAS EMPTY: " + firstTs + ": " + fv + ", " + secondTs + ": " + sv + ", " + thirdTs + ": " + tv);
			return 2;
		} else {
			// We have to replace the oldest timestamp
			int pos = -1;
			if (firstTs <= secondTs && firstTs <= thirdTs && timestamp >= firstTs) {
				pos = 2;
			} else if (secondTs <= firstTs && secondTs <= thirdTs  && timestamp >= secondTs) {
				pos = 4;
			} else if (thirdTs <= firstTs && thirdTs <= secondTs && timestamp >= thirdTs) {
				pos = 6;
			} else {
				// This is not inserted at all
				// System.out.println("NO CHANGE: " + firstTs + ": " + fv + ", " + secondTs + ": " + sv + ", " + thirdTs + ": " + tv);
				return pos;
			}

			Convert.setIntValue(timestamp, fldOffset[pos], data);
			Convert.setStrValue(value, fldOffset[pos + 1], data);

			// firstTs = Convert.getIntValue(fldOffset[2], data);
			// secondTs = Convert.getIntValue(fldOffset[4], data);
			// thirdTs = Convert.getIntValue(fldOffset[6], data);
	
			// fv = Convert.getStrValue(fldOffset[3], data, fldOffset[4] - fldOffset[3]);
			// sv = Convert.getStrValue(fldOffset[5], data, fldOffset[6] - fldOffset[5]);
			// tv = Convert.getStrValue(fldOffset[7], data, fldOffset[8] - fldOffset[7]);

			// System.out.println("POS " + ((pos / 2) - 1) + " WAS REPLACED: " + firstTs + ": " + fv + ", " + secondTs + ": " + sv + ", " + thirdTs + ": " + tv);

			return (pos / 2) - 1;
		}
	}
	
	/*
	* Class constructor
	* Create a new PhysicalMap with length = map_size, map_offset = 0.
	*/
	public  PhysicalMap()
	{
		// Create a new map
		data = new byte[map_size];
		map_offset = 0;
		map_length = map_size;
		fldOffset = getFldOffsetArray();
	}
	
	/*
	* Class constructor
	* Create a new PhysicalMap with length = map_size, map_offset = 0.
	*/
	public PhysicalMap(byte[] amap, int offset)
	{
		data = amap;
		map_offset = offset;
		map_length = map_size; 
		fldOffset = getFldOffsetArray();
	}
	
	/* Construct a map from another map through copy.
	 * Copy a tuple to the current map position
	* you must make sure the map lengths must be equal
	* @param fromMap the map being copied
	*/
	public PhysicalMap(PhysicalMap fromMap)
	{
		byte [] temparray = fromMap.getMapByteArray();
		data = new byte[map_size];	//NOTE: was getting a NULLPointer exception without this line. but Tuple.java doesn't use it...
		System.arraycopy(temparray, 0, data, map_offset, map_size);   
		fldOffset = getFldOffsetArray();
	}
	
	/*
	* Convert the row label into String
	* The row label is the first item in the map and thus the fldOffset index is 0
	*
	* @return           the converted string if success
	*			
	* @exception   IOException I/O errors
	*/
	public String getRowLabel() throws IOException 
	{ 
		String val;
		val = Convert.getStrValue(fldOffset[0], data, fldOffset[1] - fldOffset[0]); //strlen+2
		return val;
	}
	
	/*
	* Convert the column label into String
	* The row label is the second item in the map and thus the fldOffset index is 1
	*
	* @return           the converted string if success
	*			
	* @exception   IOException I/O errors
	*/
	public String getColumnLabel() throws IOException 
	{ 
		String val;
		val = Convert.getStrValue(fldOffset[1], data, fldOffset[2] - fldOffset[1]); //strlen+2
		return val;
	}
	
	/*
	* Convert the timestamp field into an int
	* The timestamp is the third item in the map and thus the fldOffset index is 2
	*
	* @return           the converted int if success
	*			
	* @exception   IOException I/O errors
	*/
	public int getTimeStamp() throws IOException 
	{ 
		int val;
		val = Convert.getIntValue(fldOffset[2], data);
		return val;
	}
	
	/*
	* Convert the value into String
	* The value is the last (fourth) item in the map and thus the fldOffset index is 3
	*
	* @return           the converted string if success
	*			
	* @exception   IOException I/O errors
	*/
	public String getValue() throws IOException 
	{ 
		String val;
		//NOTE: This is how it evaluates on the matching function from Tuple.java
		//		Our maps only have 4 fields but I believe the fldOffset will need
		//		to have 5 entries, the last one indicating the end of the value.
		//		This is why we need to use the index 4
		val = Convert.getStrValue(fldOffset[3], data, fldOffset[4] - fldOffset[3]); 
		return val;
	}
	
	/*
	* Set the row Label to a given String value
	*
	* @param     val     the string value to set row Label to
	* @exception   IOException I/O errors
	*/
	public PhysicalMap setRowLabel(String val) throws IOException
	{
		Convert.setStrValue (val, fldOffset[0], data);
		return this;
	}
	
	/*
	* Set the column Label to a given String value
	*
	* @param     val     the string value to set column Label to
	* @exception   IOException I/O errors
	*/
	public PhysicalMap setColumnLabel(String val) throws IOException
	{
		Convert.setStrValue (val, fldOffset[1], data);
		return this;
	}
	
	/*
	* Set the timestamp to a given int value
	*
	* @param     val     the int value to set the timestamp to
	* @exception   IOException I/O errors
	*/
	public PhysicalMap setTimeStamp(int val) throws IOException
	{
		Convert.setIntValue (val, fldOffset[2], data);
		return this;
	}
	
	/*
	* Set the value to a given String value
	*
	* @param     val     the string value to set value to
	* @exception   IOException I/O errors
	*/
	public PhysicalMap setValue(String val) throws IOException
	{
		
		Convert.setStrValue (val, fldOffset[3], data);
		return this;
	}
	
	/*  Copy the map byte array out
	*  @return  byte[], a byte array contains the map
	*		the length of byte[] = length of the map		NOTE: Is this line here explaining my question on line 47
	*/
	public byte [] getMapByteArray() 
	{
		byte [] mapcopy = new byte [map_size];
		System.arraycopy(data, map_offset, mapcopy, 0, map_size);
		return mapcopy;
    }

	public static byte [] getMapByteArray(Map m) 
	{
		byte [] mapcopy = new byte [map_size];
        System.arraycopy(m.data, 0, mapcopy, 0, MAXROWLABELSIZE + MAXCOLUMNLABELSIZE);
        System.arraycopy(m.data, PhysicalMapOffsets[2], mapcopy, PhysicalMapOffsets[m.version * 2 + 2], 4 + MAXVALUESIZE);
		return mapcopy;
    }

	// TODO: In future, might need to modify MAP in place.
    public static Map physicalMapToMap(byte[] record, int version) throws IOException {
        // PhysicalMap map = new PhysicalMap(record, 0);

        byte[] mapcopy = new byte [Map.map_size];
        System.arraycopy(record, 0, mapcopy, 0, MAXROWLABELSIZE + MAXCOLUMNLABELSIZE);
        System.arraycopy(record, PhysicalMapOffsets[version * 2 + 2], mapcopy, PhysicalMapOffsets[2], 4 + MAXVALUESIZE);

        String val = "";
        if (version < 2) {
            int nextVer = (version + 1) * 2 + 3;
            val = Convert.getStrValue(PhysicalMapOffsets[nextVer], record, PhysicalMapOffsets[nextVer + 1] - PhysicalMapOffsets[nextVer]);
		}
		
		// System.out.println("ACTUAL RECORD: " + map.getRowLabel() + ", " + map.getColumnLabel() + ", " + map.getFirstVer() + ", " + map.getSecondVer() + ", " + map.getThirdVer());

        return new Map(mapcopy, 0, version, !val.isEmpty());
    }
	
	/* Print out the map into console in the following format:
	 * (rowLabel, columnLabel, timeStamp) -> value
	*/
	public void print() throws IOException
	{
		String rowLabel;
		String columnLabel;
		int timeStamp;
		String value;
		
		
		//get the row label
		rowLabel = Convert.getStrValue(fldOffset[0], data, fldOffset[1]-fldOffset[0]);

		//get the column label
		columnLabel = Convert.getStrValue(fldOffset[1], data, fldOffset[2]-fldOffset[1]);

		//get the TimeStamp
		timeStamp = Convert.getIntValue(fldOffset[2], data);
		
		//get the value
		value = Convert.getStrValue(fldOffset[3], data, fldOffset[4]-fldOffset[3]);
		
		
		//print them all
		System.out.println("(" + rowLabel + ", " + columnLabel + ", " + timeStamp + ") -> " + value);
	}

	
	//NOTE the next two methods both return a size...
	//the method setHdr() is from Tuple.java and I just copied everything over
	//I believe we need to implement setHdr() but because our 'tuples' are fixed
	//structure, we should be able to make it cleaner
	
	/* get the length of a map, call this method if you did not 
	*  call setHdr () before
	* @return 	length of this map in bytes
	*/   
	public int getLength()
	{
		return map_length;
	}

	/* get the length of a map, call this method if you did 
	*  call setHdr () before
	* @return     size of this map in bytes
	*/
	public short size()
	{
		return ((short) (fldOffset[fldCnt] - map_offset));		//SHOULD WORK BUT map_size SHOULD ALSO WORK
	}
	
	
	
	
	
	//NOTE the final 3 methods here are like constructor replacements?
	//I don't understand their purpose but I've tried to implement them
	
	/* Copy a map to the current map position
	*  you must make sure the map lengths must be equal
	* @param fromMap the tuple being copied
	*/
	public void mapCopy(PhysicalMap fromMap)
	{
		byte [] temparray = fromMap.getMapByteArray();
		System.arraycopy(temparray, 0, data, map_offset, map_size);
		//may need to set the fldOffset in here
		fldOffset = getFldOffsetArray();
	}
	
	/* This is used when you don't want to use the constructor
	* @param amap  a byte array which contains the map
	* @param offset the offset of the map in the byte array
	*/
	public void mapInit(byte [] amap, int offset)
	{
		data = amap;
		map_offset = offset;
		map_length = map_size;
		fldOffset = getFldOffsetArray();
	}
	
	/*
	* Set a map with the given map offset
	* @param	record	a byte array contains the map
	* @param	offset  the offset of the map ( =0 by default)
	*/
	public void mapSet(byte [] frommap, int offset)  
	{
		System.arraycopy(frommap, offset, data, map_offset, map_size);
		map_length = map_size;	
		fldOffset = getFldOffsetArray();
	}
	
}