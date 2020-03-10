package iterator;

import BigT.*;
import global.*;
import java.io.*;
import java.lang.*;

public class MapUtils {

	/*
	 * This function compares two maps in a specified field of the map note: the
	 * first field is map_fld_no=0
	 */
	public static int CompareMapWithMap(Map m1, Map m2, int map_fld_no)
			throws IOException, UnknowAttrType, TupleUtilsException {

		// Comparing integer timestamp
		if (map_fld_no == 2) {
			int val1 = m1.getTimeStamp();
			int val2 = m2.getTimeStamp();

			if (val1 < val2)
				return -1;
			if (val1 > val2)
				return 1;
			return 0;
		}
		// Comparing string field (either row, column, or value)
		else {
			String val1;
			String val2;

			switch (map_fld_no) {
				case 0:
					val1 = m1.getRowLabel();
					val2 = m2.getRowLabel();
					break;
				case 1:
					val1 = m1.getColumnLabel();
					val2 = m2.getColumnLabel();
					break;
				case 3:
					val1 = m1.getValue();
					val2 = m2.getValue();
					break;
				default:
					throw new TupleUtilsException(null, "Only field numbers 0-3 are accepted");
			}

			if (val1.compareTo(val2) > 0)
				return 1;
			if (val1.compareTo(val2) < 0)
				return -1;
			return 0;
		}
	}

	/*
	 * This function compares two maps in all fields it utilizes CompareMapWithMap
	 * in each of the 4 fields
	 */
	public static boolean Equal(Map m1, Map m2) throws IOException, UnknowAttrType, TupleUtilsException {
		for (int i = 0; i < 4; i++) {
			if (CompareMapWithMap(m1, m2, i) != 0) {
				return false;
			}
		}

		return true;
	}

	public static void SetValue(Map value, Map map, int fld_no) throws IOException, UnknowAttrType, TupleUtilsException {

		switch (fld_no) {
			case 0:
				value.setRowLabel(map.getRowLabel());
				break;
			case 1:
				value.setColumnLabel(map.getColumnLabel());
				break;
			case 2:
				value.setTimeStamp(map.getTimeStamp());
				break;
			case 3:
				value.setValue(map.getValue());
				break;
			default:
				throw new UnknowAttrType(null, "Don't know how to handle attrSymbol, attrNull");

		}

		return;

	}
}