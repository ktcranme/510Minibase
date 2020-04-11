package storage;

import BigT.Map;
import global.AttrType;
import global.Convert;
import global.GlobalConst;

import java.io.IOException;

public class SmallMap implements GlobalConst {
    public static final int map_size = MAXROWLABELSIZE + 4 + MAXVALUESIZE;
    protected byte [] data;
    private int map_offset;
    private int map_length;
    private short fldCnt = 3;
    private short [] fldOffset;

    public static final short[] fldLengths = {
        MAXROWLABELSIZE,
        4,
        MAXVALUESIZE
    };

    public static final AttrType[] fldTypes = {
        new AttrType(AttrType.attrString),
        new AttrType(AttrType.attrInteger),
        new AttrType(AttrType.attrString)
    };

    public String getKey(Integer pos) throws IOException {
        switch (pos) {
            case 1:
                return getLabel();
            case 2:
                return String.format("%04d", getTimeStamp());
            case 3:
                return getValue();
        }

        throw new IOException("Invalid Key");
    }

    private short[] getFldOffsetArray()
    {
        return new short[] {
            (short) (map_offset),
            (short) (map_offset + MAXROWLABELSIZE),
            (short) (map_offset + MAXROWLABELSIZE + 4),
            (short) (map_offset + MAXROWLABELSIZE + 4 + MAXVALUESIZE),
        };
    }

    public SmallMap() {
        data = new byte[map_size];
        map_offset = 0;
        map_length = map_size;
        fldOffset = getFldOffsetArray();
    }

    public SmallMap(byte[] amap, int offset) {
        data = amap;
        map_offset = offset;
        map_length = map_size;
        fldOffset = getFldOffsetArray();
    }

    public SmallMap(BigT.Map fromMap, Integer ignorePos) {
        byte[] temparray = fromMap.getMapByteArray();
        data = new byte[map_size];
        System.arraycopy(temparray, MAXROWLABELSIZE + MAXCOLUMNLABELSIZE, data, MAXROWLABELSIZE, 4 + MAXVALUESIZE);
        if (ignorePos == 1) {
            // Copy column label
            System.arraycopy(temparray, MAXROWLABELSIZE, data, 0, MAXCOLUMNLABELSIZE);
        } else {
            // Copy row label
            System.arraycopy(temparray, 0, data, 0, MAXROWLABELSIZE);
        }
        fldOffset = getFldOffsetArray();
    }

    public String getLabel() throws IOException {
        String val;
        val = Convert.getStrValue(fldOffset[0], data, fldOffset[1] - fldOffset[0]); //strlen+2
        return val;
    }

    public int getTimeStamp() throws IOException {
        int val;
        val = Convert.getIntValue(fldOffset[1], data);
        return val;
    }

    public String getValue() throws IOException {
        String val;
        val = Convert.getStrValue(fldOffset[2], data, fldOffset[3] - fldOffset[2]);
        return val;
    }

    public SmallMap setLabel(String val) throws IOException {
        Convert.setStrValue (val, fldOffset[0], data);
        return this;
    }

    public SmallMap setTimeStamp(int val) throws IOException {
        Convert.setIntValue (val, fldOffset[1], data);
        return this;
    }

    public SmallMap setValue(String val) throws IOException  {
        //check if the val is an integer so we can pad with 0's
        if(!val.isEmpty() && val.chars().allMatch(c -> Character.isDigit(c))) {
            //pad with 0's
            int numZeros = MAXVALUESIZE - val.length() - 2;
            String zeros = "";
            for (int i = 0; i < numZeros; i++)
            {
                zeros += "0";
            }
            val = zeros + val;
        }

        Convert.setStrValue (val, fldOffset[2], data);
        return this;
    }

    public byte [] getMapByteArray() {
        byte [] mapcopy = new byte [map_size];
        System.arraycopy(data, map_offset, mapcopy, 0, map_size);
        return mapcopy;
    }

    public void print() throws IOException {
        String rowLabel;
        Integer timeStamp;
        Integer value;

        //get the row label
        rowLabel = Convert.getStrValue(fldOffset[0], data, fldOffset[1]-fldOffset[0]);
        //get the TimeStamp
        timeStamp = Convert.getIntValue(fldOffset[1], data);
        //get the value
        value = Integer.parseInt(Convert.getStrValue(fldOffset[2], data, fldOffset[3]-fldOffset[2]));
        //print them all
        System.out.println("(" + rowLabel + ", " + timeStamp + ") -> " + value);
    }

    public int getLength() {
        return map_length;
    }

    public short size() {
        return ((short) (fldOffset[fldCnt] - map_offset));
    }

    public Map toMap(String ignoredLabel, Integer ignoredPos) throws IOException {
        byte[] temparray = new byte[Map.map_size];
        System.arraycopy(data, MAXROWLABELSIZE, temparray, MAXROWLABELSIZE + MAXCOLUMNLABELSIZE, 4 + MAXVALUESIZE);
        if (ignoredPos == 1) {
            // Copy column label
            System.arraycopy(data, 0, temparray, MAXROWLABELSIZE, MAXCOLUMNLABELSIZE);
            Convert.setStrValue(ignoredLabel, 0, temparray);
        } else {
            // Copy row label
            System.arraycopy(data, 0, temparray, 0, MAXROWLABELSIZE);
            Convert.setStrValue(ignoredLabel, MAXROWLABELSIZE, temparray);
        }
        fldOffset = getFldOffsetArray();

        return new Map(temparray, 0);
    }
}
