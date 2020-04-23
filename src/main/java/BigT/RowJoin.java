package BigT;

import driver.FilterParser;
import global.AttrType;
import global.GlobalConst;
import global.SystemDefs;
import global.TupleOrder;
import storage.BigT;

import java.io.IOException;

public class RowJoin implements GlobalConst {
    static AttrType[] attrType = {new AttrType(AttrType.attrString), new AttrType(AttrType.attrString), new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrString)};
    static short[] attrSize = {MAXROWLABELSIZE, MAXCOLUMNLABELSIZE, MAXVALUESIZE};


    public static BigT rowJoin(BigT b1, BigT b2, String outbigTName, String columnFilter, int num_pages) throws Exception {
        BigT outB = null;
        MultiTypeFileStream fs1 = null;
        LatestVersion lv1 = null;
        MultiTypeFileStream fs2 = null;
        LatestVersion lv2 = null;
        MultiTypeFileStream fs1_1 = null;
        Sort s1 = null;
        MultiTypeFileStream fs2_2 = null;
        MultiTypeFileStream fs1_1_1 = null;
        Sort s2 = null;

        try {
            outB = new BigT(outbigTName);

            fs1 = new MultiTypeFileStream(b1, FilterParser.parseSingle(columnFilter, 2, AttrType.attrString));
            lv1 = new LatestVersion(fs1, num_pages);

            Map tempMap1;
            while ((tempMap1 = lv1.get_next()) != null) {
                fs2 = new MultiTypeFileStream(b2, FilterParser.parseSingle(columnFilter, 2, AttrType.attrString));
                lv2 = new LatestVersion(fs2, num_pages);

                Map tempMap2;
                while ((tempMap2 = lv2.get_next()) != null) {
                    if (tempMap1.getValue().equalsIgnoreCase(tempMap2.getValue())) {
                        tempMap1.print();
                        tempMap2.print();
                        System.out.println("");
                        //begin adding all maps from each row
                        String currRow1 = tempMap1.getRowLabel();
                        fs1_1 = new MultiTypeFileStream(b1, FilterParser.parseSingle(currRow1, 1, AttrType.attrString));
                        s1 = new Sort(attrType, (short) 4, attrSize, fs1_1, new int[]{1, 3}, new TupleOrder(TupleOrder.Ascending), MAXROWLABELSIZE, (int) (num_pages * 0.2));

                        //Loop through table one
                        Map tempMap1_1 = s1.get_next();
                        while (true) {
                            String currColumn1 = tempMap1_1.getColumnLabel();
                            String new_row_label = tempMap1.getRowLabel() + ":" + tempMap2.getRowLabel();

                            fs2_2 = new MultiTypeFileStream(b2, FilterParser.parseCombine(String.join("##", tempMap2.getRowLabel(), currColumn1)));
                            Map tempMap2_2 = fs2_2.get_next();
                            if (tempMap2_2 == null) {
                                while (tempMap1_1.getColumnLabel().equals(currColumn1)) {
                                    //This case only heapfile 1 has the column so just insert each of the maps that it has with new rowlabel
                                    Map insertMap = new Map();
                                    insertMap.setRowLabel(new_row_label);
                                    insertMap.setColumnLabel(currColumn1);
                                    insertMap.setTimeStamp(tempMap1_1.getTimeStamp());
                                    insertMap.setValue(tempMap1_1.getValue());
                                    outB.insertMap(insertMap);

                                    tempMap1_1 = s1.get_next();
                                    if (tempMap1_1 == null) break;
                                }
                            } else {
                                //both rows have the column
                                //The column filter matches - only grab latest three
                                if (currColumn1.equals(columnFilter)) {
                                    Map[] maps = new Map[6];
                                    int counter = 0;

                                    maps[0] = new Map();
                                    maps[0].mapCopy(tempMap2_2);
                                    counter++;
                                    while ((tempMap2_2 = fs2_2.get_next()) != null) {
                                        maps[counter] = new Map();
                                        maps[counter].mapCopy(tempMap2_2);
                                        counter++;
                                    }
                                    while (tempMap1_1.getColumnLabel().equalsIgnoreCase(currColumn1)) {
                                        maps[counter] = new Map();
                                        maps[counter].mapCopy(tempMap1_1);

                                        tempMap1_1 = s1.get_next();
                                        if (tempMap1_1 == null) break;

                                        counter++;
                                        if (counter >= 6) break;
                                    }

                                    Map[] latest = get_three_latest(maps);
                                    for (int i = 0; i < latest.length; i++) {
                                        if (latest[i] == null) break;
                                        latest[i].setRowLabel(new_row_label);
                                        outB.insertMap(latest[i]);
                                    }
                                } else {
                                    //both have same column but it isn't the column filter - include all of them
                                    //but have to rename the columns
                                    while (tempMap1_1.getColumnLabel().equalsIgnoreCase(currColumn1)) {
                                        Map insertMap = new Map();
                                        insertMap.setRowLabel(new_row_label);
                                        insertMap.setColumnLabel(tempMap1.getRowLabel() + "_" + currColumn1);
                                        insertMap.setTimeStamp(tempMap1_1.getTimeStamp());
                                        insertMap.setValue(tempMap1_1.getValue());
                                        outB.insertMap(insertMap);

                                        tempMap1_1 = s1.get_next();
                                        if (tempMap1_1 == null) break;
                                    }
                                }
                            }

                            fs2_2.close();
                            if (tempMap1_1 == null) {
                                fs1_1.close();
                                s1.close();
                                break;
                            }
                        }

                        String currRow2 = tempMap2.getRowLabel();
                        fs2_2 = new MultiTypeFileStream(b2, FilterParser.parseSingle(currRow2, 1, AttrType.attrString));
                        s2 = new Sort(attrType, (short) 4, attrSize, fs2_2, new int[]{1, 3}, new TupleOrder(TupleOrder.Ascending), MAXROWLABELSIZE, (int) (num_pages * 0.2));

                        //Loop through table two
                        Map tempMap2_2 = s2.get_next();
                        while (true) {
                            String currColumn2 = tempMap2_2.getColumnLabel();
                            String new_row_label = tempMap1.getRowLabel() + ":" + tempMap2.getRowLabel();

                            fs1_1_1 = new MultiTypeFileStream(b1, FilterParser.parseCombine(String.join("##", tempMap1.getRowLabel(), currColumn2)));
                            Map tempMap1_1_1 = fs1_1_1.get_next();
                            if (tempMap1_1_1 == null) {
                                while (tempMap2_2.getColumnLabel().equals(currColumn2)) {
                                    //This case only heapfile 2 has the column so just insert each of the maps that it has with new rowlabel
                                    Map insertMap = new Map();
                                    insertMap.setRowLabel(new_row_label);
                                    insertMap.setColumnLabel(currColumn2);
                                    insertMap.setTimeStamp(tempMap2_2.getTimeStamp());
                                    insertMap.setValue(tempMap2_2.getValue());
                                    outB.insertMap(insertMap);

                                    tempMap2_2 = s2.get_next();
                                    if (tempMap2_2 == null) break;
                                }
                            } else {
                                //both rows have the column

                                //The column filter matches - only grab latest three
                                if (currColumn2.equals(columnFilter)) {
                                    //We have already inserted the latest three of the join column
                                    //This time just iterate until we are past that column
                                    while (tempMap2_2.getColumnLabel().equalsIgnoreCase(currColumn2)) {
                                        tempMap2_2 = s2.get_next();
                                        if (tempMap2_2 == null) break;
                                    }
                                } else {
                                    //both have same column but it isn't the column filter - include all of them
                                    //but have to rename the columns
                                    while (tempMap2_2.getColumnLabel().equalsIgnoreCase(currColumn2)) {
                                        Map insertMap = new Map();
                                        insertMap.setRowLabel(new_row_label);
                                        insertMap.setColumnLabel(tempMap2.getRowLabel() + "_" + currColumn2);
                                        insertMap.setTimeStamp(tempMap2_2.getTimeStamp());
                                        insertMap.setValue(tempMap2_2.getValue());
                                        outB.insertMap(insertMap);

                                        tempMap2_2 = s2.get_next();
                                        if (tempMap2_2 == null) break;
                                    }
                                }
                            }
                            fs1_1_1.close();


                            if (tempMap2_2 == null) {
                                fs2_2.close();
                                s2.close();
                                break;
                            }
                        }

                    }
                }


                fs2.close();
                lv2.close();
            }


            lv1.close();
            fs1.close();

            return outB;
        } catch (Exception e) {
            e.printStackTrace();
            if (fs1 != null) {fs1.close();}
            if (lv1 != null) {lv1.close();}
            if (fs2 != null) {fs2.close();}
            if (lv2 != null) {lv2.close();}
            if (fs1_1 != null) {fs1_1.close();}
            if (s1 != null) {s1.close();}
            if (fs2_2 != null) {fs2_2.close();}
            if (fs1_1_1 != null) {fs1_1_1.close();}
            if (s2 != null) {s2.close();}
            if (outB != null) {outB.delete_all_files(); outB.close();}

            return null;
        }
    }

    private static Map[] get_three_latest(Map[] maps) throws IOException {

        int numberToReturn = 3;
        if(maps.length <= 3) return maps;

        Map[] returnMaps = new Map[maps.length];
        for(int i = 0; i < numberToReturn; i++) {
            if(maps[i] == null) break;
            int highestTimeStamp = -1;
            int highestIndex = 0;
            for(int j = 0; j < maps.length; j++) {
                if(maps[j] != null && maps[j].getTimeStamp() > highestTimeStamp) {
                    highestTimeStamp = maps[j].getTimeStamp();
                    highestIndex = j;
                }
            }

            returnMaps[i] = new Map();
            returnMaps[i].mapCopy(maps[highestIndex]);
            maps[highestIndex] = null;
        }
        return returnMaps;
    }
}
