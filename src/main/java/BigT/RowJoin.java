package BigT;

import driver.FilterParser;
import global.AttrType;
import global.GlobalConst;
import global.TupleOrder;
import org.w3c.dom.Attr;

import java.io.IOException;

public class RowJoin implements GlobalConst {
    static AttrType[] attrType = {new AttrType(AttrType.attrString), new AttrType(AttrType.attrString), new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrString)};
    static short[] attrSize = {MAXROWLABELSIZE, MAXCOLUMNLABELSIZE, MAXVALUESIZE};


    public static bigT rowJoin(bigT b1, bigT b2, String outbigTName, String columnFilter, int num_pages) throws Exception {
        bigT outB = new bigT(outbigTName,1);


        FileStream fs1 = new FileStream(b1.getHf(), FilterParser.parseSingle(columnFilter,2, AttrType.attrString));
        LatestVersion lv1 = new LatestVersion(fs1,num_pages);

        Map tempMap1, tempWriteMap1;
        while ((tempMap1 = lv1.get_next())!=null){
            FileStream fs2 = new FileStream(b2.getHf(), FilterParser.parseSingle(columnFilter,2, AttrType.attrString));
            LatestVersion lv2 = new LatestVersion(fs2,num_pages);

            Map tempMap2;
            while ((tempMap2 = lv2.get_next()) != null) {
                if(tempMap1.getValue().equalsIgnoreCase(tempMap2.getValue())){
                    //begin adding all maps from each row
                    String currRow1 = tempMap1.getRowLabel();
                    FileStream fs1_1 = new FileStream(b1.getHf(), FilterParser.parseSingle(currRow1, 1, AttrType.attrString));
                    Sort s1 = new Sort(attrType, (short) 4, attrSize, fs1_1, new int[]{1,3}, new TupleOrder(TupleOrder.Ascending), MAXROWLABELSIZE, (int)(num_pages*0.4));

                    //Loop through table one
                    Map tempMap1_1 = s1.get_next();
                    while (true) {
                        String currColumn1 = tempMap1_1.getColumnLabel();
                        String new_row_label = tempMap1.getRowLabel() + ":" + tempMap2.getRowLabel();

                        FileStream fs2_2 = new FileStream(b2.getHf(), FilterParser.parseCombine(String.join("##", currRow1, currColumn1)));
                        Map tempMap2_2 = fs2_2.get_next();
                        if (tempMap2_2 == null) {
                            while(tempMap1_1.getColumnLabel().equals(currColumn1)) {
                                //This case only heapfile 1 has the column so just insert each of the maps that it has with new rowlabel
                                Map insertMap = new Map();
                                insertMap.setRowLabel(new_row_label);
                                insertMap.setColumnLabel(currColumn1);
                                insertMap.setTimeStamp(tempMap1_1.getTimeStamp());
                                insertMap.setValue(tempMap1_1.getValue());
                                outB.insertMap(insertMap.getMapByteArray());

                                tempMap1_1 = s1.get_next();
                                if(tempMap1_1 == null) break;
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
                                while((tempMap2_2 = fs2_2.get_next()) != null) {
                                    maps[counter] = new Map();
                                    maps[counter].mapCopy(tempMap2_2);
                                    counter++;
                                }
                                while(tempMap1_1.getColumnLabel().equalsIgnoreCase(currColumn1)) {
                                    maps[counter] = new Map();
                                    maps[counter].mapCopy(tempMap1_1);

                                    tempMap1_1 = s1.get_next();
                                    if(tempMap1_1 == null) break;

                                    counter++;
                                    if(counter >= 6) break;
                                }

                                Map[] latest = get_three_latest(maps);
                                for(int i = 0; i < latest.length; i++) {
                                    latest[i].setRowLabel(new_row_label);
                                    outB.insertMap(latest[i].getMapByteArray());
                                }
                            } else {
                                //both have same column but it isn't the column filter - include all of them
                                //but have to rename the columns
                                while(tempMap1_1.getColumnLabel().equalsIgnoreCase(currColumn1)) {
                                    Map insertMap = new Map();
                                    insertMap.setRowLabel(new_row_label);
                                    insertMap.setColumnLabel(tempMap1.getRowLabel() + "_" + currColumn1);
                                    insertMap.setTimeStamp(tempMap1_1.getTimeStamp());
                                    insertMap.setValue(tempMap1_1.getValue());
                                    outB.insertMap(insertMap.getMapByteArray());

                                    tempMap1_1 = s1.get_next();
                                    if(tempMap1_1 == null) break;
                                }
                            }
                        }

                        fs2_2.close();
                        if(tempMap1_1 == null) {
                            fs1_1.close();
                            s1.close();
                            break;
                        }
                    }






                    String currRow2 = tempMap1.getRowLabel();
                    FileStream fs2_2 = new FileStream(b2.getHf(), FilterParser.parseSingle(currRow2, 1, AttrType.attrString));
                    Sort s2 = new Sort(attrType, (short) 4, attrSize, fs2_2, new int[]{1,3}, new TupleOrder(TupleOrder.Ascending), MAXROWLABELSIZE, (int)(num_pages*0.4));

                    //Loop through table two
                    Map tempMap2_2 = s2.get_next();
                    while (true) {
                        String currColumn2 = tempMap2_2.getColumnLabel();
                        String new_row_label = tempMap1.getRowLabel() + ":" + tempMap2.getRowLabel();

                        FileStream fs1_1_1 = new FileStream(b2.getHf(), FilterParser.parseCombine(String.join("##", currRow2, currColumn2)));
                        Map tempMap1_1_1 = fs1_1_1.get_next();
                        if (tempMap1_1_1 == null) {
                            while(tempMap2_2.getColumnLabel().equals(currColumn2)) {
                                //This case only heapfile 2 has the column so just insert each of the maps that it has with new rowlabel
                                Map insertMap = new Map();
                                insertMap.setRowLabel(new_row_label);
                                insertMap.setColumnLabel(currColumn2);
                                insertMap.setTimeStamp(tempMap2_2.getTimeStamp());
                                insertMap.setValue(tempMap2_2.getValue());
                                outB.insertMap(insertMap.getMapByteArray());

                                tempMap2_2 = s2.get_next();
                                if(tempMap2_2 == null) break;
                            }
                        } else {
                            //both rows have the column

                            //The column filter matches - only grab latest three
                            if (currColumn2.equals(columnFilter)) {
                                //We have already inserted the latest three of the join column
                                //This time just iterate until we are past that column
                                while(tempMap2_2.getColumnLabel().equalsIgnoreCase(currColumn2)) {
                                    tempMap2_2 = s2.get_next();
                                    if(tempMap2_2 == null) break;
                                }
                            } else {
                                //both have same column but it isn't the column filter - include all of them
                                //but have to rename the columns
                                while(tempMap2_2.getColumnLabel().equalsIgnoreCase(currColumn2)) {
                                    Map insertMap = new Map();
                                    insertMap.setRowLabel(new_row_label);
                                    insertMap.setColumnLabel(tempMap2.getRowLabel() + "_" + currColumn2);
                                    insertMap.setTimeStamp(tempMap2_2.getTimeStamp());
                                    insertMap.setValue(tempMap2_2.getValue());
                                    outB.insertMap(insertMap.getMapByteArray());

                                    tempMap2_2 = s2.get_next();
                                    if(tempMap2_2 == null) break;
                                }
                            }
                        }
                        fs1_1_1.close();


                        if(tempMap2_2 == null) {
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
    }

    private static Map[] get_three_latest(Map[] maps) throws IOException {
        for(int k = 0; k < maps.length; k++) {
            maps[k].print();
        }


        int numberToReturn = 3;
        if(maps.length < 3) numberToReturn = maps.length;

        Map[] returnMaps = new Map[maps.length];
        for(int i = 0; i < numberToReturn; i++) {
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


        for(int k = 0; k < returnMaps.length; k++) {
            returnMaps[k].print();
        }

        return returnMaps;
    }
}
