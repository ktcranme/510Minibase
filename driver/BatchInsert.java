package driver;

import BigT.Map;
import BigT.bigT;
import btree.AddFileEntryException;
import btree.BTreeFile;
import btree.ConstructPageException;
import btree.GetFileEntryException;
import diskmgr.BigDB;
import global.AttrType;
import global.GlobalConst;
import global.SystemDefs;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;

import java.io.*;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class BatchInsert {
    //perform the batchinsertion but not in a main method
    public static void batchinsert(String fileName, int type, String bigtName, int numbuf) throws Exception
    {
        URL url = BatchInsert.class.getResource("/data/".concat(fileName));

        File csvFile = new File(url.getPath());
        BufferedReader read = new BufferedReader(new FileReader(csvFile));
        String line,rec[];
        Map temp;

        boolean new_flag = false;

        int c=0;
        // bigT b1 = new bigT(bigtName,type);
        BigDB bdB;
        if((bdB=Driver.usedDbMap.get(fileName+"_"+type))==null){
            bdB = new BigDB(type);
            bdB.initBigT(bigtName);
            new_flag = true;
        }

        while ((line = read.readLine()) != null) {
            rec = line.split(",");
            temp = new Map();
            if(c==0) {
                temp.setRowLabel(rec[0].substring(1));
            }
            else {
                temp.setRowLabel(rec[0]);
            }
            temp.setColumnLabel(rec[1]);
            temp.setTimeStamp(Integer.parseInt(rec[3]));
            temp.setValue(rec[2]);
            bdB.bigt.insertMap(temp.getMapByteArray());
            
            c++;
            System.out.println(c);
            // if (c == 100) break;
        }
        System.out.println("Map Count : "+bdB.bigt.getMapCnt());
        bdB.bigt.btf.close();
        bdB.bigt.btfTS.close();
        read.close();

        Driver.usedDbMap.put(bigtName, bdB);
    }
}
