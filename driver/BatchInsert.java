package driver;

import BigT.Map;
import btree.BTreeFile;
import diskmgr.BigDB;
import global.AttrType;
import global.GlobalConst;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URL;

public class BatchInsert {
    //insert your data file in /data folder before running the make command, to include the data file in classpath.
    public static void batchinsert(String fileName, int type, String bigtName, int numbuf) throws Exception
    {
        URL url = BatchInsert.class.getResource("/data/".concat(fileName));
        File csvFile = new File(url.getPath());
        BufferedReader read = new BufferedReader(new FileReader(csvFile));
        String line,rec[];
        Map temp;
        boolean new_flag = false;
        int c=0;
        BigDB bdB;
        BTreeFile tempbtf = null;
        if((bdB=Driver.usedDbMap.get(fileName+"_"+type))==null){
            bdB = new BigDB(type);
            bdB.initBigT(bigtName);
            new_flag = true;
            if(type!=4){
                tempbtf = new BTreeFile("batch_insert_ind", AttrType.attrString, GlobalConst.MAXCOLUMNLABELSIZE + GlobalConst.MAXROWLABELSIZE + 1, 1);
            }
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
            if(new_flag) {
                bdB.bigt.insertMapBulk(temp.getMapByteArray(),tempbtf);
            } else {
                bdB.insertMap(temp.getMapByteArray());
            }
            c++;
            System.out.println(c);
        }
        if(tempbtf!=null)
            tempbtf.destroyFile();
        Driver.usedDbMap.put(fileName+"_"+type , bdB);
    }
}
