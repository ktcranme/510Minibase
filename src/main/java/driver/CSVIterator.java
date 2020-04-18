package driver;

import BigT.Iterator;
import BigT.Map;
import bufmgr.PageNotReadException;
import heap.InvalidTypeException;
import index.IndexException;
import iterator.*;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.input.BOMInputStream;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;

public class CSVIterator extends Iterator {
    java.util.Iterator<CSVRecord> csvItr;

    public CSVIterator(String fileName) throws UnsupportedEncodingException, IOException {
        Reader reader = new InputStreamReader(new BOMInputStream(BatchInsert.class.getResourceAsStream("/data/"+fileName)), "UTF-8");
        CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT);
        csvItr = csvParser.iterator();
    }

    public Map get_next() throws IOException, IndexException, InvalidTypeException, PageNotReadException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
        if(csvItr.hasNext()) {
            CSVRecord csvRecord = csvItr.next();
            Map tempMap = new Map();
            tempMap.setRowLabel(csvRecord.get(0));
            tempMap.setColumnLabel(csvRecord.get(1));
            tempMap.setTimeStamp(Integer.parseInt(csvRecord.get(3)));
            tempMap.setValue(csvRecord.get(2));
            return tempMap;
        }
        else {
            return null;
        }
    }

    public void close() throws IOException, JoinsException, SortException, IndexException {

    }
}
