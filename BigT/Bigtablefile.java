package BigT;

import java.io.IOException;

import diskmgr.Page;
import global.MID;
import global.PageId;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.InvalidUpdateException;
import heap.SpaceNotAvailableException;

public interface Bigtablefile {
    public PageId getFirstDirPageId();
    public abstract Mapview getNewDataPage();
    public abstract Mapview getNewDataPage(Page page);
    public abstract Mapview getNewDataPage(Page page, PageId pid) throws IOException ;
    public abstract int getMapCnt() throws InvalidSlotNumberException, InvalidTupleSizeException, HFDiskMgrException,
    HFBufMgrException, IOException;
    public abstract Map getMap(MID rid) throws InvalidSlotNumberException, InvalidTupleSizeException, HFException, HFDiskMgrException, HFBufMgrException, Exception;
    public abstract boolean updateMap(MID rid, Map newtuple) throws InvalidSlotNumberException, InvalidUpdateException, InvalidTupleSizeException, HFException, HFDiskMgrException, HFBufMgrException, Exception;
    public abstract boolean deleteMap(MID rid) throws InvalidSlotNumberException, InvalidTupleSizeException, HFException, HFBufMgrException, HFDiskMgrException, Exception;
    public abstract MID insertMap(Map tuple) throws InvalidSlotNumberException, InvalidTupleSizeException, SpaceNotAvailableException, HFException, HFBufMgrException, HFDiskMgrException, IOException;
    public abstract Stream openStream() throws InvalidMapSizeException, InvalidTupleSizeException, HFBufMgrException, InvalidSlotNumberException, IOException;
}