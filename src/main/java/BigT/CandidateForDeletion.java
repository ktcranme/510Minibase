package BigT;

import global.MID;
import storage.StorageType;

public class CandidateForDeletion {
    Map _map;
    MID _mid;
    StorageType _type;
    boolean deleteMe = true;

    public CandidateForDeletion(Map map, MID mid, StorageType type) {
        _map = map;
        _type = type;

        //have to create our own because the
        //getNext() operator just overwrites each thing
        if(mid != null) {
            _mid = new MID();
            _mid.pageNo.pid = mid.pageNo.pid;
            _mid.slotNo = mid.slotNo;
        } else {
            _mid = null;
        }
    }

    public Map getMap() {
        return _map;
    }

    public MID getMID() {
        return _mid;
    }

    public StorageType getType() {
        return _type;
    }

    public void setDeleteMe(boolean b) {
        deleteMe = b;
    }
    public boolean getDeleteMe() {
        return deleteMe;
    }
}
