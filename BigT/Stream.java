package BigT;

/** JAVA */
/**
 * Stream.java-  class Stream
 *
 * Here is how you interact with maps:
 * getNext() -> stream through the file, record by record, not caring about
 *              the MIDs
 * insertRecord() -> Insert a completely new record. If the row/column
 *              combination is already there, I dont care. You need to take
 *              care of it. You'll get an MID (which mentions version as 0)
 * updateRecord() -> You have to provide an MID and we will update the record
 *              to reflect the latest timestamp and maintain 3 versions. The
 *              version provided in the MID will no necessarily be affected.
 *              Depends on the timestamp used.
 * deleteRecord() -> Deletes the entire row/column combination (all versions).
 *               Requires MID.
 * getMap() -> You have to provide an MID with a version to see, or you
 *              default to the latest version.
 * 
 */

import java.io.*;
import global.*;
import bufmgr.*;
import diskmgr.*;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.HFPage;
import heap.HFBufMgrException;
import heap.DataPageInfo;
import heap.Tuple;

/**
 * A Stream object is created ONLY through the function openStream of a
 * HeapFile. It supports the getNext interface which will simply retrieve the
 * next record in the heapfile.
 *
 * An object of type scan will always have pinned one directory page of the
 * heapfile.
 */
public class Stream implements GlobalConst {

  /**
   * Note that one record in our way-cool HeapFile implementation is specified by
   * six (6) parameters, some of which can be determined from others:
   */

  /** The heapfile we are using. */
  private Heapfile _hf;

  /** PageId of current directory page (which is itself an HFPage) */
  private PageId dirpageId = new PageId();

  /** pointer to in-core data of dirpageId (page is pinned) */
  private HFPage dirpage = new HFPage();

  /**
   * record ID of the DataPageInfo struct (in the directory page) which describes
   * the data page where our current record lives.
   */
  private RID datapageRid = new RID();

  /** the actual PageId of the data page with the current record */
  private PageId datapageId = new PageId();

  /** in-core copy (pinned) of the same */
  private HFPage datapage = new HFPage();

  /** record ID of the current record (from the current data page) */
  private MID userrid = new MID();

  /** Status of next user status */
  private boolean nextUserStatus;

  private int currentMapVersion = 0;

  /**
   * The constructor pins the first directory page in the file and initializes its
   * private data members from the private data member from hf
   *
   * @exception InvalidMapSizeException Invalid tuple size
   * @exception IOException             I/O errors
   *
   * @param hf A HeapFile object
   * @throws InvalidSlotNumberException
   * @throws HFBufMgrException
   */
  public Stream(Heapfile hf) throws InvalidMapSizeException, InvalidTupleSizeException, IOException, HFBufMgrException,
      InvalidSlotNumberException {
    init(hf);
  }

  /**
   * Retrieve the next record in a sequential scan
   *
   * @exception InvalidMapSizeException Invalid tuple size
   * @exception IOException             I/O errors
   *
   * @param rid Record ID of the record
   * @return the Map of the retrieved record.
   */
  public Map getNext(MID rid) throws InvalidMapSizeException, InvalidTupleSizeException, IOException {
    Map recptrtuple = null;

    if (currentMapVersion == 0 && nextUserStatus != true) {
      nextDataPage();
    }

    if (datapage == null)
      return null;

    System.out.println("NEXT USERRID SLOT: " + userrid.slotNo);
    rid.pageNo.pid = userrid.pageNo.pid;
    rid.slotNo = userrid.slotNo * 3 + currentMapVersion;

    try {
      recptrtuple = datapage.getMap(rid, currentMapVersion);
      currentMapVersion = recptrtuple.hasNext ? (currentMapVersion + 1) % 3 : 0;
    }

    catch (Exception e) {
      // System.err.println("SCAN: Error in Stream" + e);
      e.printStackTrace();
    }

    if (currentMapVersion != 0)
      return recptrtuple;

    userrid = datapage.nextMap(userrid);
    if (userrid == null)
      nextUserStatus = false;
    else
      nextUserStatus = true;

    return recptrtuple;
  }

  /**
   * Position the scan cursor to the record with the given rid.
   * 
   * @exception InvalidMapSizeException Invalid tuple size
   * @exception IOException             I/O errors
   * @param rid Record ID of the given record
   * @return true if successful, false otherwise.
   * @throws InvalidSlotNumberException
   * @throws HFBufMgrException
   */
  public boolean position(MID rid) throws InvalidMapSizeException, InvalidTupleSizeException, IOException,
      HFBufMgrException, InvalidSlotNumberException {
    MID nxtrid = new MID();
    boolean bst;

    bst = peekNext(nxtrid);

    if (nxtrid.equals(rid) == true)
      return true;

    // This is kind lame, but otherwise it will take all day.
    PageId pgid = new PageId();
    pgid.pid = rid.pageNo.pid;

    if (!datapageId.equals(pgid)) {

      // reset everything and start over from the beginning
      reset();

      bst = firstDataPage();

      if (bst != true)
        return bst;

      while (!datapageId.equals(pgid)) {
        bst = nextDataPage();
        if (bst != true)
          return bst;
      }
    }

    // Now we are on the correct page.

    try {
      userrid = datapage.firstMap();
    } catch (Exception e) {
      e.printStackTrace();
    }

    if (userrid == null) {
      bst = false;
      return bst;
    }

    bst = peekNext(nxtrid);

    while ((bst == true) && (nxtrid != rid))
      bst = mvNext(nxtrid);

    return bst;
  }

  /**
   * Do all the constructor work
   *
   * @exception InvalidMapSizeException Invalid tuple size
   * @exception IOException             I/O errors
   *
   * @param hf A HeapFile object
   * @throws InvalidSlotNumberException
   * @throws HFBufMgrException
   */
  private void init(Heapfile hf) throws InvalidMapSizeException, InvalidTupleSizeException, IOException,
      HFBufMgrException, InvalidSlotNumberException {
    _hf = hf;

    firstDataPage();
  }

  /** Closes the Stream object */
  public void closestream() {
    reset();
  }

  /** Reset everything and unpin all pages. */
  private void reset() {

    if (datapage != null) {

      try {
        unpinPage(datapageId, false);
      } catch (Exception e) {
        // System.err.println("SCAN: Error in Stream" + e);
        e.printStackTrace();
      }
    }
    datapageId.pid = 0;
    datapage = null;

    if (dirpage != null) {

      try {
        unpinPage(dirpageId, false);
      } catch (Exception e) {
        // System.err.println("SCAN: Error in Stream: " + e);
        e.printStackTrace();
      }
    }
    dirpage = null;

    nextUserStatus = true;

  }

  private boolean loadNextDirectoryPage() throws IOException, HFBufMgrException, InvalidSlotNumberException,
      InvalidTupleSizeException {
    PageId nextDirPageId = new PageId();

    if (dirpage == null) {
      /** copy data about first directory page */
      nextDirPageId.pid = _hf.getFirstDirPageId().pid;
      nextUserStatus = true;
    } else {
      nextDirPageId = dirpage.getNextPage();
      unpinPage(dirpageId, false);
      dirpage = null;
    }
    
    if (nextDirPageId.pid != INVALID_PAGE) {
      // ASSERTION:
      // - nextDirPageId has correct id of the page which is to get
      /** get directory page and pin it */
      dirpageId.pid = nextDirPageId.pid;
      dirpage = new HFPage();
      pinPage(dirpageId, (Page) dirpage, false);
    }

    datapage = null;
    datapageId.pid = INVALID_PAGE;
    datapageRid = null;

    return dirpage != null;
  }


  /**
   * Move to the first data page in the file.
   * 
   * @throws InvalidMapSizeException Invalid tuple size
   * @throws InvalidTupleSizeException Invalid tuple size
   * @throws IOException             I/O errors
   * @throws InvalidSlotNumberException
   * @throws HFBufMgrException
   * @return true if successful false otherwise
   */
  private boolean firstDataPage() throws InvalidMapSizeException, InvalidTupleSizeException, IOException,
      HFBufMgrException, InvalidSlotNumberException
             {
              dirpage = null;
              DataPageInfo dpinfo;
              Tuple rectuple = null;
              
              if (!loadNextDirectoryPage() &&
                /** the first directory page is the only one which can possibly remain
                  * empty: therefore try to get the next directory page and
                  * check it. The next one has to contain a datapage record, unless
                  * the heapfile is empty:
                  */
                !loadNextDirectoryPage()) {
                // Heapfile is empty
                System.err.println("Heapfile is empty!");
                return false;
              }
              
              // Prepare to fetch datapage
              try {
                datapageRid = dirpage.firstRecord();
              } catch (Exception e) {
                datapageRid = null;
                return false;
              }

              if (datapageRid == null) {
                datapageId.pid = INVALID_PAGE;
                unpinPage(dirpageId, false);
                dirpage = null;
                return false;
              }

              rectuple = dirpage.getRecord(datapageRid);
              
              if (rectuple.getLength() != DataPageInfo.size)
                return false;

              dpinfo = new DataPageInfo(rectuple);
              datapageId.pid = dpinfo.getPageId().pid;

               datapage = null;

               try{
                 nextDataPage();
               }

               catch (Exception e) {
                 //  System.err.println("SCAN Error: 1st_next 0: " + e);
                 e.printStackTrace();
               }

               return true;

               /** ASSERTIONS:
                * - first directory page pinned
                * - this->dirpageId has Id of first directory page
                * - this->dirpage valid
                * - if heapfile empty:
                *    - this->datapage == NULL, this->datapageId==INVALID_PAGE
                * - if heapfile nonempty:
                *    - this->datapage == NULL, this->datapageId, this->datapageRid valid
                *    - first datapage is not yet pinned
                */

             }


  /** Move to the next data page in the file and 
   * retrieve the next data page. 
   *
   * @return 		true if successful
   *			false if unsuccessful
   */
  private boolean nextDataPage() 
      throws InvalidMapSizeException,
                      InvalidTupleSizeException,
                      IOException
             {
               DataPageInfo dpinfo;

               boolean nextDataPageStatus;
               PageId nextDirPageId = new PageId();
               Tuple rectuple = null;

               // ASSERTIONS:
               // - this->dirpageId has Id of current directory page
               // - this->dirpage is valid and pinned
               // (1) if heapfile empty:
               //    - this->datapage==NULL; this->datapageId == INVALID_PAGE
               // (2) if overall first record in heapfile:
               //    - this->datapage==NULL, but this->datapageId valid
               //    - this->datapageRid valid
               //    - current data page unpinned !!!
               // (3) if somewhere in heapfile
               //    - this->datapageId, this->datapage, this->datapageRid valid
               //    - current data page pinned
               // (4)- if the scan had already been done,
               //        dirpage = NULL;  datapageId = INVALID_PAGE

               if ((dirpage == null) && (datapageId.pid == INVALID_PAGE))
                 return false;

               if (datapage == null) {
                 if (datapageId.pid == INVALID_PAGE) {
                   // heapfile is empty to begin with

                   try{
                     unpinPage(dirpageId, false);
                     dirpage = null;
                   }
                   catch (Exception e){
                     //  System.err.println("Stream: Chain Error: " + e);
                     e.printStackTrace();
                   }

                 } else {

                   // pin first data page
                   try {
                     datapage  = new HFPage();
                     pinPage(datapageId, (Page) datapage, false);
                   }
                   catch (Exception e){
                     e.printStackTrace();
                   }

                   try {
                     userrid = datapage.firstMap();
                   }
                   catch (Exception e) {
                     e.printStackTrace();
                   }

                   return true;
                 }
               }

               // ASSERTIONS:
               // - this->datapage, this->datapageId, this->datapageRid valid
               // - current datapage pinned

               // unpin the current datapage
               try{
                 unpinPage(datapageId, false /* no dirty */);
                 datapage = null;
               }
               catch (Exception e){

               }

               // read next datapagerecord from current directory page
               // dirpage is set to NULL at the end of scan. Hence

               if (dirpage == null) {
                 return false;
               }

               datapageRid = dirpage.nextRecord(datapageRid);

              if (datapageRid == null) {
                nextDataPageStatus = false;
                // we have read all datapage records on the current directory page

                try {
                  if (!loadNextDirectoryPage()) {
                    return false;
                  }
                  datapageRid = dirpage.firstRecord();
                  nextDataPageStatus = true;
                } catch (Exception e) {
                  System.err.println(e);
                  return false;
                }
              }

               // ASSERTION:
               // - this->dirpageId, this->dirpage valid
               // - this->dirpage pinned
               // - the new datapage to be read is on dirpage
               // - this->datapageRid has the Rid of the next datapage to be read
               // - this->datapage, this->datapageId invalid

               // data page is not yet loaded: read its record from the directory page
               try {
                 rectuple = dirpage.getRecord(datapageRid);
               }

               catch (Exception e) {
                 System.err.println("HeapFile: Error in Stream" + e);
               }

               if (rectuple.getLength() != DataPageInfo.size)
                 return false;

               dpinfo = new DataPageInfo(rectuple);
               datapageId.pid = dpinfo.getPageId().pid;

               try {
                 datapage = new HFPage();
                 pinPage(dpinfo.getPageId(), (Page) datapage, false);
               }

               catch (Exception e) {
                 System.err.println("HeapFile: Error in Stream" + e);
               }


               // - directory page is pinned
               // - datapage is pinned
               // - this->dirpageId, this->dirpage correct
               // - this->datapageId, this->datapage, this->datapageRid correct

               userrid = datapage.firstMap();

               if(userrid == null)
               {
                 nextUserStatus = false;
                 return false;
               }

               return true;
             }


  private boolean peekNext(MID rid) {

    rid.pageNo.pid = userrid.pageNo.pid;
    rid.slotNo = userrid.slotNo;
    return true;

  }


  /** Move to the next record in a sequential scan.
   * Also returns the RID of the (new) current record.
   */
  private boolean mvNext(MID rid) 
      throws InvalidMapSizeException,
             InvalidTupleSizeException,
                      IOException
             {
               MID nextrid;
               boolean status;

               if (datapage == null)
                 return false;

               nextrid = datapage.nextMap(rid);

               if( nextrid != null ){
                 userrid.pageNo.pid = nextrid.pageNo.pid;
                 userrid.slotNo = nextrid.slotNo;
                 return true;
               } else {

                 status = nextDataPage();

                 if (status==true){
                   rid.pageNo.pid = userrid.pageNo.pid;
                   rid.slotNo = userrid.slotNo;
                 }

               }
               return true;
             }

  /**
   * short cut to access the pinPage function in bufmgr package.
   * @see bufmgr.pinPage
   */
  private void pinPage(PageId pageno, Page page, boolean emptyPage)
      throws HFBufMgrException {

      try {
        SystemDefs.JavabaseBM.pinPage(pageno, page, emptyPage);
      }
      catch (Exception e) {
        throw new HFBufMgrException(e,"Stream.java: pinPage() failed");
      }

  } // end of pinPage

  /**
   * short cut to access the unpinPage function in bufmgr package.
   * @see bufmgr.unpinPage
   */
  private void unpinPage(PageId pageno, boolean dirty)
      throws HFBufMgrException {

      try {
        SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
      }
      catch (Exception e) {
        throw new HFBufMgrException(e,"Stream.java: unpinPage() failed");
      }

  } // end of unpinPage


}
