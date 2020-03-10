package BigT;

import heap.*;
import global.*;
import bufmgr.*;
import diskmgr.*;

import java.io.*;

import BigT.Map;

/**
 * O_buf::Put takes maps and stores them on the buffer pages that were passed to
 * O_buf::init. O_buf::flush inserts them enmass into a temporary HeapFile.
 */
public class OBuf implements GlobalConst {

  /**
   * fault constructor no args -- use init to initialize
   */
  public OBuf() {
  }

  /**
   * O_buf is an output buffer. It takes as input:
   * 
   * @param bufs    temporary buffer to pages.(EACH ELEMENT IS A SINGLE BUFFER
   *                PAGE).
   * @param n_pages the number of pages
   * @param mSize   map size
   * @param temp_fd fd of a HeapFile
   * @param buffer  true => it is used as a buffer => if it is flushed, print a
   *                nasty message. it is false by default.
   */
  public void init(byte[][] bufs, int n_pages, int mSize, Heapfile temp_fd, boolean buffer) {
    _bufs = bufs;
    _n_pages = n_pages;
    m_size = mSize;
    _temp_fd = temp_fd;

    dirty = false;
    m_per_pg = MINIBASE_PAGESIZE / m_size;
    m_in_buf = n_pages * m_per_pg;
    m_wr_to_pg = 0;
    m_wr_to_buf = 0;
    m_written = 0L;
    curr_page = 0;
    buffer_only = buffer;
  }

  /**
   * Writes a map to the output buffer
   * 
   * @param buf the map written to buffer
   * @return the position of map which is in buffer
   * @exception IOException some I/O fault
   * @exception Exception   other exceptions
   */
  public Map Put(Map buf) throws IOException, Exception {

    byte[] copybuf;
    copybuf = buf.getMapByteArray();
    System.arraycopy(copybuf, 0, _bufs[curr_page], m_wr_to_pg * m_size, m_size);
    Map map_ptr = new Map(_bufs[curr_page], m_wr_to_pg * m_size);

    m_written++;
    m_wr_to_pg++;
    m_wr_to_buf++;
    dirty = true;

    if (m_wr_to_buf == m_in_buf) // Buffer full?
    {
      flush(); // Flush it

      m_wr_to_pg = 0;
      m_wr_to_buf = 0; // Initialize page info
      curr_page = 0;
    } else if (m_wr_to_pg == m_per_pg) {
      m_wr_to_pg = 0;
      curr_page++;
    }

    return map_ptr;
  }

  /**
   * returns the # of maps written.
   * 
   * @return the numbers of maps written
   * @exception IOException some I/O fault
   * @exception Exception   other exceptions
   */
  public long flush() throws IOException, Exception {
    int count;
    int bytes_written = 0;
    byte[] tempbuf = new byte[m_size];
    if (buffer_only == true)
      System.out.println("Stupid error - but no error protocol");

    if (dirty) {
      for (count = 0; count <= curr_page; count++) {
        RID rid;
        // Will have to go thru entire buffer writing maps to disk

        if (count == curr_page)
          for (int i = 0; i < m_wr_to_pg; i++) {
            System.arraycopy(_bufs[count], m_size * i, tempbuf, 0, m_size);
            try {
              rid = _temp_fd.insertRecord(tempbuf);
            } catch (Exception e) {
              throw e;
            }
          }
        else
          for (int i = 0; i < m_per_pg; i++) {
            System.arraycopy(_bufs[count], m_size * i, tempbuf, 0, m_size);
            try {
              rid = _temp_fd.insertRecord(tempbuf);
            } catch (Exception e) {
              throw e;
            }
          }
      }

      dirty = false;
    }

    return m_written;
  }

  private boolean dirty; // Does this buffer contain dirty pages?
  private int m_per_pg, // # of maps that fit in 1 page
      m_in_buf; // # of maps that fit in the buffer
  private int m_wr_to_pg, // # of maps written to current page
      m_wr_to_buf; // # of maps written to buffer.
  private int curr_page; // Current page being written to.
  private byte[][] _bufs; // Array of pointers to buffer pages.
  private int _n_pages; // number of pages in array
  private int m_size; // Size of a map
  private long m_written; // # of map written so far.
  private int TEST_temp_fd; // fd of a temporary file
  private Heapfile _temp_fd;
  private boolean buffer_only;
}