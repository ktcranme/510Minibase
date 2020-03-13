package BigT;

import heap.*;
import global.*;
import diskmgr.*;
import bufmgr.*;

import java.io.*;

import BigT.Map;
import BigT.Stream;

public class SpoofIbuf implements GlobalConst {

  /**
   * constructor, use the init to initialize
   */
  public void SpoofIbuf() {

    hf_stream = null;
  }

  /**
   * Initialize some necessary inormation, call Iobuf to create the object, and
   * call init to finish intantiation
   * 
   * @param bufs[][] the I/O buffer
   * @param n_pages  the numbers of page of this buffer
   * @param mSize    the map size
   * @param fd       the reference to an Heapfile
   * @param Nmaps    the map numbers of the page
   * @exception IOException some I/O fault
   * @exception Exception   other exceptions
   */
  public void init(VMapfile fd, byte bufs[][], int n_pages, int mSize, int Nmaps) throws IOException, Exception {
    _fd = fd;
    _bufs = bufs;
    _n_pages = n_pages;
    m_size = mSize;

    m_proc = 0;
    m_in_buf = 0;
    tot_m_proc = 0;
    curr_page = 0;
    m_rd_from_pg = 0;
    done = false;
    m_per_pg = MINIBASE_PAGESIZE / m_size;

    n_maps = Nmaps;

    // open a scan
    if (hf_stream != null)
      hf_stream = null;

    try {
      hf_stream = _fd.openStream();
    } catch (Exception e) {
      throw e;
    }

  }

  /**
   * get a map from current buffer,pass reference buf to this method
   * usage:temp_map = map.Get(buf);
   * 
   * @param buf write the result to buf
   * @return the result map
   * @exception IOException some I/O fault
   * @exception Exception   other exceptions
   */
  public Map Get(Map buf) throws IOException, Exception {
    if (tot_m_proc == n_maps)
      done = true;

    if (done == true) {
      buf = null;
      return null;
    }
    if (m_proc == m_in_buf) {
      try {
        m_in_buf = readin();
      } catch (Exception e) {
        throw e;
      }
      curr_page = 0;
      m_rd_from_pg = 0;
      m_proc = 0;
    }

    if (m_in_buf == 0) // No maps read in?
    {
      done = true;
      buf = null;
      return null;
    }

    buf.mapSet(_bufs[curr_page], m_rd_from_pg * m_size);
    tot_m_proc++;

    // Setup for next read
    m_rd_from_pg++;
    m_proc++;
    if (m_rd_from_pg == m_per_pg) {
      m_rd_from_pg = 0;
      curr_page++;
    }
    return buf;
  }

  /**
   * @return if the buffer is empty,return true. otherwise false
   */
  public boolean empty() {
    if (tot_m_proc == n_maps)
      done = true;
    return done;
  }

  /**
   *
   * @return the numbers of maps in the buffer
   * @exception IOException               some I/O fault
   * @exception InvalidTupleSizeException Heapfile error
   */
  private int readin() throws IOException, InvalidTupleSizeException {
    int m_read = 0, tot_read = 0;
    Map m = new Map();
    byte[] m_copy;

    curr_page = 0;
    while (curr_page < _n_pages) {
      while (m_read < m_per_pg) {
        MID mid = new MID();
        try {
          if ((m = hf_stream.getNext(mid)) == null)
            return tot_read;
          m_copy = m.getMapByteArray();
          System.arraycopy(m_copy, 0, _bufs[curr_page], m_read * m_size, m_size);
        } catch (Exception e) {
          System.err.println("" + e);
        }
        m_read++;
        tot_read++;
      }
      m_read = 0;
      curr_page++;
    }
    return tot_read;
  }

  private byte[][] _bufs;

  private int TEST_fd;

  private VMapfile _fd;
  private Stream hf_stream;
  private int _n_pages;
  private int m_size;

  private int m_proc, m_in_buf;
  private int tot_m_proc;
  private int m_rd_from_pg, curr_page;
  private int m_per_pg;
  private boolean done;
  private int n_maps;
}