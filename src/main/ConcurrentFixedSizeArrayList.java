import sun.misc.Unsafe;
import java.io.Serializable;

/** A wait-free, array list that supports concurrent add operations. It is expected that
 *  this data structure is written to in a multi-threaded fashion and then read from only
 *  to obtain the underlying long[], which is published safely.
 *
 *  This data structure does not support resizing, which is admittedly a major drawback.
 */
class ConcurrentFixedSizeArrayList implements Serializable {
  public void add( long l ) { _cal.add(l); }
  public long[] safePublish() { return _cal._l.clone(); }
  public long internal_size() { return _cal._cur; }
  private volatile CAL _cal; // the underlying concurrent array list
  ConcurrentFixedSizeArrayList(int sz) { _cal=new CAL(sz); }

  private static class CAL implements Serializable {
    private static final Unsafe U = GetUnsafe.getUnsafe();
    protected int _cur;  // the current index into the array, updated atomically
    private long[] _l; // the backing array
    static private final long _curOffset;
    static {
      try {
        _curOffset = U.objectFieldOffset(CAL.class.getDeclaredField("_cur"));
      } catch(Exception e) {
        throw new RuntimeException("Could not set offset with theUnsafe");
      }
    }
    CAL(int s) { _cur=0; _l=new long[s]; }
    private void add( long l ) {
      int c = _cur;
      while(!U.compareAndSwapInt(this,_curOffset,c,c+1))
        c=_cur;
      _l[c]=l;
    }
  }
}
