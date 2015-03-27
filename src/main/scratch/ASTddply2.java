package water.rapids;

import sun.misc.Unsafe;
import water.*;
import water.fvec.*;
import water.nbhm.NonBlockingHashSet;
import water.nbhm.UtilUnsafe;
import water.parser.ValueString;
import water.util.Log;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicInteger;


/** plyr's ddply: GroupBy by any other name.
*  Sample AST: (h2o.ddply $frame {1;5;10} $fun)
*
*  First arg is the frame we'll be working over.
*  Second arg is column selection to group by.
*  Third arg is the function to apply to each group.
*/
public class ASTddply2 extends ASTOp {
  protected static long[] _cols;
  protected static String _fun;
  protected static AST[] _fun_args;
  static final String VARS[] = new String[]{ "ary", "{cols}", "FUN"};
  public ASTddply2( ) { super(VARS); }

  @Override String opStr(){ return "h2o.ddply";}
  @Override ASTOp make() {return new ASTddply2();}

  @Override ASTddply2 parse_impl(Exec E) {
    // get the frame to work
    AST ary = E.parse();
    if (ary instanceof ASTId) ary = Env.staticLookup((ASTId)ary);

    // Get the col ids
    AST s=null;
    try {
      s = E.skipWS().parse(); // this jumps out to le squiggly du parse
      _cols = ((ASTSeries)s).toArray(); // do the dump to array here -- no worry about efficiency or speed

      // SANITY CHECK COLS:
      if (_cols.length > 1000) throw new IllegalArgumentException("Too many columns selected. Please select < 1000 columns.");

    } catch (ClassCastException e) {

      assert s != null;
      try {
        _cols = new long[]{(long)((ASTNum)s).dbl()};
      } catch (ClassCastException e2) {
        throw new IllegalArgumentException("Badly formed AST. Columns argument must be a ASTSeries or ASTNum");
      }
    }

    // get the fun
    _fun = ((ASTId)E.skipWS().parse())._id;

    // get any fun args
    ArrayList<AST> fun_args = new ArrayList<>();
    while(E.skipWS().hasNext()) {
      fun_args.add(E.parse());
    }
    ASTddply2 res = (ASTddply2) clone();
    res._asts = new AST[]{ary};
    if (fun_args.size() > 0) {
      _fun_args = fun_args.toArray(new AST[fun_args.size()]);
    } else {
      _fun_args = null;
    }
    return res;
  }

  @Override void apply(Env env) {
    Frame fr = env.popAry();
    for (long l : _cols) {
      if (l > fr.numCols() || l < 0) throw new IllegalArgumentException("Column "+(l+1)+" out of range for frame columns "+fr.numCols());
    }

    // Pass 1: Find Groups.
    // Build a NBHSet of unique double[]'s holding selection cols.
    // These are the unique groups, found per-node, rolled-up globally
    long s = System.currentTimeMillis();
    final ddplyPass1 p1 = new ddplyPass1(_cols).doAll(fr);
    Log.info("Finished ddply pass 1 in: " + (System.currentTimeMillis() - s)/1000. + " (s)");
    int nGrps = p1._g.size();
    GroupA[] grps;

    // multi-threaded instantiate all groups' _rows field
    s = System.currentTimeMillis();
    H2O.submitTask(new InstantiateRowsTask(grps=p1._g.toArray(new GroupA[nGrps]))).join();
    Log.info("Instantiate Rows Task: " + (System.currentTimeMillis() - s)/1000. + " (s)");

    s = System.currentTimeMillis();
    ddplyPass2 p2 = new ddplyPass2(p1._g,_cols).doAll(fr);
    Log.info("Finished ddply pass 2 in: " + (System.currentTimeMillis() - s)/1000. + " (s)");

    // pass3 to build the groups
    s = System.currentTimeMillis();
    H2O.submitTask(new ddplyPass3(fr, grps)).join();
    Log.info("Finished ddply pass 3 in: " + (System.currentTimeMillis() - s)/1000. + " (s)");


//    s = System.currentTimeMillis();
//    H2O.submitTask(new LoadTask(p1._g.toArray(new GroupA[nGrps]))).join();
//    Log.info("Time to create LongArrays: " + (System.currentTimeMillis() - s)/1000. + " (s)");

//    s = System.currentTimeMillis();
//    ddplyPass2 p2 = new ddplyPass2(p1._g, _cols).doAll(fr);
//    Log.info("Finished ddply pass 2 in: " + (System.currentTimeMillis() - s)/1000. + " (s)");
    System.out.println("moo: ");
  }

  // ---
  // Group descrption: unpacked selected double columns
  public static class GroupA extends Iced {
    private static final Unsafe U = UtilUnsafe.getUnsafe();
    public double _ds[];
    public int _hash;
    public Key _key;
    public int _node;
    public long _nrows;

    public NBALL _rows;
    static private final long _nrowsOffset;
    static {
      try {
        _nrowsOffset = U.objectFieldOffset(GroupA.class.getDeclaredField("_nrows"));
      } catch(Exception e) {
        throw new RuntimeException("Could not set offset with theUnsafe");
      }
    }
    public GroupA(int len) { _ds = new double[len]; }
    GroupA(double ds[]) { _ds = ds; _hash=hash(); _rows=null; }
    // Efficiently allow groups to be hashed & hash-probed
    public void fill(int row, Chunk chks[], long cols[]) {
      for( int c=0; c<cols.length; c++ ) // For all selection cols
        _ds[c] = chks[(int)cols[c]].atd(row); // Load into working array
      _hash = hash();
    }
    private int hash() {
      long h=0;                 // hash is sum of field bits
      for( double d : _ds ) h += Double.doubleToRawLongBits(d);
      // Doubles are lousy hashes; mix up the bits some
      h ^= (h>>>20) ^ (h>>>12);
      h ^= (h>>> 7) ^ (h>>> 4);
      return (int)((h^(h>>32))&0x7FFFFFFF);
    }
    public boolean has(double ds[]) { return Arrays.equals(_ds, ds); }
    @Override public boolean equals( Object o ) { return o instanceof GroupA && Arrays.equals(_ds,((GroupA)o)._ds); }
    @Override public int hashCode() { return _hash; }
    @Override public String toString() { return Arrays.toString(_ds); }
    private static boolean CAS( GroupA g, long o, long n) { return U.compareAndSwapLong(g,_nrowsOffset,o,n); }
    public void newRows() { _rows = new NBALL((int)_nrows); }
  }

  // Pass1: Find unique groups, based on a subset of columns.
  protected static class ddplyPass1 extends MRTask<ddplyPass1> {
    NonBlockingHashSet<GroupA> _g;
    long[] _cols;
    ddplyPass1(long[] cols) { _cols=cols; }
    @Override public void setupLocal() { _g = new NonBlockingHashSet<>(); }
    @Override public void map(Chunk[] c) {
      for (int i=0;i<c[0]._len;++i) {
        GroupA g = new GroupA(_cols.length);
        g.fill(i,c,_cols);
        if( !_g.add(g) ) g=_g.get(g);
        long r=g._nrows;
        while(!GroupA.CAS(g,r,r+1))
          r=g._nrows;
      }
    }
    @Override public void reduce(ddplyPass1 t) {
      if(_g!=t._g) {
        NonBlockingHashSet<GroupA> l = _g;
        NonBlockingHashSet<GroupA> r = t._g;
        if( l.size() < r.size() ) { l=r; r=_g; }  // larger on the left

        // loop over the smaller set of grps
        for( GroupA rg:r ) {
          GroupA lg = l.get(rg);
          if( lg == null) l.add(rg); // not in left, so put it and continue...
          else {                     // found the group, CAS in the row counts
            long R=lg._nrows;
            while(!GroupA.CAS(lg,R,R+rg._nrows))
              R=lg._nrows;
          }
        }
      }
    }

    @Override public AutoBuffer write_impl( AutoBuffer ab ) {
      ab.putA8(_cols);
      if( _g == null ) return ab.put4(0);
      ab.put4(_g.size());
      for( GroupA g: _g) ab.put(g);
      return ab;
    }

    @Override public ddplyPass1 read_impl(AutoBuffer ab) {
      _cols = ab.getA8();
      int len = ab.get4();
      if( len == 0 ) return this;
      _g = new NonBlockingHashSet<>();
      for( int i=0;i<len;++i) _g.add(ab.get(GroupA.class));
      return this;
    }
  }

  // ---
  // Pass 2: Build Groups.
  // Wrap Frame/Vec headers around all the local row-counts.


  private static class ForkedTask extends H2O.H2OCountedCompleter<ForkedTask> {
    private final GroupA _g;
    ForkedTask(H2O.H2OCountedCompleter cc, GroupA g) { super(cc); _g=g;}
    @Override protected void compute2() { _g.newRows(); tryComplete(); }
  }

  private static class InstantiateRowsTask extends H2O.H2OCountedCompleter<InstantiateRowsTask> {
    private final AtomicInteger _ctr;
    private final int _maxP = 5000;
    private final GroupA[] _g;
    InstantiateRowsTask(GroupA[] g) {_g=g; _ctr=new AtomicInteger(_maxP-1); }

    @Override protected void compute2() {
      addToPendingCount(_g.length-1);
      for( int i=0;i<Math.min(_g.length,_maxP);++i ) frkTsk(i);
    }

    private void frkTsk(final int i) { new ForkedTask(new Callback(), _g[i]).fork(); }

    private class Callback extends H2O.H2OCallback {
      public Callback(){super(InstantiateRowsTask.this);}
      @Override public void callback(H2O.H2OCountedCompleter cc) {
        int i = _ctr.incrementAndGet();
        if( i < _g.length )
          frkTsk(i);
      }
    }
  }

  private static class ddplyPass2 extends MRTask<ddplyPass2> {
    private NonBlockingHashSet<GroupA> _g;
    private long[] _cols;
    ddplyPass2( NonBlockingHashSet<GroupA> g, long[] cols) { _g=g; _cols=cols; }
    @Override public void map(Chunk[] c) {
      long start = c[0].start();
      for (int i=0;i<c[0]._len;++i) {
        GroupA g = new GroupA(_cols.length);
        g.fill(i, c, _cols);
        g=_g.get(g);
        g._rows.add(i+start);
      }
    }

    @Override public AutoBuffer write_impl( AutoBuffer ab ) {
      ab.putA8(_cols);
      if (_g == null ) return ab.put4(0);
      ab.put4(_g.size());
      for( GroupA g: _g) ab.put(g);
      return ab;
    }

    @Override public ddplyPass2 read_impl(AutoBuffer ab) {
      _cols = ab.getA8();
      int len = ab.get4();
      if( len == 0 ) return this;
      _g = new NonBlockingHashSet<>();
      for( int i=0;i<len;++i) _g.add(ab.get(GroupA.class));
      return this;
    }
  }

  private static class ddplyPass3Task extends H2O.H2OCountedCompleter<ddplyPass3Task> {
    private final Frame _fr;
    private final GroupA _g;
    private final Key[] _keys;
    private final int _i;
    private MRTask _m;
    ddplyPass3Task(H2O.H2OCountedCompleter cc, Frame fr, GroupA g, Key[] keys, int i) { super(cc); _fr=fr; _g=g; _keys=keys; _i=i; }
    @Override protected void compute2() {
      _keys[_i] = Key.make(".grpFr__"+_i+"_"+Key.make().toString());
      long s = System.currentTimeMillis();
      Frame f = new DeepSliceRows(_g._rows.unsafePublish()).doAll(_fr.numCols(),_fr,false).outputFrame(_keys[_i],_fr.names(),_fr.domains());
      DKV.put(_keys[_i],f);
      Log.info("Time to complete a DeepSliceRows: " + (System.currentTimeMillis()-s)/1000. + " (s)");
      tryComplete();
    }
  }

  private static class DeepSliceRows extends MRTask<DeepSliceRows> {
    private final long _rows[];
    DeepSliceRows( long rows[] ) {
      if(rows==null)
        throw new IllegalArgumentException("rows cannot be null.");
      _rows=rows;
    }

    @Override public void map( Chunk[] cs, NewChunk[] ncs ) {
      int len = cs[0]._len;
      long start = cs[0].start();
      long end = start + len;
      for( long l:_rows ) {
        if( l < start || l >= end) continue;
        int i = (int)(l-start);
        for( int c=0;c<cs.length;++c) {
          if( cs[c].isNA(i) ) ncs[c].addNA();
          else if( cs[c].vec().isUUID() ) ncs[c].addUUID(cs[c], i);
          else if( cs[c].vec().isString() ) ncs[c].addStr(cs[c].atStr(new ValueString(), i));
          else ncs[c].addNum(cs[c].atd(i));
        }
      }
    }
  }

  private static class ddplyPass3 extends H2O.H2OCountedCompleter<ddplyPass3> {

    //in
    private final Frame _fr;
    private final GroupA[] _grps;


    //out
    private final Key[] _keys;

    //parallelism
    final private int _maxP=1000;
    final private AtomicInteger _ctr;

    ddplyPass3(Frame fr, GroupA[] grps) { _fr=fr; _grps=grps; _ctr=new AtomicInteger(_maxP-1); _keys=new Key[grps.length]; }


    @Override protected void compute2() {
      addToPendingCount(_grps.length-1);
      for( int i=0;i<Math.min(_grps.length,_maxP);++i ) frkTsk(i);
    }

    private void frkTsk(final int i) { new ddplyPass3Task(new Callback(), _fr, _grps[i], _keys, i).fork(); }

    private class Callback extends H2O.H2OCallback {
      public Callback(){super(ddplyPass3.this);}
      @Override public void callback(H2O.H2OCountedCompleter cc) {
        int i = _ctr.incrementAndGet();
        if( i < _grps.length )
          frkTsk(i);
      }
    }
  }

  // ---
  // Called once-per-group, it executes the given function on the group.
  private static class RemoteExec extends DTask<RemoteExec> implements Freezable {
    // INS
    final Key _key;
    final Key[] _rows;
    final int _numgrps; // This group # out of total groups
    String _fun;
    AST[] _fun_args;
    // OUTS
    int _ncols;  // Number of result columns
    Key[] _vs;  // keys of the vecs created, 1 row per group results

    private int nGroups() {
      H2ONode[] array = H2O.CLOUD.members();
      Arrays.sort(array);
      // Give each H2ONode ngrps/#nodes worth of groups.  Round down for later nodes,
      // and round up for earlier nodes
      int ngrps = _numgrps / array.length;
      if( Arrays.binarySearch(array, H2O.SELF) < _numgrps - ngrps*array.length )
        ++ngrps;
      Log.info("Processing "+ngrps+" groups.");
      return ngrps;
    }

    private int grpStartIdx() {
      H2ONode[] array = H2O.CLOUD.members();
      Arrays.sort(array);
      int ngrps = _numgrps / array.length;
      int self = Arrays.binarySearch(array, H2O.SELF);
      int grp_start = 0;
      grp_start += ngrps * (self);
      // now +1 for any node idx satisfying the rounding prop
      for (int i = 0; i < self; ++i) {
        if (i < _numgrps - ngrps*array.length) grp_start++;
      }
      return grp_start;
    }

    RemoteExec(int numgrps, Key key, Key[] rows, String fun, AST[] fun_args ) {
      _key = key; _rows = rows; _numgrps = numgrps; _fun = fun; _fun_args = fun_args;
      // Always 1 higher priority than calling thread... because the caller will
      // block & burn a thread waiting for this MRTask to complete.
      Thread cThr = Thread.currentThread();
      _priority = (byte)((cThr instanceof H2O.FJWThr) ? ((H2O.FJWThr)cThr)._priority+1 : super.priority());
    }

    final private byte _priority;
    @Override public byte priority() { return _priority; }

    // Execute the function on the group
    @Override public void compute2() {
      NewChunk[] _nchks=null;             // a collection of new chunks
      AppendableVec[] _grpCols=null;
      int ngrps = nGroups();
      int grp_start = grpStartIdx();
      int grp_end = grp_start + ngrps;
      Log.info("Processing groups " + grp_start + "-" + (grp_end) + ".");
      Frame f = DKV.get(_key).get();
      Vec[] data = f.vecs();    // Full data columns
      for (; grp_start < grp_end; ) {

        Vec rows = DKV.get(_rows[grp_start++]).get();

        Vec[] gvecs = new Vec[data.length];
        Key[] keys = rows.group().addVecs(data.length);
        for (int c = 0; c < data.length; c++) {
          gvecs[c] = new SubsetVec(keys[c], rows.get_espc(), data[c]._key, rows._key);
          gvecs[c].setDomain(data[c].domain());
          DKV.put(gvecs[c]._key, gvecs[c]);
        }
        Frame aa = new Frame(Key.make(), f._names, gvecs);

        // Clone a private copy of the environment for local execution
        Env env = new Env(new HashSet<Key>());
        final ASTOp op = (ASTOp) ASTOp.get(_fun).clone();
        op.exec(env, new ASTFrame(aa), _fun_args);

        // Inspect the results; figure the result column count
        Frame fr = null;
        if (env.isAry() && (fr = env.popAry()).numRows() != 1)
          throw new IllegalArgumentException("Result of ddply can only return 1 row but instead returned " + fr.numRows());
        _ncols = fr == null ? 1 : fr.numCols();

        if (_nchks == null) {
          _nchks = new NewChunk[_ncols];
          _grpCols = new AppendableVec[_ncols];
          for (int i = 0; i < _ncols; ++i)
            _nchks[i] = new NewChunk(_grpCols[i] = new AppendableVec(Vec.VectorGroup.VG_LEN1.addVec()), 0);
        }
        for (int i = 0; i < _ncols; ++i) {
          _nchks[i].addNum(_ncols == 1 ? env.popDbl() : fr.vecs()[i].at(0));
        }
        aa.delete(); // nuke the group frame
      }
      assert _nchks!=null;
      _vs = new Key[_nchks.length];
      Futures fs = new Futures();
      for (int i=0; i < _vs.length;++i) _nchks[i].close(0, fs);
      for (int i =0; i < _vs.length;++i) _vs[i] = _grpCols[i].close(fs)._key;
      fs.blockForPending();
      tryComplete();
    }
  }
}

// not resizing!!!
class NBALL extends Iced {
  public NBALL(int sz) { _cal=new CAL(sz); }

  // API
  public void add( long l )     { _cal.add(l); }
  public long[] safePublish()   { return _cal._l.clone(); }
  public long[] unsafePublish() { return _cal._l; }
  public long internal_size()   { return _cal._cur; }


  private volatile CAL _cal; // the underlying concurrent array list... NO RESIZING!!!!

  private static class CAL extends Iced {
    private static final Unsafe U = UtilUnsafe.getUnsafe();
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

