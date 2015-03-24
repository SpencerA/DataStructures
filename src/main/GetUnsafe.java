import sun.misc.Unsafe;

import java.lang.reflect.Field;

public class GetUnsafe {
  private GetUnsafe() { }
  public static Unsafe getUnsafe() {
    // Not on bootclasspath
    if( GetUnsafe.class.getClassLoader() == null )
      return Unsafe.getUnsafe();
    try {
      final Field fld = Unsafe.class.getDeclaredField("theUnsafe");
      fld.setAccessible(true);
      return (Unsafe) fld.get(GetUnsafe.class);
    } catch (Exception e) {
      throw new RuntimeException("Could not get sun.misc.Unsafe", e);
    }
  }
}
