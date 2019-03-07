package org.apache.spark.sql.execution;

public class MyBox {
  public static Long boxJ(long a) {return new Long(a);}
  public static Integer boxI(int a) {return new Integer(a);}
  public static Boolean boxZ(boolean a) {return new Boolean(a);}
  public static Double boxD(double a) {return new Double(a);}
}
