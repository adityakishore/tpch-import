package com.mapr.tools;

import org.apache.hadoop.hbase.util.Bytes;

public final class Helper {

  public static byte[] doubleToOrderedBytes(double d) {
    return Bytes.toBytes(d);
//    byte[] bytes = new byte[9];
//    OrderedBytes.encodeFloat64(new SimplePositionedByteRange(bytes), d, Order.ASCENDING);
//    return bytes;
  }

}
