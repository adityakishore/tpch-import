package com.mapr.tools;

import org.apache.hadoop.hbase.util.Order;
import org.apache.hadoop.hbase.util.OrderedBytes;
import org.apache.hadoop.hbase.util.SimplePositionedByteRange;

public final class Helper {

  public static byte[] doubleToOrderedBytes(double d) {
    byte[] bytes = new byte[9];
    OrderedBytes.encodeFloat64(new SimplePositionedByteRange(bytes), d, Order.ASCENDING);
    return bytes;
  }

}
