package com.mapr.tools.impl;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.mapr.tools.Constants;
import com.mapr.tools.Helper;
import com.mapr.tools.PutBuilder;
import com.mapr.tools.TableCreator;
import com.mapr.tools.impl.TsvParser.ParsedLine;

public final class Lineitem extends Configured implements TableCreator, PutBuilder, Constants {

  private static final byte[] FAMILY_1 = "F".getBytes();
  private static final byte[] FAMILY_2 = "G".getBytes();
  
  private static final String[] FULL_COLUMNS = {"l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus", "l_commitdate", "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment"};

  private static final String[] SHORT_COLUMNS = {"A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K"};
  
  private static final String[] COLUMNS = SHORT_COLUMNS;
  
  private static final byte[] L_QUANTITY = COLUMNS[0].getBytes();
  private static final byte[] L_EXTENDEDPRICE = COLUMNS[1].getBytes();
  private static final byte[] L_DISCOUNT = COLUMNS[2].getBytes();
  private static final byte[] L_TAX = COLUMNS[3].getBytes();
  private static final byte[] L_RETURNFLAG = COLUMNS[4].getBytes();
  private static final byte[] L_LINESTATUS = COLUMNS[5].getBytes();
  private static final byte[] L_COMMITDATE = COLUMNS[6].getBytes();
  private static final byte[] L_RECEIPTDATE = COLUMNS[7].getBytes();
  private static final byte[] L_SHIPINSTRUCT = COLUMNS[8].getBytes();
  private static final byte[] L_SHIPMODE = COLUMNS[9].getBytes();
  private static final byte[] L_COMMENT = COLUMNS[10].getBytes();

  public Lineitem() {
    super(HBaseConfiguration.create());
  }

  public Lineitem(Configuration conf) {
    super(conf);
  }

  public Put build(ParsedLine parsed) {
    int    l_orderkey       = Integer.valueOf(parsed.getColumn(0));
    int    l_partkey        = Integer.valueOf(parsed.getColumn(1));
    int    l_suppley        = Integer.valueOf(parsed.getColumn(2));
    byte   l_linenumber     = Byte.valueOf(parsed.getColumn(3));
    double l_quantity       = Double.valueOf(parsed.getColumn(4));
    double l_extendedprice  = Double.valueOf(parsed.getColumn(5));
    double l_discount       = Double.valueOf(parsed.getColumn(6));
    double l_tax            = Double.valueOf(parsed.getColumn(7));
    byte[] l_returnflag     = parsed.getColumnBytes(8);
    byte[] l_linestatus     = parsed.getColumnBytes(9);
    int    l_shipdate       = (int) (Date.valueOf(parsed.getColumn(10)).getTime() / MILLIS_IN_DAY);
    int    l_commitdate     = (int) (Date.valueOf(parsed.getColumn(11)).getTime() / MILLIS_IN_DAY);
    int    l_receiptdate    = (int) (Date.valueOf(parsed.getColumn(12)).getTime() / MILLIS_IN_DAY);
    byte[] l_shipinstruct   = parsed.getColumnBytes(13);
    byte[] l_shipmode       = parsed.getColumnBytes(14);
    byte[] l_comment        = parsed.getColumnBytes(15);

    // encode row_key
    ByteBuffer row_key = ByteBuffer
        .wrap(new byte[4+4+4+4+4]) // 4(l_shipdate) + 4(l_orderkey) + 4(l_linenumber) + 4(l_partkey) + 4(l_suppley)
        .order(ByteOrder.BIG_ENDIAN);
    row_key.putInt(l_shipdate);
    row_key.putInt(l_orderkey);
    row_key.put(l_linenumber);
    row_key.putInt(l_partkey);
    row_key.putInt(l_suppley);
    Put put = new Put((ByteBuffer) row_key.rewind());

    put.add(FAMILY_1, L_QUANTITY, Helper.doubleToOrderedBytes(l_quantity));
    put.add(FAMILY_1, L_EXTENDEDPRICE, Helper.doubleToOrderedBytes(l_extendedprice));
    put.add(FAMILY_1, L_DISCOUNT, Helper.doubleToOrderedBytes(l_discount));
    put.add(FAMILY_1, L_TAX, Helper.doubleToOrderedBytes(l_tax));

    put.add(FAMILY_1, L_RETURNFLAG, l_returnflag);
    put.add(FAMILY_1, L_LINESTATUS, l_linestatus);

    put.add(FAMILY_1, L_COMMITDATE, Bytes.toBytes(l_commitdate));
    put.add(FAMILY_1, L_RECEIPTDATE, Bytes.toBytes(l_receiptdate));
    put.add(FAMILY_1, L_SHIPINSTRUCT, l_shipinstruct);
    put.add(FAMILY_1, L_SHIPMODE, l_shipmode);

    put.add(FAMILY_2, L_COMMENT, l_comment);

    return put;
  }

  public void create(String tableName) throws Exception {
    try (HBaseAdmin admin = new HBaseAdmin(getConf())) {
      if (admin.tableExists(tableName)) {
        throw new TableExistsException(tableName);
      }
      HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
      tableDesc.addFamily(new HColumnDescriptor(FAMILY_1));
      tableDesc.addFamily(new HColumnDescriptor(FAMILY_2));
      admin.createTable(tableDesc, getSplitKeys());
    }
  }

  @SuppressWarnings("deprecation")
  private byte[][] getSplitKeys() {
    Date firstDate = Date.valueOf(getConf().get("li.first_date", "1992-01-11"));
    firstDate.setDate(1);
    Date lastDate = Date.valueOf(getConf().get("li.last_date", "1998-12-21"));
    lastDate.setDate(1);

    Calendar c = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
    c.setTime(firstDate);

    ArrayList<byte[]> splitKeys = new ArrayList<byte[]>();
    System.out.println("Calculating split keys.");
    int i = 1;
    while (c.getTime().compareTo(lastDate) < 0) {
      byte[] bytes = Bytes.toBytes((int)(c.getTimeInMillis() / MILLIS_IN_DAY));
      splitKeys.add(bytes);
      System.out.println(i++ + ". " + Bytes.toStringBinary(bytes));
      c.add(Calendar.MONTH, 1);
    }

    System.out.println("Creating table with " + (splitKeys.size()+1) + " regions.");
    return splitKeys.toArray(new byte[splitKeys.size()][]);
  }

  public static void main(String[] args) {
    new Lineitem().getSplitKeys();
  }

}
