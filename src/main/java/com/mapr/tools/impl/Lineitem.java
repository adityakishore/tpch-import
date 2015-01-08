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
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.mapr.tools.Constants;
import com.mapr.tools.Helper;
import com.mapr.tools.PutBuilder;
import com.mapr.tools.TableCreator;
import com.mapr.tools.ViewBuilder;
import com.mapr.tools.impl.TsvParser.ParsedLine;

public final class Lineitem extends Configured implements TableCreator, PutBuilder, ViewBuilder, Constants {

  private static final String USE_FULL_COLUMNS = "lineitem.full_columns";
  private static final byte[] FAMILY_1 = "F".getBytes();
  private static final byte[] FAMILY_2 = "G".getBytes();

  private static final String[] FULL_COLUMNS = {"l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus", "l_commitdate", "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment"};

  private static final String[] SHORT_COLUMNS = {"A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K"};

  private final String[] COLUMNS;

  private final byte[] L_QUANTITY;
  private final byte[] L_EXTENDEDPRICE;
  private final byte[] L_DISCOUNT;
  private final byte[] L_TAX;
  private final byte[] L_RETURNFLAG;
  private final byte[] L_LINESTATUS;
  private final byte[] L_COMMITDATE;
  private final byte[] L_RECEIPTDATE;
  private final byte[] L_SHIPINSTRUCT;
  private final byte[] L_SHIPMODE;
  private final byte[] L_COMMENT;

  public Lineitem() {
    this(HBaseConfiguration.create());
  }

  public Lineitem(Configuration conf) {
    super(conf);

    if (getConf().getBoolean(USE_FULL_COLUMNS, false)) {
      COLUMNS = FULL_COLUMNS;
    } else {
      COLUMNS = SHORT_COLUMNS;
    }
    L_QUANTITY = COLUMNS[0].getBytes();
    L_EXTENDEDPRICE = COLUMNS[1].getBytes();
    L_DISCOUNT = COLUMNS[2].getBytes();
    L_TAX = COLUMNS[3].getBytes();
    L_RETURNFLAG = COLUMNS[4].getBytes();
    L_LINESTATUS = COLUMNS[5].getBytes();
    L_COMMITDATE = COLUMNS[6].getBytes();
    L_RECEIPTDATE = COLUMNS[7].getBytes();
    L_SHIPINSTRUCT = COLUMNS[8].getBytes();
    L_SHIPMODE = COLUMNS[9].getBytes();
    L_COMMENT = COLUMNS[10].getBytes();
  }

  @Override
  public String getView(String viewName, String tableName) {
    return String.format("create view %s as \n" +
        "select\n" +
        "    convert_from(byte_substr(lineitem.row_key, 9, 4), 'int_be') as l_orderkey,\n" +
        "    convert_from(byte_substr(lineitem.row_key, 14, 4), 'int_be') as l_partkey,\n" +
        "    convert_from(byte_substr(lineitem.row_key, 18, 4), 'int_be') as l_suppkey,\n" +
        "    convert_from(byte_substr(lineitem.row_key, 13, 1), 'tinyint') as l_linenumber,\n" +
        "    convert_from(lineitem.F.%s, 'double_be') as l_quantity,\n" +
        "    convert_from(lineitem.F.%s, 'double_be') as l_extendedprice,\n" +
        "    convert_from(lineitem.F.%s, 'double_be') as l_discount,\n" +
        "    convert_from(lineitem.F.%s, 'double_be') as l_tax,\n" +
        "    convert_from(lineitem.F.%s, 'utf8') as l_returnflag,\n" +
        "    convert_from(lineitem.F.%s, 'utf8') as l_linestatus,\n" +
        "    convert_from(byte_substr(lineitem.row_key, 1, 8), 'date_epoch_be') as l_shipdate,\n" +
        "    convert_from(lineitem.F.%s, 'date_epoch_be') l_commitdate,\n" +
        "    convert_from(lineitem.F.%s, 'date_epoch_be') l_receiptdate,\n" +
        "    convert_from(lineitem.F.%s, 'utf8') as l_shipinstruct,\n" +
        "    convert_from(lineitem.F.%s, 'utf8') as l_shipmode,\n" +
        "    convert_from(lineitem.G.%s, 'utf8') as l_comment\n" +
        "from %s lineitem;",
        viewName,
        COLUMNS[0],
        COLUMNS[1],
        COLUMNS[2],
        COLUMNS[3],
        COLUMNS[4],
        COLUMNS[5],
        COLUMNS[6],
        COLUMNS[7],
        COLUMNS[8],
        COLUMNS[9],
        COLUMNS[10],
        tableName);
  }

  @Override
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
    long   l_shipdate       = (Date.valueOf(parsed.getColumn(10)).getTime());
    long   l_commitdate     = (Date.valueOf(parsed.getColumn(11)).getTime());
    long   l_receiptdate    = (Date.valueOf(parsed.getColumn(12)).getTime());
    byte[] l_shipinstruct   = parsed.getColumnBytes(13);
    byte[] l_shipmode       = parsed.getColumnBytes(14);
    byte[] l_comment        = parsed.getColumnBytes(15);

    // encode row_key
    ByteBuffer row_key = ByteBuffer
        .wrap(new byte[8+4+1+4+4]) // 8(l_shipdate) + 4(l_orderkey) + 1(l_linenumber) + 4(l_partkey) + 4(l_suppley)
        .order(ByteOrder.BIG_ENDIAN);
    row_key.putLong(l_shipdate);  // (1, 8)
    row_key.putInt(l_orderkey);   // (9, 4)
    row_key.put(l_linenumber);    // (13, 1)
    row_key.putInt(l_partkey);    // (14, 4)
    row_key.putInt(l_suppley);    // (18, 4)
    row_key.rewind();
    Put put = new Put(row_key.array());

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
      HTableDescriptor tableDesc = new HTableDescriptor(tableName);
      tableDesc.addFamily(new HColumnDescriptor(FAMILY_1));
      tableDesc.addFamily(new HColumnDescriptor(FAMILY_2));
      admin.createTable(tableDesc, getSplitKeys());
    }
  }

  private byte[][] getSplitKeys() {
    Date firstDate = Date.valueOf(getConf().get("lineitem.first_date", "1992-01-1"));
    Date lastDate = Date.valueOf(getConf().get("lineitem.last_date", "1998-12-31"));
    int inc = getConf().getInt("lineitem.inc_day", 2);

    Calendar c = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
    c.setTime(firstDate);

    ArrayList<byte[]> splitKeys = new ArrayList<byte[]>();
    System.out.println("Calculating split keys.");
    while (c.getTime().compareTo(lastDate) < 0) {
      byte[] bytes = Bytes.toBytes(c.getTimeInMillis());
      splitKeys.add(bytes);
      c.add(Calendar.MILLISECOND, inc*24*3600*1000);
    }

    System.out.println("Creating table with " + (splitKeys.size()+1) + " regions.");
    return splitKeys.toArray(new byte[splitKeys.size()][]);
  }

  public static void main(String[] args) {
    Configuration conf = HBaseConfiguration.create();
    conf.setBoolean(USE_FULL_COLUMNS, true);
    System.out.println(new Lineitem(conf).getView("x", "y"));
  }

}
