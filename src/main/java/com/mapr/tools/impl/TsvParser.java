package com.mapr.tools.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;

public class TsvParser {
  private int maxColumnCount;
  private final byte separatorByte;

  public TsvParser(String separatorStr) {
    // Configure separator
    byte[] separator = Bytes.toBytes(separatorStr);
    Preconditions.checkArgument(separator.length == 1, "TsvParser only supports single-byte separators");
    separatorByte = separator[0];
  }

  public ParsedLine parse(byte[] lineBytes, int length) throws IOException {
    // Enumerate separator offsets
    ArrayList<Integer> tabOffsets = new ArrayList<Integer>(maxColumnCount);
    for (int i = 0; i < length; i++) {
      if (lineBytes[i] == separatorByte) {
        tabOffsets.add(i);
      }
    }
    if (tabOffsets.isEmpty()) {
      throw new IOException("No delimiter");
    }

    tabOffsets.add(length);
    return new ParsedLine(tabOffsets, lineBytes);
  }

  public static class ParsedLine {
    private final ArrayList<Integer> tabOffsets;
    private byte[] lineBytes;

    ParsedLine(ArrayList<Integer> tabOffsets, byte[] lineBytes) {
      this.tabOffsets = tabOffsets;
      this.lineBytes = lineBytes;
    }

    public String getColumn(int idx) {
      return new String(lineBytes, getColumnOffset(idx), getColumnLength(idx), Charsets.UTF_8);
    }

    public byte[] getColumnBytes(int idx) {
      return Arrays.copyOfRange(lineBytes, getColumnOffset(idx), getColumnOffset(idx+1)-1);
    }
    
    public int getColumnOffset(int idx) {
      return (idx > 0) ? tabOffsets.get(idx - 1) + 1 : 0;
    }

    public int getColumnLength(int idx) {
      return tabOffsets.get(idx) - getColumnOffset(idx);
    }

    public int getColumnCount() {
      return tabOffsets.size();
    }

    public byte[] getLineBytes() {
      return lineBytes;
    }

  }
}
