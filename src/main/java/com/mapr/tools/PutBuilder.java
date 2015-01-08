package com.mapr.tools;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;

import com.mapr.tools.impl.TsvParser.ParsedLine;

public interface PutBuilder {

  public Put build(ParsedLine parsed) throws IOException;

}
