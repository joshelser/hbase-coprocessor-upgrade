package com.github.joshelser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class CoprocessorTest {

  private static HBaseTestingUtility TEST_UTIL;
  private static final byte[] DATA_FAMILY = Bytes.toBytes("data");
 
  @BeforeClass
  public static void before() throws Exception {
    TEST_UTIL = new HBaseTestingUtility();
    TEST_UTIL.getConfiguration().set("hbase.coprocessor.region.classes", UpdatingCoprocessor.class.getName());
    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void after() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void test() throws IOException {
    final long testStartMillis = System.currentTimeMillis();
    TableName tn = TableName.valueOf("coprocessorTest");
    Connection conn = TEST_UTIL.getConnection();
    HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
    if (admin.tableExists(tn)) {
      admin.disableTable(tn);
      admin.deleteTable(tn);
    }

    HTableDescriptor htd = new HTableDescriptor(tn);
    htd.addFamily(new HColumnDescriptor(UpdatingCoprocessor.METADATA_FAMILY));
    htd.addFamily(new HColumnDescriptor(DATA_FAMILY));
    admin.createTable(htd);

    Table t = conn.getTable(tn);
    Put p = new Put(Bytes.toBytes("row1"));
    p.addColumn(DATA_FAMILY, Bytes.toBytes("first_name"), Bytes.toBytes("Josh"));
    p.addColumn(DATA_FAMILY, Bytes.toBytes("last_name"), Bytes.toBytes("Elser"));
    p.addColumn(DATA_FAMILY, Bytes.toBytes("country"), Bytes.toBytes("USA"));
    t.put(p);

    Result result = t.get(new Get(p.getRow()));
    assertEquals(p.size() + 3, result.size());
    for (Cell cell : result.listCells()) {
      System.out.println(CellUtil.toString(cell, true));
    }
    System.out.flush();
    Cell lastUpdateCell = result.getColumnLatestCell(UpdatingCoprocessor.METADATA_FAMILY,
        UpdatingCoprocessor.LAST_UPDATE_MILLIS_COLUMN);
    assertNotNull(lastUpdateCell);
    assertTrue(testStartMillis < Bytes.toLong(CellUtil.cloneValue(lastUpdateCell)));
    Cell respondingRegionServerCell = result.getColumnLatestCell(UpdatingCoprocessor.METADATA_FAMILY,
        UpdatingCoprocessor.RESPONDING_SERVER_COLUMN);
    assertNotNull(respondingRegionServerCell);
    // Should not throw an error.
    ServerName.parseServerName(Bytes.toString(CellUtil.cloneValue(respondingRegionServerCell)));
    // Validate the expensive value we computed
    Cell expensiveResultCell = result.getColumnLatestCell(UpdatingCoprocessor.METADATA_FAMILY,
        UpdatingCoprocessor.EXPENSIVE_RESULT_COLUMN);
    assertNotNull(expensiveResultCell);
    assertEquals(Bytes.toString(CellUtil.cloneValue(expensiveResultCell)), "$1,000,000.00");
  }

}
