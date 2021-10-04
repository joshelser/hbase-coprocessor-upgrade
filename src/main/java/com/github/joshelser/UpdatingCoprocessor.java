package com.github.joshelser;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdatingCoprocessor extends BaseRegionObserver {
  private static final Logger LOG = LoggerFactory.getLogger(UpdatingCoprocessor.class);

  public static final byte[] METADATA_FAMILY = Bytes.toBytes("metadata");
  public static final byte[] LAST_UPDATE_MILLIS_COLUMN = Bytes.toBytes("last_update");
  public static final byte[] RESPONDING_SERVER_COLUMN = Bytes.toBytes("regionserver");

  private byte[] regionServerName = null;

  @Override
  public void start(CoprocessorEnvironment e) throws IOException {
    RegionCoprocessorEnvironment regionEnv = (RegionCoprocessorEnvironment) e;
    regionServerName = Bytes.toBytes(regionEnv.getRegionServerServices().getServerName().toString());
  }

  @Override
  public void prePut(ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit,
      Durability durability) throws IOException {
    LOG.info("Adding metadata:last_update to row");
    put.addColumn(METADATA_FAMILY, LAST_UPDATE_MILLIS_COLUMN, Bytes.toBytes(System.currentTimeMillis()));
  }

  @Override
  public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> e,
      MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
    for (int i = 0; i < miniBatchOp.size(); i++) {
      Mutation mutation = miniBatchOp.getOperation(i);
      if (mutation instanceof Put) {
        prePut(e, (Put) mutation, null, null);
      }
    }
  }

  @Override
  public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> c, Get get, List<Cell> result) {
    LOG.info("Ensure metadata family is fetched in preGetOp");
    get.addFamily(METADATA_FAMILY);
  }

  @Override
  public void postGetOp(ObserverContext<RegionCoprocessorEnvironment> c, Get get, List<Cell> result) {
    LOG.info("Updating result with metadata:regionserver in postGetOp");
    result.add(CellUtil.createCell(Bytes.copy(get.getRow()), METADATA_FAMILY,
        RESPONDING_SERVER_COLUMN, System.currentTimeMillis(), KeyValue.Type.Put.getCode(),
        regionServerName));
  }
}
