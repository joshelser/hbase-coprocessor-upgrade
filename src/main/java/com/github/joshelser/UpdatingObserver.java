package com.github.joshelser;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.Cell.Type;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Observer which is attached to get Region in a table.
 */
public class UpdatingObserver implements RegionCoprocessor, RegionObserver {
  private static final Logger LOG = LoggerFactory.getLogger(UpdatingObserver.class);

  public static final byte[] METADATA_FAMILY = Bytes.toBytes("metadata");
  public static final byte[] LAST_UPDATE_MILLIS_COLUMN = Bytes.toBytes("last_update");
  public static final byte[] RESPONDING_SERVER_COLUMN = Bytes.toBytes("regionserver");
  public static final byte[] EXPENSIVE_RESULT_COLUMN = Bytes.toBytes("expensive_result");

  private byte[] regionServerName = null;
  private static final AtomicReference<byte[]> EXPENSIVE_RESULT = new AtomicReference<>(null);
  private static final AtomicBoolean COMPUTED_ONCE = new AtomicBoolean(false);

  boolean isHBaseSystemTable(ObserverContext<RegionCoprocessorEnvironment> c) {
    return Bytes.equals(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME,
        c.getEnvironment().getRegion().getTableDescriptor().getTableName().getNamespace());
  }

  @Override
  public Optional<RegionObserver> getRegionObserver() {
    return Optional.of(this);
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void start(CoprocessorEnvironment e) {
    RegionCoprocessorEnvironment rsEnv = (RegionCoprocessorEnvironment) e;
    regionServerName = Bytes.toBytes(rsEnv.getServerName().toString());
    synchronized (UpdatingObserver.class) { 
      // Compute this once per RegionServer. We have the lock and if this is null,
      // then we need to compute it.
      if (EXPENSIVE_RESULT.get() == null) {
        LOG.info("Computing expensive operation in UpdatingObserver");
        EXPENSIVE_RESULT.set(Bytes.toBytes(computeExpensiveValue()));
      }
    }
    LOG.info("UpdatingObserver started.");
  }

  String computeExpensiveValue() {
    if (!COMPUTED_ONCE.compareAndSet(false, true)) {
      throw new RuntimeException("This value should only be computed once per RegionServer!");
    }
    // This is a very expensive function we only want to do once.
    return "$1,000,000.00";
  }

  @Override
  public void prePut(ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit,
      Durability durability) throws IOException {
    if (isHBaseSystemTable(c)) {
      return;
    }
    LOG.info("Adding metadata:last_update to row");
    put.addColumn(METADATA_FAMILY, LAST_UPDATE_MILLIS_COLUMN, HConstants.LATEST_TIMESTAMP,
        Bytes.toBytes(System.currentTimeMillis()));
  }

  @Override
  public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c,
      MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
    if (isHBaseSystemTable(c)) {
      return;
    }
    for (int i = 0; i < miniBatchOp.size(); i++) {
      Mutation mutation = miniBatchOp.getOperation(i);
      if (mutation instanceof Put) {
        prePut(c, (Put) mutation, null, null);
      }
    }
  }

  @Override
  public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> c, Get get, List<Cell> result) {
    if (isHBaseSystemTable(c)) {
      return;
    }
    LOG.info("Ensure metadata family is fetched in preGetOp");
    get.addFamily(METADATA_FAMILY);
  }

  @Override
  public void postGetOp(ObserverContext<RegionCoprocessorEnvironment> c, Get get, List<Cell> result) {
    if (isHBaseSystemTable(c)) {
      return;
    }
    LOG.info("Updating result with metadata:regionserver in postGetOp");
    Cell cell = CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
        .setRow(get.getRow())
        .setFamily(METADATA_FAMILY)
        .setQualifier(RESPONDING_SERVER_COLUMN)
        .setTimestamp(System.currentTimeMillis() / 1000)
        .setType(Type.Put)
        .setValue(regionServerName)
        .build();
    result.add(cell);
    cell = CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
        .setRow(get.getRow())
        .setFamily(METADATA_FAMILY)
        .setQualifier(EXPENSIVE_RESULT_COLUMN)
        .setTimestamp(System.currentTimeMillis() / 1000)
        .setType(Type.Put)
        .setValue(EXPENSIVE_RESULT.get())
        .build();
    result.add(cell);
    // The list of cells sent back to the client must be sorted.
    result.sort(CellComparator.getInstance());
  }
}
