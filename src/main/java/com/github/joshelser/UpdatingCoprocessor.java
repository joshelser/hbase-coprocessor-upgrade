package com.github.joshelser;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Observer which is attached to the RegionServer, used to compute values once.
 */
public class UpdatingCoprocessor implements RegionServerCoprocessor {

  private static byte[] regionServerName = null;
  private static final AtomicReference<byte[]> EXPENSIVE_RESULT = new AtomicReference<>(null);
  private static final AtomicBoolean COMPUTED_ONCE = new AtomicBoolean(false);

  @SuppressWarnings("rawtypes")
  @Override
  public void start(CoprocessorEnvironment e) throws IOException {
    RegionServerCoprocessorEnvironment rsEnv = (RegionServerCoprocessorEnvironment) e;
    regionServerName = Bytes.toBytes(rsEnv.getServerName().toString());
    // Compute this once per RegionServer
    EXPENSIVE_RESULT.set(Bytes.toBytes(computeExpensiveValue()));
  }

  String computeExpensiveValue() {
    if (!COMPUTED_ONCE.compareAndSet(false, true)) {
      throw new RuntimeException("This value should only be computed once per RegionServer!");
    }
    // This is a very expensive function we only want to do once.
    return "$1,000,000.00";
  }

  public static byte[] getServerName() {
    if (regionServerName == null) {
      throw new IllegalStateException("RegionServer name was null");
    }
    return regionServerName;
  }

  public static byte[] getExpensiveValue() {
    byte[] expensiveResult = EXPENSIVE_RESULT.get();
    if (expensiveResult == null) {
      throw new IllegalStateException("Expensive result was not yet computed");
    }
    return expensiveResult;
  }
}
