package com.netease.spark;

import com.netease.spark.utils.HdfsUtils;
import org.junit.Test;

/**
 * @author : lishuming
 */
public class HdfsUtilsTest {
  @Test
  public void testGetConn() {
    try {
      HdfsUtils.getHdfsConnection("/tmp");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
