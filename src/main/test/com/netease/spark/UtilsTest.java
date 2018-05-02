package com.netease.spark;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * @author : lishuming
 */
public class UtilsTest {
  @Test
  public void testKafakUtils() {
    Set<String> topicsSet = new HashSet<>(Arrays.asList("a, b".split(",")));
    Collection<String> collection  = Arrays.asList(topicsSet.toArray(new String[0]));
    for (String a: collection) {
      System.out.println(a);
    }
  }
}
