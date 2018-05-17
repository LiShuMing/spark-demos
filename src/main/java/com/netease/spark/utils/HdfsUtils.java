package com.netease.spark.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;


public class HdfsUtils {
  private final static Logger LOGGER = LoggerFactory.getLogger(HdfsUtils.class);
  private static Configuration conf = null;
  private static FileSystem fileSystem = null;

  // format is "yyyy-mm-dd"
  private static String currentDay = null;
  // the directory of hdfs path
  private static String currentPath = null;
  private static ThreadLocal<FSDataOutputStreamHour> writeHandler = null;

  static class FSDataOutputStreamHour {
    FSDataOutputStream fsDataOutputStream;
    String hour;

    public FSDataOutputStreamHour(FSDataOutputStream fout, String hour) {
      this.setFsDataOutputStream(fout);
      this.setHour(hour);
    }

    public FSDataOutputStream getFsDataOutputStream() {
      return fsDataOutputStream;
    }

    public void setFsDataOutputStream(FSDataOutputStream fsDataOutputStream) {
      this.fsDataOutputStream = fsDataOutputStream;
    }

    public String getHour() {
      return hour;
    }

    public void setHour(String hour) {
      this.hour = hour;
    }
  }


  static {
    conf = new Configuration();
    try {
      fileSystem = FileSystem.get(conf);
    } catch (Exception e) {
      LOGGER.warn("init hdfs system failed: ", e);
    }

    writeHandler = new ThreadLocal<FSDataOutputStreamHour>() {
      @Override
      public FSDataOutputStreamHour initialValue() {
        return null;
      }
    };
  }

  // 删除 checkpoint(这是由于 checkpoint 的不稳定特性导致)
  public void removeCheckpoint(String checkpoint) throws Exception {
    fileSystem.delete(new Path(checkpoint), true);
  }

  // 获取 hdfs 的连接
  public static FSDataOutputStream getHdfsConnection(String currentPath) throws Exception {
    synchronized (HdfsUtils.class) {
      // 获取当前时间
      Date now = new Date();

      // 如果当前时间不一致, 则会重新构建 Path
      SimpleDateFormat format1 = new SimpleDateFormat("yyyy-MM-dd");
      SimpleDateFormat format2 = new SimpleDateFormat("yyyy-MM-dd-HH");
      String nowDay = format1.format(now);
      String nowHour = format2.format(now);

      // 如果 "天" 已经过时, 那么会创建一个目录
      if (currentDay == null || currentDay != nowDay) {
        currentDay = nowDay;

        // 创建新的目录
        Path path = new Path(currentPath + File.separator + currentDay);
        LOGGER.info("create dir: " + path);

        if (!fileSystem.exists(path)) {
          fileSystem.mkdirs(path);
        }
      }

      // 获取句柄, 以及当前存储的时间
      if (writeHandler.get() == null) {
        Path newPath = new Path(currentPath + File.separator + currentDay + File.separator + UUID.randomUUID() + nowHour);

        LOGGER.info("create file: " + newPath.getName());

        FSDataOutputStream fout = fileSystem.create(newPath);

        writeHandler.set(new FSDataOutputStreamHour(fout, nowHour));
      } else {
        FSDataOutputStream handler = writeHandler.get().getFsDataOutputStream();
        String hour = writeHandler.get().getHour();

        // 如果 "小时" 已经过时, 也创建一个文件
        if (hour == null || hour != nowHour) {
          if (handler != null) {
            handler.close();
          }

          Path newPath = new Path(currentPath + File.separator + currentDay + File.separator + UUID.randomUUID() + nowHour);

          LOGGER.info("create file: " + newPath.getName());

          FSDataOutputStream fout = fileSystem.create(newPath);

          writeHandler.set(new FSDataOutputStreamHour(fout, nowHour));
        }
      }

      // 返回最新的句柄
      return writeHandler.get().getFsDataOutputStream();
    }
  }
}
