package com.netease.spark.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author jiacx
 * @date 2018-05-14
 */
public class HBaseConnPool {
	private final static Logger logger = LoggerFactory.getLogger(HBaseConnPool.class);
	static Connection conn;
	static Table table = null;
	public static Connection getConn() throws IOException {
		Configuration conf = HBaseConfiguration.create();

		synchronized (Connection.class) {
			if (conn == null) {
				logger.debug("HBase 新建连接******************************************");
				/**
				 * 使用ConnectionFactory，在获取getTable的时候会调用默认的连接池，默认配置最大连接数256
				 **/
				try {
					conn = ConnectionFactory.createConnection(conf);
				} catch (Exception e) {
					e.printStackTrace();
					if (conn != null) {
						conn.close();
					}
				}
				logger.debug("HBase 连接成功******************************************");
			}
			return conn;
		}
	}

	public static Table getTable(Connection conn,String tableName) throws IOException {
		synchronized (Connection.class) {
			if (table == null) {
				table = conn.getTable(TableName.valueOf(tableName));
			}
			return table;
		}
	}

	public static void closeConn() {
		if (table != null) {
			try {
				table.close();
			} catch (IOException e) {
				logger.error("关闭HBase connection 出错", e);
			}
		}
		if (conn != null) {
			try {
				conn.close();
			} catch (IOException e) {
				logger.error("关闭HBase connection 出错", e);
			}
		}
	}
}