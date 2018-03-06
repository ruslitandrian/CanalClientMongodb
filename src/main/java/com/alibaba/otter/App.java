package com.alibaba.otter;
import java.net.InetSocketAddress;
import java.util.List;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.common.utils.AddressUtils;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.alibaba.otter.canal.client.*;


public class App
{
    public static void main(String args[]) {


        // 创建链接
        // String host = "172.16.234.117"; //AddressUtils.getHostIp()
        String host = AddressUtils.getHostIp();
        System.out.println(host);
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress(host,
                11111), "example", "", "");

        int batchSize = 5 * 1024;

        try {
            connector.connect();
            connector.subscribe(".*\\..*");
            // connector.rollback();
            while (true) {
                Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据
                long batchId = message.getId();
                int size = message.getEntries().size();
                if (batchId == -1 || size == 0) {
                   // try {
                   //     Thread.sleep(1000);
                  //  } catch (InterruptedException e) {
                  //      e.printStackTrace();
                 //   }
                } else {
                    printEntry(message.getEntries());
                }

                connector.ack(batchId); // 提交确认
                // connector.rollback(batchId); // 处理失败, 回滚数据
            }
        } catch (Exception e) {
            System.out.println(e);
        } finally {
            connector.disconnect();
        }
    }

    private static void printEntry( List<Entry> entrys) {
        for (Entry entry : entrys) {
            if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN || entry.getEntryType() == EntryType.TRANSACTIONEND) {
                continue;
            }

            RowChange rowChage = null;
            try {
                rowChage = RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(),
                        e);
            }

            EventType eventType = rowChage.getEventType();
            System.out.println(String.format("================> binlog[%s:%s] , name[%s,%s] , eventType : %s",
                    entry.getHeader().getLogfileName(), entry.getHeader().getLogfileOffset(),
                    entry.getHeader().getSchemaName(), entry.getHeader().getTableName(),
                    eventType));

            // CREATE TABLE
            if (eventType == EventType.QUERY || rowChage.getIsDdl()) {
                System.out.println(" sql ----> " + rowChage.getSql());
                if( eventType == EventType.ERASE) {
                    //MongoDbUtil.QuerySQL("DROP TABLE `" + entry.getHeader().getSchemaName() + "`.`" + entry.getHeader().getTableName() + "`");
                    MongoDbUtil.DropTable(entry.getHeader().getSchemaName(), entry.getHeader().getTableName());
                }else if( eventType == EventType.RENAME){
                    // Mongodb 不支持
                    // MongoDbUtil.AlterTable(entry.getHeader().getSchemaName(), entry.getHeader().getTableName());
                }else{
                    MongoDbUtil.CreateTable(entry.getHeader().getSchemaName(), entry.getHeader().getTableName());
                }

                continue;
            }

            for (RowData rowData : rowChage.getRowDatasList()) {
                if (eventType == EventType.DELETE) {
                    MongoDbUtil.MongoDelete(entry, rowData.getBeforeColumnsList());
                } else if (eventType == EventType.INSERT) {
                    MongoDbUtil.MongoInsert(entry, rowData.getAfterColumnsList());
                } else {
                    System.out.println("-------> before");
                    printColumn(rowData.getBeforeColumnsList());
                    System.out.println("-------> after");
                    MongoDbUtil.MongoUpdate(entry, rowData.getBeforeColumnsList(), rowData.getAfterColumnsList());
                }
            }
        }
    }

    private static void printColumn( List<Column> columns) {
        for (Column column : columns) {
            System.out.println(column.getName() + " : " + column.getValue() + "    update=" + column.getUpdated());
        }
    }
}