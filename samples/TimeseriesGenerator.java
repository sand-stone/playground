import java.io.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;

import org.kududb.*;
import org.kududb.client.*;

public class TimeseriesGenerator {
  private static final String KUDU_MASTER = System.getProperty("kuduMaster","localhost");

  static String tableName = "XTimeSeries";

  private static void createTable(int num) throws Exception
  {
    KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTER).build();
    List<ColumnSchema> columns = new ArrayList();
    columns.add(new ColumnSchema.ColumnSchemaBuilder("host",Type.STRING).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("measure",Type.STRING).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("time",Type.TIMESTAMP).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("mm5",Type.INT8).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("min",Type.DOUBLE).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("max",Type.DOUBLE).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("sum",Type.DOUBLE).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("count",Type.INT64).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("mm",Type.INT8).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("hh",Type.INT8).build());
    Schema schema = new Schema(columns);
    CreateTableOptions opt= new CreateTableOptions();
    List<String> hashcols=new ArrayList<String>();
    List<String> rangecols=new ArrayList<String>();
    hashcols.add("host");hashcols.add("measure");
    rangecols.add("mm5");
    opt.addHashPartitions(hashcols,num);opt.setRangePartitionColumns(rangecols);
    for (int i=0;i<=12;i++) {
      PartialRow split=schema.newPartialRow();
      split.addByte(3,(byte)i);
      opt.addSplitRow(split);
    }
    client.createTable(tableName,schema,opt);
    client.shutdown();
  }

  static int tscount=20000000;static int mcount=3000;static int interval=60*60*1000;
  public static class InsertTask implements Runnable
  {
    @Override
    public void run()
    {
      try {
        KuduClient client=new KuduClient.KuduClientBuilder(KUDU_MASTER).build();
        KuduTable table=client.openTable(tableName);
        KuduSession session=client.newSession();
        session.setFlushMode(AsyncKuduSession.FlushMode.AUTO_FLUSH_SYNC);
        //session.setFlushMode(AsyncKuduSession.FlushMode.AUTO_FLUSH_BACKGROUND);
        session.setFlushInterval(100);
        //session.setFlushInterval(10000);
        //session.setMutationBufferSpace(10000);
        //session.flush().join(50000);
        long count=tscount;
        Instant now=Instant.now();
        Random rand = new Random();long rcount=0;
        while(count-->0) {
          int batch=mcount;
          while(batch-->0) {
            Insert insert = table.newInsert();
            PartialRow row = insert.getRow();
            row.addString(0,"host#"+rand.nextInt(tscount));
            row.addString(1,"measure#"+rand.nextInt(mcount));
            Instant rts=now.plusMillis(rand.nextInt(interval));
            LocalDateTime ldt = LocalDateTime.ofInstant(rts,ZoneId.systemDefault());
            int mm=ldt.getMinute();
            row.addLong(2,rts.toEpochMilli());
            row.addByte(3,(byte)(mm/5));
            row.addDouble(4,rand.nextDouble());
            row.addDouble(5,rand.nextDouble()+1);
            row.addDouble(6,rand.nextDouble()*10.0);
            row.addLong(7,rand.nextInt(100));
            row.addByte(8,(byte)mm);
            row.addByte(9,(byte)ldt.getHour());
            rcount++;
            session.apply(insert);
          }
          session.flush();
          //System.out.println("count="+count);
          if(rcount%100000==0) System.out.println("row count="+rcount);
        }
        client.shutdown();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  static LinkedBlockingQueue<String> q=new LinkedBlockingQueue<String>();
  public static void main(String[] args) {
    System.out.println("-----------------------------------------------");
    System.out.println("Will try to connect to Kudu master at " + KUDU_MASTER);
    System.out.println("Run with -DkuduMaster=myHost:port to override.");
    System.out.println("-----------------------------------------------");
    try {
      int nums=1;
      if(args.length>0) {
        nums=Integer.parseInt(args[0]);
      }
      createTable(nums);
      //KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTER).build();
      //client.deleteTable(tableName);
      ThreadPoolExecutor executor=(ThreadPoolExecutor)Executors.newFixedThreadPool(64);
      for (int i=0; i<64;i++) {
        InsertTask task = new InsertTask();
        executor.execute(task);
      }
      executor.awaitTermination(-1,TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
