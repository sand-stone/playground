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
    columns.add(new ColumnSchema.ColumnSchemaBuilder("mm5",Type.INT8).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("min",Type.DOUBLE).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("max",Type.DOUBLE).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("sum",Type.DOUBLE).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("count",Type.INT64).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("mm",Type.INT8).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("hh",Type.INT8).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("time",Type.TIMESTAMP).build());
    Schema schema = new Schema(columns);
    CreateTableOptions opt= new CreateTableOptions();
    List<String> hashcols=new ArrayList<String>();
    List<String> rangecols=new ArrayList<String>();
    hashcols.add("host");hashcols.add("measure");
    rangecols.add("mm5");
    opt.addHashPartitions(hashcols,num);opt.setRangePartitionColumns(rangecols);
    client.createTable(tableName,schema,opt);
    client.shutdown();
  }

  static int tscount=5000000;static int mcount=3000;static int interval=60*60*1000;
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
        long count=10;
        List<String> timeseries = new ArrayList<String>();
        String prefix=UUID.randomUUID().toString();
        for(int i=0;i<500;i++) {
          timeseries.add(prefix+i);
        }
        Instant now=Instant.now();
        Random rand = new Random();
        while(count-->0) {
          int batch=2;
          while(batch-->0) {
            Insert insert = table.newInsert();
            PartialRow row = insert.getRow();
            row.addString(0,"host#"+rand.nextInt(tscount));
            row.addString(1,"measure#"+rand.nextInt(mcount));
            Instant rts=now.plusMillis(rand.nextInt(interval)+rand.nextInt(interval));
            LocalDateTime ldt = LocalDateTime.ofInstant(rts,ZoneId.systemDefault());
            int mm=ldt.getMinute();
            row.addByte(2,(byte)(mm/5));
            row.addDouble(3,rand.nextDouble());
            row.addDouble(4,rand.nextDouble()+1);
            row.addDouble(5,rand.nextDouble()*10.0);
            row.addLong(6,rand.nextInt(100));
            row.addByte(7,(byte)mm);
            row.addByte(8,(byte)ldt.getHour());
            row.addLong(9,rts.toEpochMilli());
            session.apply(insert);
          }
          session.flush();
          System.out.println("count="+count);
          //if(count%1000==0) System.out.println("count="+count);
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
      for (int i=0; i<1;i++) {
        InsertTask task = new InsertTask();
        executor.execute(task);
      }
      while(true)
        Thread.currentThread().sleep(1);
      //executor.shutdown();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
