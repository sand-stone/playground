import java.io.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import org.kududb.*;
import org.kududb.client.*;

public class TimeseriesGenerator {
  private static final String KUDU_MASTER = System.getProperty("kuduMaster","localhost");

  static String tableName = "XTimeSeries";

  private static void createTable(int num) throws Exception
  {
    KuduClient client=null;
    try {
      client = new KuduClient.KuduClientBuilder(KUDU_MASTER).build();
      List<ColumnSchema> columns = new ArrayList();
      columns.add(new ColumnSchema.ColumnSchemaBuilder("host",Type.STRING).key(true)
                  .encoding(ColumnSchema.Encoding.DICT_ENCODING).build());
      columns.add(new ColumnSchema.ColumnSchemaBuilder("measure",Type.STRING).key(true)
                  .encoding(ColumnSchema.Encoding.DICT_ENCODING).build());
      columns.add(new ColumnSchema.ColumnSchemaBuilder("time",Type.TIMESTAMP).key(true)
                  .encoding(ColumnSchema.Encoding.BIT_SHUFFLE).build());
      columns.add(new ColumnSchema.ColumnSchemaBuilder("mm5",Type.INT8).key(true)
                  .encoding(ColumnSchema.Encoding.DICT_ENCODING).build());
      columns.add(new ColumnSchema.ColumnSchemaBuilder("min",Type.DOUBLE)
                  .encoding(ColumnSchema.Encoding.BIT_SHUFFLE).build());
      columns.add(new ColumnSchema.ColumnSchemaBuilder("max",Type.DOUBLE)
                  .encoding(ColumnSchema.Encoding.BIT_SHUFFLE).build());
      columns.add(new ColumnSchema.ColumnSchemaBuilder("sum",Type.DOUBLE)
                  .encoding(ColumnSchema.Encoding.BIT_SHUFFLE).build());
      columns.add(new ColumnSchema.ColumnSchemaBuilder("count",Type.INT64)
                  .encoding(ColumnSchema.Encoding.BIT_SHUFFLE).build());
      columns.add(new ColumnSchema.ColumnSchemaBuilder("mm",Type.INT8)
                  .encoding(ColumnSchema.Encoding.DICT_ENCODING).build());
      columns.add(new ColumnSchema.ColumnSchemaBuilder("hh",Type.INT8)
                  .encoding(ColumnSchema.Encoding.DICT_ENCODING).build());
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
    } catch(MasterErrorException e) {
      System.out.println(e.getMessage());
    } finally {
      if(client!=null) client.shutdown();
    }
  }

  static int tscount=100000;static int mcount=10000;static int interval=60*60*1000;
  public static class InsertTask implements Runnable
  {
    int id;

    public InsertTask(int id) {
      this.id=id;
    }

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
        //session.setFlushInterval(100);
        session.setFlushInterval(1000);
        session.setMutationBufferSpace(1000000);
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
          rows.addAndGet(mcount);
          //System.out.printf("insert %d rows in %f\n",mcount,(t2-t1)/1e9);
          //if(rcount%100000==0) System.out.println("row count="+rcount);
        }
        client.shutdown();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
  private static AtomicLong rows= new AtomicLong(0);
  public static void main(String[] args) {
    System.out.println("-----------------------------------------------");
    System.out.println("Will try to connect to Kudu master at " + KUDU_MASTER);
    System.out.println("Run with -DkuduMaster=myHost:port to override.");
    System.out.println("-----------------------------------------------");
    try {
      int nums=1;int c=1;
      if(args.length>0) {
        nums=Integer.parseInt(args[0]);
      }
      if(args.length>1) {
        c=Integer.parseInt(args[1]);
      }
      createTable(nums);
      //KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTER).build();
      //client.deleteTable(tableName);
      ThreadPoolExecutor executor=(ThreadPoolExecutor)Executors.newFixedThreadPool(64);
      long t1=System.nanoTime();
      for (int i=0; i<c;i++) {
        InsertTask task = new InsertTask(i);
        executor.execute(task);
      }
      boolean s=false;
      do {
        s=executor.awaitTermination(10000,TimeUnit.MILLISECONDS);
        long r=rows.get();double e=(System.nanoTime()-t1)/1e9;
        System.out.printf("totla rows %d elapsed time %f insert rate %f per second \n",r,e,r/e);
      } while(!s);
      System.out.printf("elapsed %f total rows %d \n",(System.nanoTime()-t1)/1e9,rows.get());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
