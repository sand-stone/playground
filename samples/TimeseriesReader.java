import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import org.joda.time.*;
import org.kududb.*;
import org.kududb.client.*;

public class TimeseriesReader {
  private static final String KUDU_MASTER = System.getProperty("kuduMaster","localhost");

  static String tableName = "XTimeSeries";

  public static class ReadTask implements Runnable
  {
    @Override
    public void run()
    {
      try {
        KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTER).build();
        KuduTable table=client.openTable(tableName);
        List<String> projectColumns = new ArrayList<>(1);
        projectColumns.add("tsId");projectColumns.add("time");projectColumns.add("measure");
        KuduScanner scanner = client.newScannerBuilder(table)
          .setProjectedColumnNames(projectColumns)
          .build();
        long rows=0;
        while (scanner.hasMoreRows()) {
          RowResultIterator results = scanner.nextRows();
          while (results.hasNext()) {
            RowResult result = results.next();
            rows++;
            System.out.println(result.getString(0));
            System.out.println(result.getLong(1));
            System.out.println(result.getString(2));
          }
        }
        client.shutdown();
        System.out.println("rows:"+rows);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  public static class PointReadTask implements Runnable
  {
    @Override
    public void run()
    {
      try {
        KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTER).build();
        KuduTable table=client.openTable(tableName);
        List<String> projectColumns = new ArrayList<>(1);
        projectColumns.add("tsId");projectColumns.add("time");projectColumns.add("measure");
        KuduScanner scanner = client.newScannerBuilder(table)
          .setProjectedColumnNames(projectColumns)
          .addPredicate(KuduPredicate.newComparisonPredicate(new ColumnSchema.ColumnSchemaBuilder("time", Type.TIMESTAMP).build(), KuduPredicate.ComparisonOp.EQUAL,1462502386629L))
          .build();
        long rows=0;
        while (scanner.hasMoreRows()) {
          RowResultIterator results = scanner.nextRows();
          while (results.hasNext()) {
            RowResult result = results.next();
            rows++;
            System.out.println(result.getString(0));
            System.out.println(result.getLong(1));
            System.out.println(result.getString(2));
          }
        }
        client.shutdown();
        System.out.println("rows:"+rows);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  public static class RangeReadTask implements Runnable
  {
    @Override
    public void run()
    {
      try {
        KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTER).build();
        KuduTable table=client.openTable(tableName);
        List<String> projectColumns = new ArrayList<>(1);
        projectColumns.add("tsId");projectColumns.add("time");projectColumns.add("measure");
        ColumnRangePredicate pred=new ColumnRangePredicate(new ColumnSchema.ColumnSchemaBuilder("time", Type.TIMESTAMP).build());
        pred.setLowerBound(1462502342877L);
        pred.setUpperBound(1562502342877L);
        KuduScanner scanner = client.newScannerBuilder(table)
          .setProjectedColumnNames(projectColumns)
          .addColumnRangePredicate(pred)
          .build();
        long rows=0;
        while (scanner.hasMoreRows()) {
          RowResultIterator results = scanner.nextRows();
          while (results.hasNext()) {
            RowResult result = results.next();
            rows++;
            System.out.println(result.getString(0));
            System.out.println(result.getLong(1));
            System.out.println(result.getString(2));
          }
        }
        client.shutdown();
        System.out.println("rows:"+rows);
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
      if(args.length==1)
        nums=Integer.parseInt(args[0]);

      ThreadPoolExecutor executor=(ThreadPoolExecutor)Executors.newFixedThreadPool(nums);
      for (int i=0;i<nums;i++) {
        //ReadTask task = new ReadTask();
        Runnable task = new RangeReadTask();//PointReadTask();
        executor.execute(task);
      }
      executor.shutdown();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
