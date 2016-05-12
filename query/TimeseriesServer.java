import java.io.*;
import java.nio.file.*;
import java.nio.charset.*;
import java.util.Map;
import java.util.logging.Logger;
import javax.json.*;
import fi.iki.elonen.*;
import fi.iki.elonen.util.*;

public class TimeseriesServer extends NanoHTTPD {

  private static final Logger LOG = Logger.getLogger(TimeseriesServer.class.getName());

  public static void main(String[] args) {
    ServerRunner.run(TimeseriesServer.class);
  }

  public TimeseriesServer() {
    super(9000);
  }

  //private JsonObject getdata() {
  private String getData() throws IOException {
    byte[] data = Files.readAllBytes(Paths.get("data.json"));
    return new String(data,StandardCharsets.UTF_8);    
    
    //return "var trace1 = {x: [1, 2, 3, 4,5],y: [10, 15, 13, 17,18],type: 'scatter'};var trace2 = {x: [1, 2, 3, 4,8], y: [16, 5, 11, 9,12], type: 'scatter'};var data = [trace1, trace2];";

    /*
    JsonObject value = Json.createObjectBuilder()
      .add("firstName", "John")
      .add("lastName", "Smith")
      .add("age", 25)
      .add("address", Json.createObjectBuilder()
           .add("streetAddress", "21 2nd Street")
           .add("city", "New York")
           .add("state", "NY")
           .add("postalCode", "10021"))
      .add("phoneNumber", Json.createArrayBuilder()
           .add(Json.createObjectBuilder()
                .add("type", "home")
                .add("number", "212 555-1234"))
           .add(Json.createObjectBuilder()
                .add("type", "fax")
                .add("number", "646 555-4567")))
      .build();
    */
  }

  private String getLayout() throws IOException {
    byte[] data = Files.readAllBytes(Paths.get("layout.json"));
    return new String(data,StandardCharsets.UTF_8);    
  }
  
  @Override
  public Response serve(IHTTPSession session) {
    try {
      Method method = session.getMethod();
      String uri = session.getUri();
      byte[] data = Files.readAllBytes(Paths.get("timeseries.html"));
      String templ = new String(data,StandardCharsets.UTF_8);
      Map<String, String> parms = session.getParms();
      String msg=templ.replace("<<layout>>",getLayout());      
      msg = msg.replace("<<data>>",getData());
      return newFixedLengthResponse(msg);
    } catch(Exception e) {
      System.out.println(e);
    }
    return null;
  }
}
