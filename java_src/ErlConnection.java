import com.ericsson.otp.erlang.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.Object.*;

public class ErlConnection {
 
    private static OtpConnection conn;
     public OtpErlangObject received;
     private final String peer;
     private final String cookie;
      
     public static void main(String []args){
         new ErlConnection("n2@Viets-Mac-mini.home","ecc-ycsb");
     }
 
      public ErlConnection(String _peer, String _cookie) {
          peer = _peer;
          cookie = _cookie;
          connect();
         
          BufferedReader in = new BufferedReader( new InputStreamReader(System.in));
          Boolean undone = true;
          while (undone) {
            // Read line and try to call parseInt on it.
            System.out.println("Enter your command: fun arg1 arg2 ---");
            try{
                 String line = in.readLine();
                 String[] arr = line.split("\\s+");

                String res = "";
                OtpErlangAtom at;
                switch (arr[0]) {                    
                  case "put":
                      res = put(arr);
                      break;
                  case "get":
                      res = get(arr);
                      break;
                  case "delete":
                      res = delete(arr);
                      break;
                  case "list_keys":
                      res = list_keys();
                      break;
                  case "done":
                      System.out.println("done");
                      undone = false;
                      break;
                  default:
                      System.out.println("Default");  
                      break;
                  }
                  System.out.println("Reply: " + res);
            }
            catch (Exception exp) {
              System.out.println("connection error is :" + exp.toString());
              exp.printStackTrace();
            }
             
          }


          disconnect();
 
      }
 
      private void connect() {
       System.out.print("Please wait, connecting to "+peer+"....\n");
 
       String javaClient ="java";
       try {
         OtpSelf self = new OtpSelf(javaClient, cookie.trim());
         OtpPeer other = new OtpPeer(peer.trim());
         conn = self.connect(other);
         System.out.println("Connection Established with "+peer+"\n");
       }
       catch (Exception exp) {
         System.out.println("connection error is :" + exp.toString());
         exp.printStackTrace();
       }
 
     }
 
     public void disconnect() {
       System.out.println("Disconnecting....");
       if(conn != null){
         conn.close();
       }
       System.out.println("Successfuly Disconnected");
     }

     private String put(String[] arr) {
          System.out.println("put command");
          try {
              conn.sendRPC("ecc_java","insert", 
                new OtpErlangObject[] {
                    new OtpErlangAtom(arr[1]), 
                    new OtpErlangAtom(arr[2])});
              OtpErlangObject response = conn.receiveRPC(); 
              OtpErlangAtom at = (OtpErlangAtom) response;
              String res = at.atomValue();
              if (!res.equals("ok")) {
                return "INSERT ERROR";
              }
              return res;
          }
          catch (Exception exp) {
               System.out.println("connection error is :" + exp.toString());
               exp.printStackTrace();
          }
          return "";
     }

     private String get(String[] arr) {
          System.out.println("get command");
          try {
              conn.sendRPC("ecc_java","lookup", new OtpErlangObject[] {new OtpErlangAtom(arr[1])});
              OtpErlangObject response = conn.receiveRPC(); 
              // OtpErlangAtom at = (OtpErlangAtom) response;
              // String res = at.atomValue();
              // System.out.println("res: "+ res);
              //OtpErlangTuple tuple = (OtpErlangTuple) response;
              //if (tuple.arity() == 2){
               //  return "NOT FOUND";
              //}
              return response.toString();
              //return tuple.toString();
          }
          catch (Exception exp) {
               System.out.println("connection error is :" + exp.toString());
               exp.printStackTrace();
          }
          return "";
     }

     private String delete(String[] arr) {
          System.out.println("delete command");
          try {
              conn.sendRPC("ecc_java","delete", new OtpErlangObject[] {new OtpErlangAtom(arr[1])});
              OtpErlangObject response = conn.receiveRPC(); 
              OtpErlangAtom at = (OtpErlangAtom) response;
              String res = at.atomValue();
              return res;
          }
          catch (Exception exp) {
               System.out.println("connection error is :" + exp.toString());
               exp.printStackTrace();
          }
          return "";
     }

     private String list_keys() {
          System.out.println("list_keys command");
          try {
              conn.sendRPC("ecc_java","list_keys", new OtpErlangObject[0]); //no arguments passed
              OtpErlangObject response = conn.receiveRPC(); 
              OtpErlangList list = (OtpErlangList) response;
              return list.toString();
          }
          catch (Exception exp) {
               System.out.println("connection error is :" + exp.toString());
               exp.printStackTrace();
          }
          return "";
     }

    private OtpErlangObject[] withArgs2(String key, String value) {
      return new OtpErlangObject[] { 
        new OtpErlangAtom(key),
        new OtpErlangAtom(value) 
      };
    }
}
