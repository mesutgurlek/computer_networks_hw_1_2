/**
 * Created by mesutgurlek on 3/15/17.
 */

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Base64;


public class CloudDownloader {
    public static String getBaseLength(String prevUp, String nextBase){
        int upper = Integer.parseInt(prevUp);
        int base = Integer.parseInt(nextBase);
        if(upper < base) return "1";
        int diff = upper - base + 1;
        return Integer.toString(diff);
    }

    public static String increaseByOne(String str){
        int num = Integer.parseInt(str);
        num += 1;
        return Integer.toString(num);
    }

    public static int difference(String str1, String str2){
        return Math.abs(Integer.parseInt(str1) - Integer.parseInt(str2));
    }

    public static void main(String args[]) throws IOException {
        if(args.length != 2){
            System.out.println("Wrong command!");
            System.exit(0);
        }
        System.out.println("Command Line:");
        System.out.println("URL of the index file:" + args[0]);

        String host = args[0].split("/")[0];
        String index_file = args[0].split("/")[1];
        String path = "/" + index_file;
        String authString = args[1];
        String authToken = Base64.getEncoder().encodeToString(authString.getBytes( "utf-8" ));

        //Create a socket
        Socket socket = new Socket(host, 80);
        //Create a Printwriter to sen message to server
        PrintWriter writer = new PrintWriter(socket.getOutputStream());
        //Send message
        writer.print("GET " + path + " HTTP/1.1\r\n" +
                "Host: " + host + "\r\n" +
                "Authorization: Basic " + authToken + "\r\n" +
                "Connection: close\r\n\r\n");
        writer.flush();
        //Create a Reader to get server response
        BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

        String line;
        boolean firstLine = true;
        int counter = 1;
        boolean isBody = false;
        String message = "";
        //Handle with connection errors and get response
        while ((line = reader.readLine()) != null) {
            if(firstLine & !line.contains("200 OK")){
                System.out.println("ERROR: Response Message is not 200!");
                System.exit(0);
            }

            if(line.trim().isEmpty()){
                isBody = true;
                continue;
            }

            if(isBody){
                message += line + "\n";
                if(counter == 2) System.out.println("File size is " + line + " Bytes");
                counter++;
            }

            firstLine = false;
        }
        //Print console and close the spcket and reader
        System.out.println("Index file is downloaded");
        reader.close();
        socket.close();
        //Separate the information of file parts
        String[] message_parts = message.split("\n");
        String fileName = message_parts[0];
        String contentSize = message_parts[1];

        ArrayList<String> addresses = new ArrayList<>();
        ArrayList<String> authStrings = new ArrayList<>();
        ArrayList<String> byteRanges = new ArrayList<>();

        for(int i = 2; i < message_parts.length; i += 3){
            addresses.add(message_parts[i]);
            authStrings.add(message_parts[i+1]);
            byteRanges.add(message_parts[i+2]);
        }

        int numOfRequests= addresses.size();

        String prevUpper = "0";
        String completeFile = "";
        StringBuffer buffer = new StringBuffer();
        System.out.println("There are " + addresses.size() + " servers in the index");
        //Make Http request to each server for each part
        for(int i = 0; i < numOfRequests; i++){
            String _path = "/" + addresses.get(i).split("/")[1];
            String _host = addresses.get(i).split("/")[0];
            String _authString = authStrings.get(i);
            String _authToken = Base64.getEncoder().encodeToString(_authString.getBytes( "utf-8" ));
            String baseLength = getBaseLength(prevUpper, byteRanges.get(i).split("-")[0]);
            int size = Integer.parseInt(byteRanges.get(i).split("-")[1]) - Integer.parseInt(byteRanges.get(i).split("-")[0]);
            String _byteRange = baseLength + "-" + size;
            //Create a new server connection for each request
            Socket socket2 = new Socket(_host, 80);
            PrintWriter writerReq = new PrintWriter(socket2.getOutputStream());
            System.out.println("Connected to " + _host + _path);
            //Send message
            writerReq.print("GET " + _path + " HTTP/1.1\r\n" +
                    "Host: " + _host + "\r\n" +
                    "Authorization: Basic " + _authToken + "\r\n" +
                    "Range: bytes=" + _byteRange + "\r\n"+
                    "Connection: close\r\n\r\n");
            writerReq.flush();

            BufferedReader readerRes = new BufferedReader(new InputStreamReader(socket2.getInputStream()));

            //Get response
            int num;
            String linex = "";
            while((linex = readerRes.readLine()) != null){
                if(linex.trim().isEmpty()){
                    break;
                }
            }
            while ((num = readerRes.read()) >= 0) {
                buffer.append((char) num);
            }

            System.out.println("Downloaded bytes " + increaseByOne(prevUpper) + " to " + byteRanges.get(i).split("-")[1] +
                    " (size = "+ difference(prevUpper, byteRanges.get(i).split("-")[1]) +")");
            prevUpper = byteRanges.get(i).split("-")[1];
            //Close the reader and socket
            readerRes.close();
            socket2.close();
        }
        System.out.println("Download of the file is complete (size = "+ contentSize +")");
        //Write whole content to a file
        try {
            BufferedWriter out = new BufferedWriter(new FileWriter(fileName));
            out.write(buffer.toString());
            out.close();
        }
        catch (IOException e)
        {
            System.out.println("Exception ");

        }
    }

}
