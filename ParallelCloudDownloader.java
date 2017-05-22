import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Base64;

/**
 * Created by mesutgurlek on 4/21/17.
 */
public class ParallelCloudDownloader {
    public static String calculateByteRange(String byteRange, int threadCount, int index){
        String[] range = byteRange.split("-");
        int lower = 0;
        int upper = Integer.parseInt(range[1]) - Integer.parseInt(range[0]);
        int extraBytes = (upper - lower + 1) % threadCount;

        int[] steps = new int[threadCount];
        for(int i = 0; i < threadCount; i++){
            if(extraBytes != 0 && extraBytes > i) steps[i] = (upper - lower + 1) / threadCount + 1;
            else steps[i] = (upper - lower + 1) / threadCount;
        }

        int newLower = lower, newUpper = 0;
        for(int i = 0; i < index; i++){
            newLower += steps[i];
        }
        newUpper = newLower + steps[index] - 1;
        return Integer.toString(newLower) + "-" + newUpper;
    }

    public static void main(String args[]) throws IOException, InterruptedException {
        if(args.length != 3){
            System.out.println("Wrong command!");
            System.exit(0);
        }
        System.out.println("URL of the index file:" + args[0]);

        String url_parts[] = args[0].split("/");
        String host = url_parts[0];
        String index_file = url_parts[1];
        String path = "/" + index_file;
        String authString = args[1];
        String authToken = Base64.getEncoder().encodeToString(authString.getBytes( "ISO-8859-1" ));
        int threadCount = Integer.parseInt(args[2]);

        //Create a socket
        Socket socket = new Socket(host, 80);
        //Create a Printwriter to sen message to server
        Writer writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), "ISO-8859-1"));
        //Send message
        writer.write("GET " + path + " HTTP/1.1\r\n" +
                "Host: " + host + "\r\n" +
                "Authorization: Basic " + authToken + "\r\n" +
                "Connection: close\r\n\r\n");
        writer.flush();

        //Create a Reader to get server response
        BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), "ISO-8859-1"));

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

        int numOfRequests = addresses.size();
        ArrayList<StringBuffer> fileBuffer = new ArrayList<>();
        System.out.println("There are " + numOfRequests + " servers in the index");

        int subFileCount = addresses.size();
        CloudThread[] threads = new CloudThread[threadCount * subFileCount];
        for(int i = 0; i < numOfRequests; i++){
            String _path = "/" + addresses.get(i).split("/")[1];
            String _host = addresses.get(i).split("/")[0];
            String _authString = authStrings.get(i);
            String _authToken = Base64.getEncoder().encodeToString(_authString.getBytes( "utf-8" ));
            String _byteRange = byteRanges.get(i);

            for (int j = 0; j < threadCount; j++){
                threads[i*threadCount + j] = new CloudThread(i*threadCount + j, _path, _host, _authString, _authToken, calculateByteRange(_byteRange, threadCount, j));
            }
        }

        // Start threads and join them
        long start = System.currentTimeMillis();
        for(int j = 0; j < threadCount * subFileCount; j++){
            threads[j].start();
        }
        for(int j = 0; j < threadCount * subFileCount; j++){
            threads[j].join();
        }
        // Combine the buffers
        String result = "";
        for(int j = 0; j < threadCount * subFileCount; j++){
            result += threads[j].getBuffer().toString();
        }
        long time = System.currentTimeMillis() - start;
        System.out.println(fileName + " is downloaded in " + time + " milliseconds with " + threadCount * subFileCount + " threads");
        System.out.println("# of threads: " + threadCount * subFileCount + " with download speed: " + Double.parseDouble(contentSize )/time + " bytes/ms");

        //Write whole content to a file
        try {
            BufferedWriter out = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(fileName), "ISO-8859-1"
            ));
            out.write(result);
            out.close();
        }
        catch (IOException e)
        {
            System.out.println("Exception ");

        }
    }

    public static class CloudThread extends Thread
    {
        private int idx;
        private String path, host, authString, authToken, byteRange;
        private StringBuffer buffer;

        public CloudThread(int idx, String path, String host, String authString, String authToken, String byteRange)
        {
            this.idx = idx;
            this.path = path;
            this.host = host;
            this.authString = authString;
            this.authToken = authToken;
            this.buffer = new StringBuffer();
            this.byteRange = byteRange;
        }

        public long getId(){
            return idx;
        }

        public StringBuffer getBuffer() {return buffer;}

        public String toString(){
            return "Id: " + idx + " path: " + path + " host: " + host + " byterange: " + byteRange;
        }

        @Override
        public void run()
        {
            //Create a new server connection for each request
            Socket socket = null;
            Writer writerReq = null;
            try {
                socket = new Socket(this.host, 80);
                writerReq = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), "ISO-8859-1"));

                //Send message
                writerReq.write("GET " + path + " HTTP/1.1\r\n" +
                        "Host: " + host + "\r\n" +
                        "Authorization: Basic " + authToken + "\r\n" +
                        "Range: bytes=" + byteRange + "\r\n"+
                        "Connection: close\r\n\r\n");
                writerReq.flush();

                BufferedReader readerRes = new BufferedReader(new InputStreamReader(socket.getInputStream(), "ISO-8859-1"));

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

                readerRes.close();
                socket.close();

            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }
}
