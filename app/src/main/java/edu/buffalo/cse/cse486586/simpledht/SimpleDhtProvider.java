package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

/*
* Seperate key from value => <<<
* Seperate key value pairs => >>>
* Seperate different contexts => ###
* */

public class SimpleDhtProvider extends ContentProvider {

    static final String TAG = SimpleDhtProvider.class.getSimpleName();

    ArrayList<String> REMOTE_PORTS = new ArrayList<String>(Arrays.asList("11108", "11112", "11116", "11120", "11124"));
    static final int SERVER_PORT = 10000;
    String portString;
    String myPort;

    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";

    private String prev_id;
    private String next_id;

    Uri uri = new Uri.Builder().authority("edu.buffalo.cse.cse486586.simpledht.provider").scheme("content").build();

    ArrayList<AVD> AVD_List = new ArrayList<AVD>(5);

    Map<String, String> queryResponse = new HashMap<String, String>();

    boolean globalDumpComplete = false;

    public class AVD implements Comparable<AVD> {

        public String my_id;
        public String hashValue;

        public AVD(String my_id, String hashValue) {
            this.my_id     = my_id;
            this.hashValue = hashValue;
        }

        @Override
        public int compareTo(AVD avd) {
            return this.hashValue.compareTo(avd.hashValue);
        }
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub

        String filename = values.getAsString(KEY_FIELD);
        String content =  values.getAsString(VALUE_FIELD) + "\n";

        try {
            if (portString.equals(prev_id) && portString.equals(next_id)) {
                FileOutputStream outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
                outputStream.write(content.getBytes());
                outputStream.close();
            }
            else if ((genHash(prev_id).compareTo(genHash(portString)) > 0 && genHash(filename).compareTo(genHash(portString)) <= 0) || (genHash(prev_id).compareTo(genHash(portString)) > 0 && genHash(filename).compareTo(genHash(prev_id)) > 0)){
                FileOutputStream outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
                outputStream.write(content.getBytes());
                outputStream.close();
            }
            else if (genHash(filename).compareTo(genHash(portString)) > 0 || genHash(filename).compareTo(genHash(prev_id)) <= 0){
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, portString, "forwardInsert", filename, content);
            }
            else {
                FileOutputStream outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
                outputStream.write(content.getBytes());
                outputStream.close();
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            Log.d(TAG, "File write failed: " + filename);
        }

        return uri;
    }

    @Override
    public boolean onCreate() {

        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        portString = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        prev_id = portString;
        next_id = portString;
        myPort = String.valueOf((Integer.parseInt(portString) * 2));

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Unable to create a ServerSocket");
            return false;
        }

        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, portString, "addNewAVD", myPort);
        return true;
    }

    public Void updateAVDNeighbours(String REMOTE_PORT, String prev_id, String next_id){
        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(REMOTE_PORT));

            String msgToSend = REMOTE_PORT + "###" + "updateNeighbours" + "###" + prev_id + "###" + next_id;

            OutputStream outputStream = socket.getOutputStream();
            PrintWriter printWriter = new PrintWriter(outputStream, true);
            printWriter.print(msgToSend);
            printWriter.flush();

            socket.close();
        } catch (UnknownHostException e) {
            Log.e(TAG, "ClientTask UnknownHostException");
        } catch (IOException e) {
            e.printStackTrace();
            Log.e(TAG, "ClientTask socket IOException");
        }
        return null;
    }



    public Void handleAddNewAvd(String[] msg_split){
        try {
            String hashValue = genHash(msg_split[0]);
            AVD avd = new AVD(msg_split[0], hashValue);
            AVD_List.add(avd);
            Collections.sort(AVD_List);

            String REMOTE_PORT;

            if (AVD_List.size() == 1){
                // Do nothing
            }
            else if (AVD_List.size() == 2){
                REMOTE_PORT = String.valueOf((Integer.parseInt(AVD_List.get(0).my_id) * 2));
                updateAVDNeighbours(REMOTE_PORT, AVD_List.get(1).my_id, AVD_List.get(1).my_id);
                REMOTE_PORT = String.valueOf((Integer.parseInt(AVD_List.get(1).my_id) * 2));
                updateAVDNeighbours(REMOTE_PORT, AVD_List.get(0).my_id, AVD_List.get(0).my_id);
            }
            else {
                int curIndex = AVD_List.indexOf(avd);
                int prevIndex, nextIndex;
                if (curIndex == 0){
                    prevIndex = AVD_List.size() - 1;
                    nextIndex = curIndex + 1;
                }
                else if (curIndex == AVD_List.size() - 1){
                    prevIndex = curIndex - 1;
                    nextIndex = 0;
                }
                else {
                    prevIndex = curIndex - 1;
                    nextIndex = curIndex + 1;
                }

                AVD prevAVD = AVD_List.get(prevIndex);
                AVD nextAVD = AVD_List.get(nextIndex);

                REMOTE_PORT = String.valueOf((Integer.parseInt(prevAVD.my_id) * 2));
                updateAVDNeighbours(REMOTE_PORT, "NULL", avd.my_id);

                REMOTE_PORT = String.valueOf((Integer.parseInt(avd.my_id) * 2));
                updateAVDNeighbours(REMOTE_PORT, prevAVD.my_id, nextAVD.my_id);

                REMOTE_PORT = String.valueOf((Integer.parseInt(nextAVD.my_id) * 2));
                updateAVDNeighbours(REMOTE_PORT, avd.my_id, "NULL");
            }
        }
        catch (NoSuchAlgorithmException e){
            Log.d(TAG, "Could not add the AVD into the chord");
        }
        return null;
    }

    public Void handleForwardQuery(String msgReceived){
        String msg_split[] = msgReceived.split("###");
        String selection = msg_split[2];

        boolean isSelectionPresent = false;

        try {
            String listOfFiles[] = getContext().fileList();
            for (String S : listOfFiles){
                if (S.equals(selection)){
                    isSelectionPresent = true;
                    break;
                }
            }

            if (isSelectionPresent){
                // return the file with value
                FileInputStream fileInputStream = getContext().openFileInput(selection);
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileInputStream));
                String content = bufferedReader.readLine();
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg_split[0], "queryResponse", selection, content);
            }
            else {
                // Forward the query
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg_split[0], "forwardQuery", selection);
            }
        }
        catch (Exception e) {
            Log.d(TAG, "File name failed "+selection);
            Log.d(TAG, "Reading file failed "+ e.getLocalizedMessage());
        }

        return null;
    }

    public Void handleGlobalQuery(String msgReceived){
        String msg_split[] = msgReceived.split("###");

        try {
            String keyValuePairs = "";
            FileInputStream fileInputStream;
            BufferedReader bufferedReader;
            String content;

            String listOfFiles[] = getContext().fileList();
            for (String S : listOfFiles){
                fileInputStream = getContext().openFileInput(S);
                bufferedReader = new BufferedReader(new InputStreamReader(fileInputStream));
                content = bufferedReader.readLine();
                keyValuePairs = keyValuePairs + S + "<<<" + content + ">>>";
            }

            if (msg_split[0].equals(next_id)){
                // Global query is complete
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg_split[0], "globalQueryResponse", keyValuePairs, "globalQueryComplete");
            }
            else {
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg_split[0], "globalQueryResponse", keyValuePairs, "globalQueryIncomplete");
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg_split[0], msg_split[1]);
            }
        }
        catch (Exception e) {
            Log.d(TAG, "Global Query failed ");
        }
        return null;
    }

    public Void handleGlobalQueryResponse(String msgReceived){
        String msg_split[] = msgReceived.split("###");
        String keyValuePairs[] = msg_split[2].split(">>>");

        for (String keyValue : keyValuePairs){
            Log.d(TAG, "keyvalue: " + keyValue);
            String key = keyValue.split("<<<")[0];
            String value = keyValue.split("<<<")[1];
            queryResponse.put(key, value);
        }

        if (msg_split[3].equals("globalQueryComplete")){
            globalDumpComplete = true;
            synchronized (queryResponse)
            {
                queryResponse.notifyAll();
            }
        }
        return null;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            String msgReceived;

            while (true) {
                try {
                    Socket socket = serverSocket.accept();
                    InputStream inputStream = socket.getInputStream();
                    InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
                    BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                    msgReceived = bufferedReader.readLine();

                    String msg_split[] = msgReceived.split("###");

                    if (msg_split[1].equals("addNewAVD")) {
                        handleAddNewAvd(msg_split);
                    }
                    else if (msg_split[1].equals("updateNeighbours")){
                        prev_id = !msg_split[2].equals("NULL") ? msg_split[2] : prev_id;
                        next_id = !msg_split[3].equals("NULL") ? msg_split[3] : next_id;
                    }
                    else {
                        publishProgress(msgReceived);
                    }

                    bufferedReader.close();
                    socket.close();
                }
                catch (IOException e) {
                    Log.e(TAG, "Failed to publish message");
                }
            }
        }

        protected void onProgressUpdate(String...strings) {
            String msgReceived = strings[0].trim();
            String msg_split[] = msgReceived.split("###");

            if (msg_split[1].equals("forwardInsert")){
                ContentValues contentValues = new ContentValues();
                contentValues.put(KEY_FIELD, msg_split[2]);
                contentValues.put(VALUE_FIELD, msg_split[3]);
                insert(uri, contentValues);
            }
            else if (msg_split[1].equals("forwardQuery")){
                handleForwardQuery(msgReceived);
            }
            else if (msg_split[1].equals("queryResponse")){
                queryResponse.put(msg_split[2], msg_split[3]);
                synchronized (queryResponse)
                {
                    queryResponse.notifyAll();
                }
            }
            else if (msg_split[1].equals("globalQuery")){
                handleGlobalQuery(msgReceived);
            }
            else if (msg_split[1].equals("globalQueryResponse")){
                handleGlobalQueryResponse(msgReceived);
            }

            return;
        }
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        // TODO Auto-generated method stub

        try {

            MatrixCursor cursor = new MatrixCursor(new String[]{KEY_FIELD, VALUE_FIELD});
            FileInputStream fileInputStream;
            BufferedReader bufferedReader;
            String content;

            if (selection.equals("@")){
                String listOfFiles[] = getContext().fileList();
                for (String S : listOfFiles){
                    fileInputStream = getContext().openFileInput(S);
                    bufferedReader = new BufferedReader(new InputStreamReader(fileInputStream));
                    content = bufferedReader.readLine();
                    cursor.addRow(new String[] {S, content});
                }
                return cursor;
            }
            else if (selection.equals("*")){
                String listOfFiles[] = getContext().fileList();
                for (String S : listOfFiles){
                    fileInputStream = getContext().openFileInput(S);
                    bufferedReader = new BufferedReader(new InputStreamReader(fileInputStream));
                    content = bufferedReader.readLine();
                    cursor.addRow(new String[] {S, content});
                }
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, portString, "globalQuery");
                synchronized (queryResponse){
                    while (globalDumpComplete == false){
                        queryResponse.wait();
                    }
                    globalDumpComplete = false;
                    for (Map.Entry<String, String> entry : queryResponse.entrySet())
                    {
                        cursor.addRow(new String[] {entry.getKey(), entry.getValue()});
                    }
                }
                return cursor;
            }
            else {
                try {
                    boolean isSelectionPresent = false;
                    String listOfFiles[] = getContext().fileList();
                    for (String S : listOfFiles){
                        if (S.equals(selection)){
                            isSelectionPresent = true;
                            break;
                        }
                    }

                    if (isSelectionPresent){
                        fileInputStream = getContext().openFileInput(selection);
                        bufferedReader = new BufferedReader(new InputStreamReader(fileInputStream));
                        content = bufferedReader.readLine();
                        cursor.addRow(new String[] {selection, content});
                        return cursor;
                    }
                    else {
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, portString, "forwardQuery", selection);
                        synchronized (queryResponse){
                            while (queryResponse.isEmpty()){
                                queryResponse.wait();
                            }
                            content = queryResponse.get(selection);
                            queryResponse.clear();
                            cursor.addRow(new String[] {selection, content});
                        }
                        return cursor;
                    }
                }
                catch (Exception e) {
                    Log.d(TAG, "File name failed " + selection);
                    Log.d(TAG, "Reading file failed " + e.getLocalizedMessage());
                }
            }
        } catch (Exception e) {
            Log.d(TAG, "File name failed "+selection);
            Log.d(TAG, "Reading file failed "+ e.getLocalizedMessage());
        }



        return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {

            String REMOTE_PORT = "11108";

            try {
                if (msgs[1].equals("addNewAVD")){
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(REMOTE_PORT));

                    String msgToSend = msgs[0] + "###" + msgs[1];

                    OutputStream outputStream = socket.getOutputStream();
                    PrintWriter printWriter = new PrintWriter(outputStream, true);
                    printWriter.print(msgToSend);
                    printWriter.flush();

                    socket.close();
                }
                else if (msgs[1].equals("forwardInsert")){
                    REMOTE_PORT = String.valueOf((Integer.parseInt(next_id) * 2));
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(REMOTE_PORT));

                    String msgToSend = portString + "###" + "forwardInsert" + "###" + msgs[2] + "###" + msgs[3];

                    OutputStream outputStream = socket.getOutputStream();
                    PrintWriter printWriter = new PrintWriter(outputStream, true);
                    printWriter.print(msgToSend);
                    printWriter.flush();

                    socket.close();
                }
                else if (msgs[1].equals("forwardQuery")){
                    REMOTE_PORT = String.valueOf((Integer.parseInt(next_id) * 2));
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(REMOTE_PORT));

                    String msgToSend = msgs[0] + "###" + msgs[1] + "###" + msgs[2];

                    OutputStream outputStream = socket.getOutputStream();
                    PrintWriter printWriter = new PrintWriter(outputStream, true);
                    printWriter.print(msgToSend);
                    printWriter.flush();

                    socket.close();
                }
                else if (msgs[1].equals("queryResponse")){
                    REMOTE_PORT = String.valueOf((Integer.parseInt(msgs[0]) * 2));
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(REMOTE_PORT));

                    String msgToSend = msgs[0] + "###" + msgs[1] + "###" + msgs[2] + "###" + msgs[3];

                    OutputStream outputStream = socket.getOutputStream();
                    PrintWriter printWriter = new PrintWriter(outputStream, true);
                    printWriter.print(msgToSend);
                    printWriter.flush();

                    socket.close();
                }
                else if (msgs[1].equals("globalQuery")){
                    REMOTE_PORT = String.valueOf((Integer.parseInt(next_id) * 2));
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(REMOTE_PORT));

                    String msgToSend = msgs[0] + "###" + msgs[1];

                    OutputStream outputStream = socket.getOutputStream();
                    PrintWriter printWriter = new PrintWriter(outputStream, true);
                    printWriter.print(msgToSend);
                    printWriter.flush();

                    socket.close();
                }
                else if (msgs[1].equals("globalQueryResponse")){
                    REMOTE_PORT = String.valueOf((Integer.parseInt(msgs[0]) * 2));
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(REMOTE_PORT));

                    String msgToSend = msgs[0] + "###" + msgs[1] + "###" + msgs[2] + "###" + msgs[3];

                    OutputStream outputStream = socket.getOutputStream();
                    PrintWriter printWriter = new PrintWriter(outputStream, true);
                    printWriter.print(msgToSend);
                    printWriter.flush();

                    socket.close();
                }
                else {
                    Log.d(TAG, "Something went wrong with clientTask logic.");
                }
            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                e.printStackTrace();
                Log.e(TAG, "ClientTask socket IOException");
            }
            return null;
        }

    }
}
