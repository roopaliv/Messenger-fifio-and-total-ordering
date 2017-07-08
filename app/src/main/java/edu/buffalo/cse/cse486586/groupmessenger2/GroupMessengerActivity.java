package edu.buffalo.cse.cse486586.groupmessenger2;

import android.app.Activity;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.View;
import android.widget.EditText;
import android.widget.TextView;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

class MulticastMessage implements Comparable<MulticastMessage> {
    int priority;
    int port;
    boolean shallDeliver;
    public String msg;
    public String sentTo;

    @Override
    public int compareTo(MulticastMessage another) {
        int result = 0;
        result = (this.priority > another.priority) ? 1 : (this.priority < another.priority ? -1 : 0);
        if (result == 0)
            result = this.port > another.port ? 1 : (this.port < another.port ? -1 : 0);
        return result;
    }

    @Override
    public boolean equals(Object o) {
        return this.msg.equals(((MulticastMessage)o).msg);
    }

}

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 *
 * @author stevko
 */
public class GroupMessengerActivity extends Activity {
    static final String TAG = "GroupMessengerActivity";
    static final int PORT1 = 11108;
    static final int PORT2 = 11112;
    static final int PORT3 = 11116;
    static final int PORT4 = 11120;
    static final int PORT5 = 11124;
    ArrayList<String> ALLPORTS = new ArrayList<String>() {{
        add(Integer.toString(PORT1));
        add(Integer.toString(PORT2));
        add(Integer.toString(PORT3));
        add(Integer.toString(PORT4));
        add(Integer.toString(PORT5));
    }};
    static final int SERVER_PORT = 10000;
    private ContentResolver mContentResolver = null;
    private final Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger2.provider");
    private String delimiter = ":";
    private int totalWorkingPorts = 5;
    private int dbCnt = 0;
    private int currentPriority = 0;
    private int prCount = 1;
    private int livePorts = 1;
    private HashMap<String, Integer> noOfMsgs = new HashMap<String, Integer>();

    private String myPort;
    private String failedPort;

    private ScheduledFuture<?> continueHeartbeatDetection = null;
    boolean done = false;

    private PriorityQueue<MulticastMessage> orderedMsgsList = new PriorityQueue<MulticastMessage>();
    private HashMap<String, String> MsgsList = new HashMap<String, String>();
    private ArrayList<String> aliveList = new ArrayList<String>();
    private Lock lock = new ReentrantLock();
    private Lock listLock = new ReentrantLock();
    private HashMap<String, String> portsMap = new HashMap<String, String>();


    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    protected void bMulticast(String message, boolean checkFailure) {
        for (String p: ALLPORTS) {
            try {
                if (!p.equals(failedPort)) {
                    Socket multicastSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(p));
                    OutputStreamWriter multicastWriter = new OutputStreamWriter(multicastSocket.getOutputStream());
                    multicastWriter.write(message, 0, message.length());
                    multicastWriter.close();
                    multicastSocket.close();
                }
            } catch (Exception e) {
                if(checkFailure)
                    failedPort = p;
                Log.e(TAG, e.getMessage());
            }
        }
    }
    protected String prepareMessage(MulticastMessage msg){
        return msg.msg + delimiter + msg.priority +delimiter+ msg.port +delimiter+ msg.shallDeliver;
    }

    protected MulticastMessage ParseMsg(String received){
        MulticastMessage msg = new MulticastMessage();
        msg.priority = Integer.parseInt(received.split(delimiter)[2]);
        msg.port = Integer.parseInt(received.split(delimiter)[3]);
        msg.msg = received.split(delimiter)[1];
        return msg;
    }



    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);
        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

        final EditText editText = (EditText) findViewById(R.id.editText1);

        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, e.getMessage());
            return;
        }

        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));

        findViewById(R.id.button4).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                String message = editText.getText().toString();
                editText.setText("");
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message, myPort);
            }
        });

    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            while (true) {
                try {
                    ServerSocket ipSocket = sockets[0];
                    Socket input = ipSocket.accept();
                    InputStreamReader ipStream = new InputStreamReader(input.getInputStream());
                    BufferedReader ipBuffer = new BufferedReader(ipStream);
                    String receivedMsg = ipBuffer.readLine();
                    switch(Character.getNumericValue(receivedMsg.charAt(0))) {
                        case 1:
                            receivedHeartbeatMsg(receivedMsg);
                            break;
                        case 2:
                            receivedFailureMsg(receivedMsg);
                            break;
                        case 3:
                            receivedNewMessage(receivedMsg);
                            break;
                        case 4:
                            receivedPriorityMsg(receivedMsg);
                            break;
                        case 5:
                            receivedAgreementMsg(receivedMsg);
                            break;
                    }
                } catch (IOException e) {
                    Log.e(TAG, e.getMessage());
                }
            }

        }

        protected void onProgressUpdate(String... strings) {
            if (strings[0] != null) {
                ContentValues cv = new ContentValues();
                cv.put("key", dbCnt);
                cv.put("value", strings[0].trim().split(delimiter)[0]);
                ++dbCnt;
                mContentResolver = getContentResolver();
                mContentResolver.insert(mUri, cv);
            }
        }

        protected void receivedAgreementMsg(String receivedMsg){
            if (Integer.parseInt(receivedMsg.split(delimiter)[2]) > currentPriority)
                currentPriority = Integer.parseInt(receivedMsg.split(delimiter)[2])+1;
            listLock.lock();
            MulticastMessage parsedMsg = ParseMsg(receivedMsg);
            parsedMsg.shallDeliver = true;
            AddMsgsToList(parsedMsg);
            listLock.unlock();
            insertAgreedMsg();
        }

        protected void insertAgreedMsg(){
            while (orderedMsgsList.size() > 0 && orderedMsgsList.peek().shallDeliver) {
                MulticastMessage msg = orderedMsgsList.poll();
                noOfMsgs.remove(msg.msg);
                MsgsList.remove(msg.msg);
                publishProgress(prepareMessage(msg));
            }
        }

        protected void AddMsgsToList(MulticastMessage msg) {
            listLock.lock();
            if(!orderedMsgsList.contains(msg)){
                orderedMsgsList.add(msg);
            }
            orderedMsgsList.remove(msg);
            orderedMsgsList.add(msg);
            listLock.unlock();
        }

        protected void receivedHeartbeatMsg(String receivedMsg){
            if (!aliveList.contains(receivedMsg.split(delimiter)[1]))
                aliveList.add(receivedMsg.split(delimiter)[1]);
        }

        protected  void receivedPriorityMsg(String receivedMsg){
            lock.lock();
            if (!noOfMsgs.containsKey(receivedMsg.split(delimiter)[1])) {
                noOfMsgs.put(receivedMsg.split(delimiter)[1], 1);
                MsgsList.put(receivedMsg.split(delimiter)[1], receivedMsg.split(delimiter)[2] + delimiter + receivedMsg.split(delimiter)[3]);
                portsMap.put(receivedMsg.split(delimiter)[1], receivedMsg.split(delimiter)[5]);
                MulticastMessage parsedMsg = ParseMsg(receivedMsg);
                parsedMsg.shallDeliver = false;
                AddMsgsToList(parsedMsg);
                lock.unlock();
            } else {
                updateLocalPriority(receivedMsg.split(delimiter), receivedMsg);
                sendAgreementMsg(receivedMsg.split(delimiter), receivedMsg);
            }
        }
        protected  void updateLocalPriority(String[] priorityMsgs, String receivedMsg){
            lock.lock();
            prCount =(noOfMsgs.get(priorityMsgs[1]));
            noOfMsgs.put(priorityMsgs[1], ++prCount);
            portsMap.put(priorityMsgs[1], portsMap.get(priorityMsgs[1]) + "," + priorityMsgs[5]);
            lock.unlock();
            if (Integer.parseInt(MsgsList.get(priorityMsgs[1]).split(delimiter)[0]) < Integer.parseInt(priorityMsgs[2])) {
                MsgsList.put(priorityMsgs[1], Integer.parseInt(priorityMsgs[2]) + delimiter + Integer.parseInt(priorityMsgs[3]));
                MulticastMessage parsedMsg = ParseMsg(receivedMsg);
                parsedMsg.shallDeliver = false;
                AddMsgsToList(parsedMsg);
            }
        }
        protected void sendAgreementMsg(String[] priorityArr, String receivedMsg){
            if (prCount >= totalWorkingPorts) {
                lock.lock();
                MulticastMessage agreedMsg = new MulticastMessage();
                agreedMsg.msg = "5"+delimiter + priorityArr[1];
                agreedMsg.priority = Integer.parseInt(MsgsList.get(priorityArr[1]).split(delimiter)[0]);
                agreedMsg.port = Integer.parseInt(MsgsList.get(priorityArr[1]).split(delimiter)[1]);
                MulticastMessage parsedMsg = ParseMsg(receivedMsg);
                parsedMsg.shallDeliver = false;
                AddMsgsToList(parsedMsg);
                bMulticast(prepareMessage(agreedMsg), true);
                lock.unlock();
            }
        }
        protected void receivedFailureMsg(String receivedMsg){
            if (!done) {
                if(continueHeartbeatDetection !=null)
                    continueHeartbeatDetection.cancel(true);
                lock.lock();
                PriorityQueue<MulticastMessage> heapifiedList = new PriorityQueue<MulticastMessage>(orderedMsgsList);
                failedPort = receivedMsg.split(delimiter)[1];
                while (heapifiedList.size() > 0) {
                    MulticastMessage failureMsg =  heapifiedList.poll();
                    if (failureMsg.port == Integer.parseInt(failedPort))
                        orderedMsgsList.remove(failureMsg);
                    sendMsgsFromLivePorts(failureMsg);
                }
                lock.unlock();
                totalWorkingPorts--;
                done = true;
            }
        }

        protected  void sendMsgsFromLivePorts(MulticastMessage failureMsg){
            if (portsMap.get(failureMsg.msg) != null) {
                ArrayList<String> portList = new ArrayList(Arrays.asList(portsMap.get(failureMsg.msg).split(",")));
                ArrayList<String> ports = new ArrayList<String>();
                if (noOfMsgs.get(failureMsg.msg) == 4 && !portList.contains(failedPort)) {
                    bMulticast("5"+delimiter + prepareMessage(failureMsg), true);
                } else if (noOfMsgs.get(failureMsg.msg) < 4 && portList.contains(failedPort)) {
                    for (String port : portList) {
                        if (!port.equals(failedPort))
                            ports.add(port);
                    }
                    portsMap.put(failureMsg.msg, ports.toString());
                    noOfMsgs.put(failureMsg.msg, noOfMsgs.get(failureMsg.msg) - 1);
                }
            }
        }

        protected void receivedNewMessage(String receivedMsg){
            if (continueHeartbeatDetection == null)
                continueHeartbeatDetection = Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(new CheckAlive(), 0, 5, TimeUnit.SECONDS);
            try {
                listLock.lock();
                if (!(orderedMsgsList.contains(ParseMsg(receivedMsg))))
                    orderedMsgsList.add(ParseMsg(receivedMsg));
                listLock.unlock();
                proposePriority(receivedMsg);
            }
            catch(Exception e){
                Log.e(TAG, e.getMessage());
            }
        }


        protected void proposePriority(String receivedMsg){
            if (continueHeartbeatDetection == null)
                continueHeartbeatDetection = Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(new CheckAlive(), 0, 5, TimeUnit.SECONDS);
            try {
                String msgArr[] = receivedMsg.split(delimiter);
                if (!msgArr[3].equals(failedPort)) {
                    MulticastMessage pMsg = new MulticastMessage();
                    pMsg.msg = "4"+delimiter + msgArr[1];
                    pMsg.priority = ++currentPriority;
                    pMsg.port = Integer.parseInt(msgArr[3]);
                    pMsg.shallDeliver = false;
                    String pMsgProcessed = prepareMessage(pMsg) + delimiter + myPort;
                    Socket pSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgArr[3]));
                    OutputStreamWriter pWriter = new OutputStreamWriter(pSocket.getOutputStream());
                    pWriter.write(pMsgProcessed, 0, pMsgProcessed.length());
                    pWriter.close();
                    pSocket.close();
                }
            }
            catch(Exception e){
                Log.e(TAG, e.getMessage());
            }
        }
    }


    private class ClientTask extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... msgs) {
            MulticastMessage simpleMessage = new MulticastMessage();
            simpleMessage.msg = "3" + delimiter + msgs[0];
            simpleMessage.port = Integer.parseInt(myPort);
            simpleMessage.priority = currentPriority;
            simpleMessage.shallDeliver = false;
            bMulticast(prepareMessage(simpleMessage), true);
            return null;
        }
    }

    private class CheckAlive implements Runnable {
        @Override
        public void run() {
            checkHeartbeat();
        }

        protected void checkHeartbeat() {
            if (livePorts % 2 != 0)
                bMulticast("1" + delimiter + myPort, false);
            else
                sendFailureMsg();
            ++livePorts;
        }

        protected  void sendFailureMsg(){
            if(aliveList.size()!=0 && aliveList.size() < 5){
                for (String port : ALLPORTS) {
                    if (!aliveList.contains(port)) {
                        failedPort = port;
                        bMulticast("2" + delimiter + port, false);
                        continueHeartbeatDetection.cancel(true);
                    }
                }
            }
            aliveList.clear();
        }
    }

}
