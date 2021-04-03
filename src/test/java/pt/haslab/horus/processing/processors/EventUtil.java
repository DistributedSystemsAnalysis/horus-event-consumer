package pt.haslab.horus.processing.processors;

import pt.haslab.horus.events.SocketReceive;
import pt.haslab.horus.events.SocketSend;

import java.util.ArrayList;

public class EventUtil {
    public static ArrayList<SocketSend> getSendEvents(int... sizes) {
        ArrayList<SocketSend> events = new ArrayList<>();

        int i = 0;
        for (int size : sizes) {
            i++;
            SocketSend sendEvent = new SocketSend();
            sendEvent.setId("snd" + i);
            sendEvent.setUserTime(1);
            sendEvent.setKernelTime(3 + i);
            sendEvent.setPid(9);
            sendEvent.setTid(10);
            sendEvent.setComm("comm");
            sendEvent.setHost("host");
            sendEvent.setSocketId("socket-id");
            sendEvent.setSocketFamily(10);
            sendEvent.setSocketFrom("0.0.0.0");
            sendEvent.setSourcePort(10);
            sendEvent.setSocketTo("0.0.0.0");
            sendEvent.setDestinationPort(10);
            sendEvent.setSize(size);
            events.add(sendEvent);
        }

        return events;
    }

    public static ArrayList<SocketReceive> getReceiveEvents(int... sizes) {
        ArrayList<SocketReceive> events = new ArrayList<>();

        int i = 0;
        for (int size : sizes) {
            i++;
            SocketReceive receiveEvent = new SocketReceive();
            receiveEvent.setId("rcv" + i);
            receiveEvent.setUserTime(1);
            receiveEvent.setKernelTime(3 + i);
            receiveEvent.setPid(9);
            receiveEvent.setTid(10);
            receiveEvent.setComm("comm");
            receiveEvent.setHost("host2");
            receiveEvent.setSocketId("socket-id");
            receiveEvent.setSocketFamily(10);
            receiveEvent.setSocketFrom("0.0.0.0");
            receiveEvent.setSourcePort(10);
            receiveEvent.setSocketTo("0.0.0.0");
            receiveEvent.setDestinationPort(10);
            receiveEvent.setSize(size);
            events.add(receiveEvent);
        }

        return events;
    }
}
