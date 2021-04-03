package pt.haslab.horus.graph.datastores.neo4j;

import com.google.gson.JsonObject;
import pt.haslab.horus.events.*;
import pt.haslab.horus.graph.timeline.TimelineIdGenerator;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class EventParamsBuilder {

    private final String idField;
    private final TimelineIdGenerator timelineIdGenerator;

    public EventParamsBuilder(String idField, TimelineIdGenerator timelineIdGenerator) {
        this.idField = idField;
        this.timelineIdGenerator = timelineIdGenerator;
    }

    public HashMap<String, Object> buildParamsMap(ProcessCreate event) {
        return new HashMap<String, Object>() {{
            put(idField, event.getId());
            put("userTime", event.getUserTime());
            put("kernelTime", event.getKernelTime());
            put("pid", event.getPid());
            put("tid", event.getTid());
            put("comm", event.getComm());
            put("host", event.getHost());
            put("threadId", timelineIdGenerator.getTimelineId(event));
            put("childPid", event.getChildPid());
            putAll(getJsonPropertiesAsString(event.getExtraData()));
        }};
    }

    public HashMap<String, Object> buildParamsMap(ProcessStart event) {
        return new HashMap<String, Object>() {{
            put(idField, event.getId());
            put("userTime", event.getUserTime());
            put("kernelTime", event.getKernelTime());
            put("pid", event.getPid());
            put("tid", event.getTid());
            put("comm", event.getComm());
            put("host", event.getHost());
            put("threadId", timelineIdGenerator.getTimelineId(event));
            putAll(getJsonPropertiesAsString(event.getExtraData()));
        }};
    }

    public HashMap<String, Object> buildParamsMap(ProcessEnd event) {
        return new HashMap<String, Object>() {{
            put(idField, event.getId());
            put("userTime", event.getUserTime());
            put("kernelTime", event.getKernelTime());
            put("pid", event.getPid());
            put("tid", event.getTid());
            put("comm", event.getComm());
            put("host", event.getHost());
            put("threadId", timelineIdGenerator.getTimelineId(event));
            putAll(getJsonPropertiesAsString(event.getExtraData()));
        }};
    }

    public HashMap<String, Object> buildParamsMap(ProcessJoin event) {
        return new HashMap<String, Object>() {{
            put(idField, event.getId());
            put("userTime", event.getUserTime());
            put("kernelTime", event.getKernelTime());
            put("pid", event.getPid());
            put("tid", event.getTid());
            put("comm", event.getComm());
            put("host", event.getHost());
            put("threadId", timelineIdGenerator.getTimelineId(event));
            put("childPid", event.getChildPid());
            putAll(getJsonPropertiesAsString(event.getExtraData()));
        }};
    }

    public HashMap<String, Object> buildParamsMap(SocketAccept event) {
        return new HashMap<String, Object>() {{
            put(idField, event.getId());
            put("userTime", event.getUserTime());
            put("kernelTime", event.getKernelTime());
            put("pid", event.getPid());
            put("tid", event.getTid());
            put("comm", event.getComm());
            put("host", event.getHost());
            put("threadId", timelineIdGenerator.getTimelineId(event));
            put("socketId", event.getSocketId());
            put("socketFamily", event.getSocketFamily());
            put("socketFrom", event.getSocketFrom());
            put("socketFromPort", event.getSourcePort());
            put("socketTo", event.getSocketTo());
            put("socketToPort", event.getDestinationPort());
            putAll(getJsonPropertiesAsString(event.getExtraData()));
        }};
    }

    public HashMap<String, Object> buildParamsMap(SocketConnect event) {
        return new HashMap<String, Object>() {{
            put(idField, event.getId());
            put("userTime", event.getUserTime());
            put("kernelTime", event.getKernelTime());
            put("pid", event.getPid());
            put("tid", event.getTid());
            put("comm", event.getComm());
            put("host", event.getHost());
            put("threadId", timelineIdGenerator.getTimelineId(event));
            put("socketId", event.getSocketId());
            put("socketFamily", event.getSocketFamily());
            put("socketFrom", event.getSocketFrom());
            put("socketFromPort", event.getSourcePort());
            put("socketTo", event.getSocketTo());
            put("socketToPort", event.getDestinationPort());
            putAll(getJsonPropertiesAsString(event.getExtraData()));
        }};
    }

    public HashMap<String, Object> buildParamsMap(SocketSend event) {
        return new HashMap<String, Object>() {{
            put(idField, event.getId());
            put("userTime", event.getUserTime());
            put("kernelTime", event.getKernelTime());
            put("pid", event.getPid());
            put("tid", event.getTid());
            put("comm", event.getComm());
            put("host", event.getHost());
            put("threadId", timelineIdGenerator.getTimelineId(event));
            put("socketId", event.getSocketId());
            put("socketFamily", event.getSocketFamily());
            put("socketFrom", event.getSocketFrom());
            put("socketFromPort", event.getSourcePort());
            put("socketTo", event.getSocketTo());
            put("socketToPort", event.getDestinationPort());
            put("size", event.getSize());
            putAll(getJsonPropertiesAsString(event.getExtraData()));
        }};
    }

    public HashMap<String, Object> buildParamsMap(SocketReceive event) {
        return new HashMap<String, Object>() {{
            put(idField, event.getId());
            put("userTime", event.getUserTime());
            put("kernelTime", event.getKernelTime());
            put("pid", event.getPid());
            put("tid", event.getTid());
            put("comm", event.getComm());
            put("host", event.getHost());
            put("threadId", timelineIdGenerator.getTimelineId(event));
            put("socketId", event.getSocketId());
            put("socketFamily", event.getSocketFamily());
            put("socketFrom", event.getSocketFrom());
            put("socketFromPort", event.getSourcePort());
            put("socketTo", event.getSocketTo());
            put("socketToPort", event.getDestinationPort());
            put("size", event.getSize());
            putAll(getJsonPropertiesAsString(event.getExtraData()));
        }};
    }

    public HashMap<String, Object> buildParamsMap(Log event) {
        return new HashMap<String, Object>() {{
            put(idField, event.getId());
            put("userTime", event.getUserTime());
            put("kernelTime", event.getKernelTime());
            put("pid", event.getPid());
            put("tid", event.getTid());
            put("comm", event.getComm());
            put("host", event.getHost());
            put("threadId", timelineIdGenerator.getTimelineId(event));
            putAll(getJsonPropertiesAsString(event.getExtraData()));
        }};
    }

    public HashMap<String, Object> buildParamsMap(FSync event) {
        return new HashMap<String, Object>() {{
            put(idField, event.getId());
            put("userTime", event.getUserTime());
            put("kernelTime", event.getKernelTime());
            put("pid", event.getPid());
            put("tid", event.getTid());
            put("comm", event.getComm());
            put("host", event.getHost());
            put("threadId", timelineIdGenerator.getTimelineId(event));
            putAll(getJsonPropertiesAsString(event.getExtraData()));
        }};
    }

    public HashMap<String, Object> buildParamsMap(Event event) {
        if(event instanceof ProcessCreate)
            return this.buildParamsMap((ProcessCreate) event);

        if(event instanceof ProcessStart)
            return this.buildParamsMap((ProcessStart) event);

        if(event instanceof ProcessJoin)
            return this.buildParamsMap((ProcessJoin) event);

        if(event instanceof ProcessEnd)
            return this.buildParamsMap((ProcessEnd) event);

        if(event instanceof SocketSend)
            return this.buildParamsMap((SocketSend) event);

        if(event instanceof SocketReceive)
            return this.buildParamsMap((SocketReceive) event);

        if(event instanceof SocketAccept)
            return this.buildParamsMap((SocketAccept) event);

        if(event instanceof SocketConnect)
            return this.buildParamsMap((SocketConnect) event);

        if(event instanceof Log)
            return this.buildParamsMap((Log) event);

        if(event instanceof FSync)
            return this.buildParamsMap((FSync) event);

        throw new IllegalArgumentException("Unrecognized event type.");
    }

    private Map<? extends String, ?> getJsonPropertiesAsString(JsonObject extraData) {
        return extraData.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, data -> data.getValue().getAsString()));
    }
}
