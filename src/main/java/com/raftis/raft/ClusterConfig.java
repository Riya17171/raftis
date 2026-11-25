package com.raftis.raft;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Cluster membership, from {@code --node-id 1 --cluster 1=host:6380:7380,2=host:6381:7381,...}.
 * Each member is {@code id=host:clientPort:raftPort}.
 */
public record ClusterConfig(int selfId, Map<Integer, Member> members) {

    public record Member(int id, String host, int clientPort, int raftPort) {
        public String clientAddress() {
            return host + ":" + clientPort;
        }

        public InetSocketAddress raftAddress() {
            return new InetSocketAddress(host, raftPort);
        }
    }

    public static ClusterConfig parse(int selfId, String spec) {
        Map<Integer, Member> members = new LinkedHashMap<>();
        for (String part : spec.split(",")) {
            String[] idAndRest = part.trim().split("=");
            String[] fields = idAndRest.length == 2 ? idAndRest[1].split(":") : new String[0];
            if (fields.length != 3) {
                throw new IllegalArgumentException("cluster members look like id=host:clientPort:raftPort, got '" + part + "'");
            }
            try {
                int id = Integer.parseInt(idAndRest[0]);
                members.put(id, new Member(id, fields[0], Integer.parseInt(fields[1]), Integer.parseInt(fields[2])));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("bad number in cluster member '" + part + "'");
            }
        }
        if (!members.containsKey(selfId)) {
            throw new IllegalArgumentException("--node-id " + selfId + " is not in --cluster");
        }
        return new ClusterConfig(selfId, members);
    }

    public Member self() {
        return members.get(selfId);
    }

    public List<Integer> peerIds() {
        List<Integer> ids = new ArrayList<>(members.keySet());
        ids.remove(Integer.valueOf(selfId));
        return ids;
    }

    public Map<Integer, InetSocketAddress> peerAddresses() {
        Map<Integer, InetSocketAddress> peers = new LinkedHashMap<>();
        for (int id : peerIds()) peers.put(id, members.get(id).raftAddress());
        return peers;
    }
}
