package com.example.demo.services;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Bin;
import com.aerospike.client.Record;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

@Service
public class AerospikeService {

    @Autowired
    private AerospikeClient client;

    public void putData(String namespace, String set, String keyStr, String binName, String value, int ttl) {
        Key key = new Key(namespace, set, keyStr);
        Bin bin = new Bin(binName, value);
        WritePolicy policy = new WritePolicy();
        policy.expiration = ttl;
        client.put(policy, key, bin);
    }

    public Record getData(String namespace, String set, String keyStr) {
        Key key = new Key(namespace, set, keyStr);
        return client.get(null, key);
    }

    public List<Record> getMultipleData(String namespace, String set, List<String> keys) {
        List<Key> keyList = keys.stream().map(keyStr -> new Key(namespace, set, keyStr)).collect(Collectors.toList());
        Key[] keyArray = keyList.toArray(new Key[0]);
        Record[] records = client.get(null, keyArray);
        return Arrays.asList(records);
    }

    public List<Map<String, Object>> executeQuery(String namespace, String set, String binName, String value) {
        QueryPolicy queryPolicy = new QueryPolicy();
        Statement stmt = new Statement();
        stmt.setNamespace(namespace);
        stmt.setSetName(set);
        stmt.setFilter(Filter.equal(binName, value));

        List<Map<String, Object>> result = new ArrayList<>();
        try (RecordSet rs = client.query(queryPolicy, stmt)) {
            while (rs.next()) {
                Record record = rs.getRecord();
                Map<String, Object> dataMap = new HashMap<>();
                dataMap.put("bins", record.bins);
                dataMap.put("generation", record.generation);
                dataMap.put("expiration", record.expiration);
                result.add(dataMap);
            }
        }
        return result;
    }

}
