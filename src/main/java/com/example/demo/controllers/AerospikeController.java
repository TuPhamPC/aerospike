package com.example.demo.controllers;

import com.example.demo.services.AerospikeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api")
public class AerospikeController {

    @Autowired
    private AerospikeService aerospikeService;

    @PostMapping("/write")
    public String writeData(@RequestParam String namespace, @RequestParam String set, @RequestParam String key, @RequestParam String bin, @RequestParam String value, @RequestParam(defaultValue = "0") int ttl) {
        aerospikeService.putData(namespace, set, key, bin, value, ttl);
        return "Data written successfully!";
    }

    @GetMapping("/read")
    public com.aerospike.client.Record readData(@RequestParam String namespace, @RequestParam String set, @RequestParam String key) {
        return aerospikeService.getData(namespace, set, key);
    }

    @PostMapping("/readMultiple")
    public List<com.aerospike.client.Record> readMultipleData(@RequestParam String namespace, @RequestParam String set, @RequestBody List<String> keys) {
        return aerospikeService.getMultipleData(namespace, set, keys);
    }
    @GetMapping("/query")
    public List<Map<String, Object>> queryRecords(
            @RequestParam String namespace,
            @RequestParam String set,
            @RequestParam String bin,
            @RequestParam String value) {

        return aerospikeService.executeQuery(namespace, set, bin, value);
    }
}
