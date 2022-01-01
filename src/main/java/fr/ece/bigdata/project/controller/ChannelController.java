package fr.ece.bigdata.project.controller;

import fr.ece.bigdata.project.config.HBaseConfig;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.HashMap;

@RestController
@RequestMapping("/channel")
public class ChannelController {
    private final HBaseConfig hBaseConfig;

    @Autowired
    public ChannelController(HBaseConfig hBaseConfig) {
        this.hBaseConfig = hBaseConfig;
    }

    @PutMapping(value = "/", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public boolean create(@RequestBody HashMap<String, Object> body) {
        String label = body.getOrDefault("label", "").toString();
        int user = (int) body.getOrDefault("user", 0);

        try {
            if (label.equals("")) throw new Exception("Channel label empty");
            if (user < 1) throw new Exception("Invalid channel author");

            Connection connection = hBaseConfig.get();
            Table table = connection.getTable(TableName.valueOf("ece_2021_fall_app_2:TMusset"));

            // TODO Use last row or counter instead of random, for test purpose only
            int rand = (int) (Math.random() * 100);
            String id = "u" + user + "-c" + rand;

            Put put = new Put(Bytes.toBytes("c" + rand));
            put.addColumn(Bytes.toBytes("channel"), Bytes.toBytes("label"), Bytes.toBytes(label));
            put.addColumn(Bytes.toBytes("channel"), Bytes.toBytes("author"), Bytes.toBytes(user));

            table.put(put);

            put = new Put(Bytes.toBytes(id));
            put.addColumn(Bytes.toBytes("channel"), Bytes.toBytes("label"), Bytes.toBytes(label));
            put.addColumn(Bytes.toBytes("channel"), Bytes.toBytes("author"), Bytes.toBytes(user));

            table.put(put);

            return true;
        }
        catch (IOException io) {
            System.err.println("Unable to add channel in table");
            io.printStackTrace();
        }
        catch (Exception ex) {
            System.err.println("Invalid channel");
        }
        return false;
    }

    @PatchMapping(value = "/{uid}-{cid}", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public boolean update(@PathVariable("uid") int uid, @PathVariable("cid") int cid, @RequestBody HashMap<String, Object> body) {
        String label = body.getOrDefault("label", "").toString();

        try {
            if (label.equals("")) throw new Exception("Channel label empty");
            if (uid < 1) throw new Exception("Invalid channel author");
            if (cid < 1) throw new Exception("Invalid channel id");

            Connection connection = hBaseConfig.get();
            Table table = connection.getTable(TableName.valueOf("ece_2021_fall_app_2:TMusset"));

            Put put = new Put(Bytes.toBytes("u" + uid + "-c" + cid));
            put.addColumn(Bytes.toBytes("channel"), Bytes.toBytes("label"), Bytes.toBytes(label));

            table.put(put);

            put = new Put(Bytes.toBytes("c" + cid));
            put.addColumn(Bytes.toBytes("channel"), Bytes.toBytes("label"), Bytes.toBytes(label));

            table.put(put);

            return true;
        }
        catch (IOException io) {
            System.err.println("Unable to update channel in table");
            io.printStackTrace();
        }
        catch (Exception ex) {
            System.err.println("Invalid channel");
        }
        return false;
    }

    @GetMapping(value = "/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
    public HashMap<String, Object> get(@PathVariable("id") int id) {
        try {
            if (id < 1) throw new Exception("Invalid channel id");

            Connection connection = hBaseConfig.get();
            Table table = connection.getTable(TableName.valueOf("ece_2021_fall_app_2:TMusset"));

            Get get = new Get(Bytes.toBytes("c" + id));
            Result result = table.get(get);

            byte[] temp = result.getValue(Bytes.toBytes("channel"), Bytes.toBytes("label"));
            String label = Bytes.toString(temp);

            temp = result.getValue(Bytes.toBytes("channel"), Bytes.toBytes("author"));
            String user = Bytes.toString(temp);

            HashMap<String, Object> response = new HashMap<>();
            response.put("label", label);
            response.put("user", user);

            return response;
        }
        catch (IOException io) {
            System.err.println("Unable to get channel in table");
            io.printStackTrace();
        }
        catch (Exception ex) {
            System.err.println("Invalid channel");
        }
        return new HashMap<>();
    }

    @DeleteMapping(value = "/{uid}-{cid}", produces = MediaType.APPLICATION_JSON_VALUE)
    public boolean delete(@PathVariable("uid") int uid, @PathVariable("cid") int cid) {
        try {
            if (uid < 1) throw new Exception("Invalid channel author");
            if (cid < 1) throw new Exception("Invalid channel id");

            Connection connection = hBaseConfig.get();
            Table table = connection.getTable(TableName.valueOf("ece_2021_fall_app_2:TMusset"));

            Delete delete = new Delete(Bytes.toBytes("u" + uid + "-c" + cid));
            table.delete(delete);

            delete = new Delete(Bytes.toBytes("c" + cid));
            table.delete(delete);

            return true;
        }
        catch (IOException io) {
            System.err.println("Unable to delete channel in table");
            io.printStackTrace();
        }
        catch (Exception ex) {
            System.err.println("Invalid channel");
        }
        return false;
    }
}
