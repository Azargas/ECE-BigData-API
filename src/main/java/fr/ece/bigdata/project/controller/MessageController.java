package fr.ece.bigdata.project.controller;

import fr.ece.bigdata.project.config.HBaseConfig;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

@RestController
@RequestMapping("/message")
public class MessageController {
    private final HBaseConfig hBaseConfig;

    @Autowired
    public MessageController(HBaseConfig hBaseConfig) {
        this.hBaseConfig = hBaseConfig;
    }

    @PutMapping(value = "/", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public boolean create(@RequestBody HashMap<String, Object> body) {
        String content = body.getOrDefault("content", "").toString();
        int user = (int) body.getOrDefault("user", 0);
        int channel = (int) body.getOrDefault("channel", 0);

        try {
            if (content.equals("")) throw new Exception("Message content empty");
            if (user < 1) throw new Exception("Invalid message author");
            if (channel < 1) throw new Exception("Invalid message channel");

            Connection connection = hBaseConfig.get();
            Table table = connection.getTable(TableName.valueOf("ece_2021_fall_app_2:TMusset"));

            // TODO Use last row or counter instead of random, for test purpose only
            int rand = (int) (Math.random() * 100);
            //String id = "u" + user + "-c" + channel + "-m" + rand;

            ArrayList<String> idList = new ArrayList<>();
            idList.add("m" + rand);
            idList.add("c" + channel + "-m" + rand);
            idList.add("u" + user + "-c" + channel + "-m" + rand);

            for(String id : idList) {
                Put put = new Put(Bytes.toBytes(id));
                put.addColumn(Bytes.toBytes("message"), Bytes.toBytes("content"), Bytes.toBytes(content));
                put.addColumn(Bytes.toBytes("message"), Bytes.toBytes("author"), Bytes.toBytes(user));
                put.addColumn(Bytes.toBytes("message"), Bytes.toBytes("channel"), Bytes.toBytes(channel));

                table.put(put);
            }

            return true;
        }
        catch (IOException io) {
            System.err.println("Unable to add message in table");
            io.printStackTrace();
        }
        catch (Exception ex) {
            System.err.println("Invalid message");
        }
        return false;
    }

    @PatchMapping(value = "/{uid}-{cid}-{mid}", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public boolean update(@PathVariable("uid") int uid, @PathVariable("cid") int cid, @PathVariable("mid") int mid, @RequestBody HashMap<String, Object> body) {
        String content = body.getOrDefault("content", "").toString();

        try {
            if (content.equals("")) throw new Exception("Message content empty");
            if (uid < 1) throw new Exception("Invalid message author");
            if (cid < 1) throw new Exception("Invalid message channel");
            if (mid < 1) throw new Exception("Invalid message id");

            Connection connection = hBaseConfig.get();
            Table table = connection.getTable(TableName.valueOf("ece_2021_fall_app_2:TMusset"));

            ArrayList<String> idList = new ArrayList<>();
            idList.add("m" + mid);
            idList.add("c" + cid + "-m" + mid);
            idList.add("u" + uid + "-c" + cid + "-m" + mid);

            for(String id : idList) {
                Put put = new Put(Bytes.toBytes(id));
                put.addColumn(Bytes.toBytes("message"), Bytes.toBytes("content"), Bytes.toBytes(content));
                table.put(put);
            }

            return true;
        }
        catch (IOException io) {
            System.err.println("Unable to update message in table");
            io.printStackTrace();
        }
        catch (Exception ex) {
            System.err.println("Invalid message");
        }
        return false;
    }

    @GetMapping(value = "/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
    public HashMap<String, Object> get(@PathVariable("id") int id) {
        try {
            if (id < 1) throw new Exception("Invalid message id");

            Connection connection = hBaseConfig.get();
            Table table = connection.getTable(TableName.valueOf("ece_2021_fall_app_2:TMusset"));

            Get get = new Get(Bytes.toBytes("m" + id));
            Result result = table.get(get);

            byte[] temp = result.getValue(Bytes.toBytes("message"), Bytes.toBytes("content"));
            String content = Bytes.toString(temp);

            temp = result.getValue(Bytes.toBytes("message"), Bytes.toBytes("author"));
            String user = Bytes.toString(temp);

            temp = result.getValue(Bytes.toBytes("message"), Bytes.toBytes("channel"));
            String channel = Bytes.toString(temp);

            HashMap<String, Object> response = new HashMap<>();
            response.put("content", content);
            response.put("user", user);
            response.put("channel", channel);

            return response;
        }
        catch (IOException io) {
            System.err.println("Unable to get message in table");
            io.printStackTrace();
        }
        catch (Exception ex) {
            System.err.println("Invalid message");
        }
        return new HashMap<>();
    }

    @DeleteMapping(value = "/{uid}-{cid}-{mid}", produces = MediaType.APPLICATION_JSON_VALUE)
    public boolean delete(@PathVariable("uid") int uid, @PathVariable("cid") int cid, @PathVariable("mid") int mid) {
        try {
            if (uid < 1) throw new Exception("Invalid message author");
            if (cid < 1) throw new Exception("Invalid message channel");
            if (mid < 1) throw new Exception("Invalid message id");

            Connection connection = hBaseConfig.get();
            Table table = connection.getTable(TableName.valueOf("ece_2021_fall_app_2:TMusset"));

            ArrayList<String> idList = new ArrayList<>();
            idList.add("m" + mid);
            idList.add("c" + cid + "-m" + mid);
            idList.add("u" + uid + "-c" + cid + "-m" + mid);

            for(String id : idList) {
                Delete delete = new Delete(Bytes.toBytes(id));
                table.delete(delete);
            }

            return true;
        }
        catch (IOException io) {
            System.err.println("Unable to delete message in table");
            io.printStackTrace();
        }
        catch (Exception ex) {
            System.err.println("Invalid message");
        }
        return false;
    }
}
