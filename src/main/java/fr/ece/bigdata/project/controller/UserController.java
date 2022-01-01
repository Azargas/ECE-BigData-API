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
@RequestMapping("/user")
public class UserController {
    private final HBaseConfig hBaseConfig;

    @Autowired
    public UserController(HBaseConfig hBaseConfig) {
        this.hBaseConfig = hBaseConfig;
    }

    @PutMapping(value = "/", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public boolean create(@RequestBody HashMap<String, Object> body) {
        String name = body.getOrDefault("name", "").toString();
        String password = body.getOrDefault("password", "").toString();

        try {
            if (name.equals("")) throw new Exception("User name empty");
            if (password.equals("")) throw new Exception("User password empty");

            Connection connection = hBaseConfig.get();
            Table table = connection.getTable(TableName.valueOf("ece_2021_fall_app_2:TMusset"));

            // TODO Use last row or counter instead of random, for test purpose only
            int rand = (int) (Math.random() * 100);
            String id = "u" + rand;

            Put put = new Put(Bytes.toBytes(id));
            put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("name"), Bytes.toBytes(name));
            put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("password"), Bytes.toBytes(password));

            table.put(put);

            return true;
        }
        catch (IOException io) {
            System.err.println("Unable to add user in table");
            io.printStackTrace();
        }
        catch (Exception ex) {
            System.err.println("Invalid user");
        }
        return false;
    }

    @PatchMapping(value = "/{id}", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public boolean update(@PathVariable("id") int id, @RequestBody HashMap<String, Object> body) {
        String name = body.getOrDefault("name", "").toString();
        String password = body.getOrDefault("password", "").toString();

        try {
            if (name.equals("")) throw new Exception("User name empty");
            if (password.equals("")) throw new Exception("User password empty");
            if (id < 1) throw new Exception("Invalid user id");

            Connection connection = hBaseConfig.get();
            Table table = connection.getTable(TableName.valueOf("ece_2021_fall_app_2:TMusset"));

            Put put = new Put(Bytes.toBytes("u" + id));
            put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("name"), Bytes.toBytes(name));
            put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("password"), Bytes.toBytes(password));

            table.put(put);

            return true;
        }
        catch (IOException io) {
            System.err.println("Unable to update user in table");
            io.printStackTrace();
        }
        catch (Exception ex) {
            System.err.println("Invalid user");
        }
        return false;
    }

    @GetMapping(value = "/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
    public HashMap<String, Object> get(@PathVariable("id") int id) {
        try {
            if (id < 1) throw new Exception("Invalid user id");

            Connection connection = hBaseConfig.get();
            Table table = connection.getTable(TableName.valueOf("ece_2021_fall_app_2:TMusset"));

            Get get = new Get(Bytes.toBytes("u" + id));
            Result result = table.get(get);

            byte[] temp = result.getValue(Bytes.toBytes("user"), Bytes.toBytes("name"));
            String name = Bytes.toString(temp);

            HashMap<String, Object> response = new HashMap<>();
            response.put("name", name);
            response.put("password", "*");

            return response;
        }
        catch (IOException io) {
            System.err.println("Unable to get user in table");
            io.printStackTrace();
        }
        catch (Exception ex) {
            System.err.println("Invalid user");
        }
        return new HashMap<>();
    }

    @DeleteMapping(value = "/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
    public boolean delete(@PathVariable("id") int id) {
        try {
            if (id < 1) throw new Exception("Invalid user id");

            Connection connection = hBaseConfig.get();
            Table table = connection.getTable(TableName.valueOf("ece_2021_fall_app_2:TMusset"));

            Delete delete = new Delete(Bytes.toBytes("u" + id));
            table.delete(delete);

            return true;
        }
        catch (IOException io) {
            System.err.println("Unable to delete user in table");
            io.printStackTrace();
        }
        catch (Exception ex) {
            System.err.println("Invalid user");
        }
        return false;
    }
}
