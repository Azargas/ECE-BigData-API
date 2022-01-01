package fr.ece.bigdata.project.controller;

import fr.ece.bigdata.project.config.HBaseConfig;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

@RestController
public class AppController {
    private final HBaseConfig hBaseConfig;

    @Autowired
    public AppController(HBaseConfig hBaseConfig) {
        this.hBaseConfig = hBaseConfig;
    }

    @GetMapping(value = "/test")
    public String test() {
        try {
            Connection connection = hBaseConfig.get();
            Admin admin = connection.getAdmin();
            for (TableName t : admin.listTableNames())
                System.err.println(t.getNameAsString());
            return "Connection success";
        }
        catch (IOException io) {
            System.err.println("Unable to get user access");
            io.printStackTrace();
        }
        return "Connection failed";
    }
}
