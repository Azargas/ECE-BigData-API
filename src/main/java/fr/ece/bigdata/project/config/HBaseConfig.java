package fr.ece.bigdata.project.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.security.UserGroupInformation;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.File;
import java.io.IOException;
import java.util.function.Supplier;

@Component
public class HBaseConfig implements Supplier<Connection> {
    private final File krb5LocationF = new File("./krb5.conf");
    @Value("classpath:conf/hbase-site.xml")
    private Resource hbaseSiteLocation;
    @Value("classpath:conf/core-site.xml")
    private Resource coreSiteLocation;
    private final File keytabLocationF = new File("./adaltas.keytab");
    @Value("${conf.hbase.principal}")
    private String principal;

    private Connection connection;

    @PostConstruct
    public void openHBaseConnection() {
        if (!krb5LocationF.exists()) System.err.println("krb5.conf not found");
        if (!keytabLocationF.exists()) System.err.println("adaltas.keytab not found");
        if (principal.equals("")) System.err.println("Principal is empty, please replace it in application.properties");
        try {
            System.setProperty("java.security.krb5.conf", krb5LocationF.getCanonicalPath());

            Configuration configuration = HBaseConfiguration.create();
            configuration.addResource(hbaseSiteLocation.getURI().toURL());
            configuration.addResource(coreSiteLocation.getURI().toURL());

            UserGroupInformation.setConfiguration(configuration);
            UserGroupInformation.loginUserFromKeytab(principal, keytabLocationF.toURI().getPath());
            connection = ConnectionFactory.createConnection(configuration);
            HBaseAdmin.available(configuration);
        }
        catch (IOException io) {
            System.err.println("Unable to initialize HBase connection, please check files location and keytab validity");
            io.printStackTrace();
        }
    }

    @PreDestroy
    public void closeHBaseConnection() {
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException io) {
                System.err.println("Unable to close HBase connection, it is open before closing ?");
                io.printStackTrace();
            }
        }
    }

    @Override
    public Connection get() {
        return connection;
    }
}
