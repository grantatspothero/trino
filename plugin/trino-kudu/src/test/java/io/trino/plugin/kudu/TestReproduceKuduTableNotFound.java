package io.trino.plugin.kudu;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduTable;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static java.lang.String.format;

public class TestReproduceKuduTableNotFound
{
    TestingKuduServer kuduServer;

    @BeforeClass
    public void init()
    {
        kuduServer = new TestingKuduServer();
    }

//    @Test(invocationCount = 4000, threadPoolSize = 4)
    @Test(invocationCount = 1000)
    public void testCreateAndOpenTable()
            throws KuduException
    {
        String kuduTableName = format("my_table_%s", Thread.currentThread().getId());
        KuduClient kuduClientOne = null;
        KuduClient kuduClientTwo = null;

        try {
            KuduClient.KuduClientBuilder builder = new KuduClient.KuduClientBuilder(kuduServer.getMasterAddress().toString());
            kuduClientOne = builder.build();
            kuduClientTwo = builder.build();

            // Create the table using kuduClientOne
            ColumnSchema columnSchema = new ColumnSchema.ColumnSchemaBuilder("field_one", Type.STRING)
                    .key(true)
                    .build();
            CreateTableOptions options = new CreateTableOptions();
            options.setWait(false);
            options.addHashPartitions(ImmutableList.of(columnSchema.getName()), 2);
            KuduTable kuduTableOne = kuduClientOne.createTable(kuduTableName, new Schema(ImmutableList.of(columnSchema)), options);
            System.out.println(kuduTableOne);

            // Now try to open the table using kuduClientTwo
            KuduTable kuduTableTwo = kuduClientTwo.openTable(kuduTableName);
            System.out.println(kuduTableTwo);
        }
        finally {
            if (kuduClientOne != null) {
                kuduClientOne.deleteTable(kuduTableName);
                kuduClientOne.close();
            }
            if (kuduClientTwo != null) {
                kuduClientTwo.close();
            }
        }
    }

}
