/*   Copyright (C) 2013-2014 Computer Sciences Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. */
package ezbake.training;

import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.ArrayList;

import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TSimpleJSONProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ezbake.configuration.EzConfiguration;
import ezbake.data.common.ThriftClient;
import ezbake.thrift.ThriftUtils;
import ezbake.base.thrift.Visibility;
import ezbake.data.jdbc.EzJdbcDriver;
import ezbakehelpers.ezconfigurationhelpers.hive.HiveConfigurationHelper;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class HiveDatasetClient
{

    private static final Logger logger = LoggerFactory
            .getLogger(PostgresDatasetClient.class);

    private static HiveDatasetClient instance;    
    private Properties properties;

    private HiveDatasetClient()
    {
        createClient();
    }

    public static HiveDatasetClient getInstance()
    {
        if (instance == null)
        {
            instance = new HiveDatasetClient();
        }
        return instance;
    }

    public void close() throws Exception
    {
        ThriftClient.close();
    }

    public List<String> searchText(String searchText) throws TException,
                                                             SQLException
    {
        List<String> results = new ArrayList<String>();

        logger.info("Query hive for {}...", searchText);

        try
        {
              
            //use this helper for getting strings
            HiveConfigurationHelper helper = new HiveConfigurationHelper(this.properties);
            
            //get connection string
            String connectionString = helper.getConnectionString();
            
            //new up the ez driver
            EzJdbcDriver driver = new EzJdbcDriver();            
            
            //open a connection to hive
            try (Connection connection = driver.connect(connectionString, properties))                 
            {
                try (PreparedStatement ps = connection
                        .prepareStatement("select tweet, visibility from tweets where tweet like ?"))
                {
                    ps.setString(1, "%" + searchText + "%");
                    ResultSet rs = ps.executeQuery();
                    while (rs.next())
                    {
                        results.add(rs.getString("tweet")
                                    + ":"
                                    + ThriftUtils.deserializeFromBase64(
                                        Visibility.class,
                                        rs.getString("visibility")).getFormalVisibility());
                    }
                }
            }

            logger.info("Text search results: {}", results);
        }
        finally
        {
            assert true;
        }
        return results;
    }

    public void insertText(String text, String inputVisibility)
            throws TException, SQLException
    {

        try
        {
            //create a tweet
            Tweet tweet = new Tweet();
            tweet.setTimestamp(System.currentTimeMillis());
            tweet.setId(0);
            tweet.setText(text);
            tweet.setUserId(1);
            tweet.setUserName("test");
            tweet.setIsFavorite(new Random().nextBoolean());
            tweet.setIsRetweet(new Random().nextBoolean());

            //serialize object to JSON
            TSerializer serializer = new TSerializer(
                    new TSimpleJSONProtocol.Factory());
            String jsonContent = serializer.toString(tweet);

            //create a visibility for this tweet
            Visibility visibility = new Visibility();
            visibility.setFormalVisibility(inputVisibility);

            //use this helper for getting strings
            HiveConfigurationHelper helper = new HiveConfigurationHelper(this.properties);
            
            //get connection string
            String connectionString = helper.getConnectionString();
            
            //new up the ez driver
            EzJdbcDriver driver = new EzJdbcDriver();
            
            //open a connection to hive
            try (Connection connection = driver.connect(connectionString, properties))                 
            {
            
                try (PreparedStatement ps = connection
                        .prepareStatement("insert into table tweets values(?,?)"))
                {
                    ps.setString(1, jsonContent);
                    ps.setString(2, ThriftUtils.serializeToBase64(visibility));
                    ps.execute();
                }
            }
            logger.info("Successful hive client insert");
        }
        finally
        {
            assert true;
        }
    }

    private void createClient()
    {
        try
        {
            EzConfiguration configuration = new EzConfiguration();
            this.properties = configuration.getProperties();
            logger.info("in createClient, configuration: {}", properties);            

            //use this helper for getting strings
            HiveConfigurationHelper helper = new HiveConfigurationHelper(this.properties);
            
            //get connection string
            String connectionString = helper.getConnectionString();
            
            //new up the ez driver
            EzJdbcDriver driver = new EzJdbcDriver();
            
            //open a connection to hive
            try (Connection connection = driver.connect(connectionString, properties))                 
            {
                //drop and purge the table, then create it again.
                try (PreparedStatement ps = connection
                        .prepareStatement("DROP TABLE IF EXISTS tweets purge;"
                                          + "CREATE TABLE tweets(tweet String, visibility String default E'CwABAAAAAVUA')"))
                {
                    ps.execute();
                }
            }

        }
        catch (Exception ex)
        {
            ex.printStackTrace();
        }
    }

}
