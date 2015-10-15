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
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.thrift.ThriftUtils;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.Visibility;
import ezbakehelpers.ezconfigurationhelpers.postgres.PostgresConfigurationHelper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

//import java.sql.SQLException;

public class PostgresDatasetClient {
	private static final Logger logger = LoggerFactory
			.getLogger(PostgresDatasetClient.class);

	private static PostgresDatasetClient instance;
	private EzbakeSecurityClient securityClient;
	private Properties properties;

	private PostgresDatasetClient() {
		createClient();
	}

	public static PostgresDatasetClient getInstance() {
		if (instance == null) {
			instance = new PostgresDatasetClient();
		}
		return instance;
	}

	public void close() throws Exception {
		ThriftClient.close();
	}

	public List<String> searchText(String searchText) throws TException,
			SQLException {
		List<String> results = new ArrayList<String>();

		logger.info("Query postgres for {}...", searchText);

		try {
			EzSecurityToken token = securityClient.fetchTokenForProxiedUser();
			PostgresConfigurationHelper helper = new PostgresConfigurationHelper(
					this.properties);
			try (Connection connection = helper.getEzPostgresConnection(token)) {
				try (PreparedStatement ps = connection
						.prepareStatement("select tweet, visibility from tweets where tweet like ?")) {
					ps.setString(1, "%" + searchText + "%");
					ResultSet rs = ps.executeQuery();
					while (rs.next()) {
						results.add(rs.getString("tweet")
								+ ":"
								+ ThriftUtils.deserializeFromBase64(
										Visibility.class,
										rs.getString("visibility")).getFormalVisibility());
					}
				}
			}

			logger.info("Text search results: {}", results);
		} finally {
			assert true;
		}
		return results;
	}

	public void insertText(String text, String inputVisibility)
			throws TException, SQLException {

		try {
			EzSecurityToken token = securityClient.fetchTokenForProxiedUser();

			Tweet tweet = new Tweet();
			tweet.setTimestamp(System.currentTimeMillis());
			tweet.setId(0);
			tweet.setText(text);
			tweet.setUserId(1);
			tweet.setUserName("test");
			tweet.setIsFavorite(new Random().nextBoolean());
			tweet.setIsRetweet(new Random().nextBoolean());

			TSerializer serializer = new TSerializer(
					new TSimpleJSONProtocol.Factory());
			String jsonContent = serializer.toString(tweet);

			PostgresConfigurationHelper helper = new PostgresConfigurationHelper(
					this.properties);
			Visibility visibility = new Visibility();
			visibility.setFormalVisibility(inputVisibility);

			try (Connection connection = helper.getEzPostgresConnection(token)) {
				try (PreparedStatement ps = connection
						.prepareStatement("insert into tweets(tweet, visibility) values(?,?)")) {
					ps.setString(1, jsonContent);
					ps.setString(2, ThriftUtils.serializeToBase64(visibility));
					ps.execute();
				}
			}
			logger.info("Successful postgres client insert");
		} finally {
			assert true;
		}
	}

	private void createClient() {
		try {
			EzConfiguration configuration = new EzConfiguration();
			this.properties = configuration.getProperties();
			logger.info("in createClient, configuration: {}", properties);
			this.securityClient = new EzbakeSecurityClient(properties);

			EzSecurityToken token = securityClient.fetchTokenForProxiedUser();
			PostgresConfigurationHelper helper = new PostgresConfigurationHelper(
					this.properties);
			try (Connection connection = helper.getEzPostgresConnection(token)) {
				try (PreparedStatement ps = connection
						.prepareStatement("DROP TABLE IF EXISTS tweets;"
								+ "CREATE TABLE tweets(id SERIAL, tweet TEXT, visibility varchar(32768) default E'CwABAAAAAVUA')")) {
					ps.execute();
				}
			}

		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

}
