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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ezbake.configuration.EzConfiguration;
import ezbake.data.common.ThriftClient;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.thrift.ThriftUtils;

public class PostgresDatasetClient {
	private static final Logger logger = LoggerFactory
			.getLogger(PostgresDatasetClient.class);

	private static PostgresDatasetClient instance;
	private EzbakeSecurityClient securityClient;

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

	public List<String> searchText(String searchText) throws TException {
		List<String> results = new ArrayList<String>();

		try {
			EzSecurityToken token = securityClient.fetchTokenForProxiedUser();
			String serToken = ThriftUtils.serializeToBase64(token);
			System.out.println("{" + serToken + "}");

			logger.info("Query postgres for {}...", searchText);
			logger.info("Text search results: {}", results);
		} finally {
			assert true;
		}
		return results;
	}

	public void insertText(String text) throws TException {

		try {
			EzSecurityToken token = securityClient.fetchTokenForProxiedUser();
			String serToken = ThriftUtils.serializeToBase64(token);
			System.out.println("{" + serToken + "}");

			Tweet tweet = new Tweet();
			tweet.setTimestamp(System.currentTimeMillis());
			tweet.setId(0);
			tweet.setText(text);
			tweet.setUserId(1);
			tweet.setUserName("test");
			tweet.setIsFavorite(new Random().nextBoolean());
			tweet.setIsRetweet(new Random().nextBoolean());

			logger.info("Successful postgres client insert");
		} finally {
			assert true;
		}
	}

	private void createClient() {
		try {
			EzConfiguration configuration = new EzConfiguration();
			Properties properties = configuration.getProperties();
			logger.info("in createClient, configuration: {}", properties);
			securityClient = new EzbakeSecurityClient(properties);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

}
