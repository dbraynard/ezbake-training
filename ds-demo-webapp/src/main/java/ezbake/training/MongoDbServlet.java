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

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
//import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

//import ezbake.configuration.EzConfiguration;
import ezbake.data.mongo.redact.RedactHelper;
//import ezbake.security.client.EzbakeSecurityClient;
//import ezbake.thrift.ThriftClientPool;

public class MongoDbServlet extends HttpServlet {
    public static final String COLLECTION_NAME = "ezmongo_demo";
    public static final String USER_FIELD_NAME = "user";
    public static final String USERNAME_FIELD_NAME = "username";
    public static final String SCREEN_NAME_FIELD_NAME = "screenName";
    public static final String SECONDARY_SCREEN_NAME_FIELD_NAME = "screen_name";
    public static final String TEXT_FIELD_NAME = "text";

    private static final long serialVersionUID = 9051600090960237717L;

    protected static Logger logger = LoggerFactory.getLogger(MongoDbServlet.class);

    //private static EzbakeSecurityClient securityClient;
    //private ThriftClientPool pool;

    public void destroy() {
        try {
            MongoDatasetClient.getInstance().close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void init() throws ServletException {
        try {
            MongoDatasetClient client = MongoDatasetClient.getInstance();

            logger.info("Initializing mongo db servlet, COLLECTION_NAME: {}", COLLECTION_NAME);

            // if collection doesn't exist, create it.
            if (!client.collectionExists(COLLECTION_NAME)) {
                logger.info("collection doesn't exist, we need to create the collection.");
                client.createCollection(COLLECTION_NAME);
            }

            createTextIndex(client);

            //final Properties props = new EzConfiguration().getProperties();
            //this.pool = new ThriftClientPool(props);
            //this.securityClient = new EzbakeSecurityClient(props);
        } catch (Exception e) {
            logger.error("Error during initialization", e);
            throw new ServletException(e.getMessage());
        }
    }

    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        doPost(request, response);
    }

    public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
        String action = request.getParameter("action");

        String result;
        if ("insertText".equalsIgnoreCase(action)) {
            result = insertText(request, response);
        } else if ("searchEzMongo".equalsIgnoreCase(action)) {
            result = searchEzMongo(request, response);
        } else {
            result = "Unknown action: " + action;
        }

        response.setHeader("Content-Type", "text/html;charset=UTF-8");
        PrintWriter out = response.getWriter();
        out.println(result);
    }

    /**
     * Creates a text index on the "text" field if it doesn't exist
     *
     * @throws TException
     */
    private void createTextIndex(MongoDatasetClient client) throws TException {
        boolean hasTextIndex = false;
        String namespace = null;

        logger.info("getting index info..");

        List<String> indexList = client.getIndexInfo(COLLECTION_NAME);
        for (String index : indexList) {
            logger.info("we have an index: {}", index);

            DBObject indexObj = (DBObject) JSON.parse(index);
            String indexName = (String) indexObj.get("name");
            if (namespace == null) {
                namespace = (String) indexObj.get("ns");
            }

            if (indexName.equals(TEXT_FIELD_NAME + "_text")) {
                hasTextIndex = true;
            }
        }

        if (!hasTextIndex) {
            DBObject obj = new BasicDBObject();
            // we are putting a text index on the "text" field in the mongo collection
            obj.put(TEXT_FIELD_NAME, "text");
            String jsonKeys = JSON.serialize(obj);

            logger.info("creating text index with jsonKeys: {}, COLLECTION_NAME: {}", jsonKeys, COLLECTION_NAME);

            client.createIndex(COLLECTION_NAME, jsonKeys, null);

            logger.info("MongoDbServlet: created text index: {}", jsonKeys);
        } else {
            logger.info("MongoDbServlet: we already have the text index.");
        }
    }

    private String searchEzMongo(HttpServletRequest request, HttpServletResponse response) {
        String searchText = request.getParameter("searchText");
        String result;

        try {
            MongoDatasetClient client = MongoDatasetClient.getInstance();
            createTextIndex(client);

            logger.info("searchText: {}", searchText);

            List<String> data = client.searchText(COLLECTION_NAME, searchText);

            if (data.size() == 0) {
                result = "No results found.";
            } else {
                StringBuilder buffer = new StringBuilder();
                for (String recordJSON : data) {
                    buffer.append("<tr>");

                    DBObject dbObj = (DBObject) JSON.parse(recordJSON);
                    String _id = dbObj.get("_id").toString();
                    String id = null;
                    Object idObj = dbObj.get("id");
                    if (idObj != null && (idObj instanceof Long || idObj instanceof Integer)) {
                        id = idObj.toString();
                    }

                    DBObject formalVisibilityObj = (DBObject) dbObj.get(RedactHelper.FORMAL_VISIBILITY_FIELD);
                    String formalVisibility = null;
                    if (formalVisibilityObj != null) {
                        formalVisibility = formalVisibilityObj.toString();
                    }

                    DBObject userObj = (DBObject) dbObj.get(USER_FIELD_NAME);
                    String userName;
                    if (userObj != null) {
                        userName = (String) userObj.get(SCREEN_NAME_FIELD_NAME);
                        if (userName == null) {
                            // it's possible that the tweets were ingested with a
                            // different field name for the screen name.
                            userName = (String) userObj.get(SECONDARY_SCREEN_NAME_FIELD_NAME);
                        }
                    } else { // try a different field name for the username
                        userName = (String) dbObj.get(USERNAME_FIELD_NAME);
                    }

                    String context = (String) dbObj.get(TEXT_FIELD_NAME);

                    // construct the columns to display on the jsp
                    buffer.append("<td>");
                    buffer.append(_id);
                    buffer.append("</td>");
                    buffer.append("<td>");
                    buffer.append(id);
                    buffer.append("</td>");
                    buffer.append("<td>");
                    buffer.append(formalVisibility);
                    buffer.append("</td>");
                    buffer.append("<td>");
                    buffer.append("@");
                    buffer.append(userName);
                    buffer.append(": ");
                    buffer.append(context);
                    buffer.append("</td>");

                    buffer.append("</tr>");
                }

                result = buffer.toString();
                logger.info(result);
            }
        } catch (Exception e) {
            result = "Unable to retrieve any results: " + e.getMessage();
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }

        return result;
    }

    private String insertText(HttpServletRequest request, HttpServletResponse response) {
        String textContent = request.getParameter("Content");
        String result = null;
        try {
            logger.info("Inserted text into EzMongo");
            result = "Successfully added the text(id=" + textContent + ")";
        } catch (Exception e) {
            result = "Failed to insert data: " + e.getMessage();
            logger.error("Failed to insert data", e);
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
//        } finally {
//            //pool.returnToPool(client);
        }

        return result;
    }

}
