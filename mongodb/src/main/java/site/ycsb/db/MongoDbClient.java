/**
 * Copyright (c) 2012 - 2015 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

/*
 * MongoDB client binding for YCSB.
 *
 * Submitted by Yen Pai on 5/11/2010.
 *
 * https://gist.github.com/000a66b8db2caf42467b#file_mongo_database.java
 */
package site.ycsb.db;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.db.DataModel.DocumentGenerator;

import org.bson.Document;
import org.bson.types.Binary;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * MongoDB binding for YCSB framework using the MongoDB Inc.
 * <a href="http://docs.mongodb.org/ecosystem/drivers/java/">driver</a>
 * <p>
 * See the <code>README.md</code> for configuration information.
 * </p>
 * 
 * @author ypai
 * @see <a href="http://docs.mongodb.org/ecosystem/drivers/java/">MongoDB Inc.
 *      driver</a>
 */
public class MongoDbClient extends DB {

	/** Used to include a field in a response. */
	private static final Integer INCLUDE = Integer.valueOf(1);

	/** The options to use for inserting many documents. */
	private static final InsertManyOptions INSERT_UNORDERED = new InsertManyOptions().ordered(false);

	/** The options to use for inserting a single document. */
	private static final UpdateOptions UPDATE_WITH_UPSERT = new UpdateOptions().upsert(true);

	/**
	 * The database name to access.
	 */
	private static String databaseName;

	/** The database name to access. */
	private static MongoDatabase database;

	/**
	 * Count the number of times initialized to teardown on the last
	 * {@link #cleanup()}.
	 */
	private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

	/**
	 * Singleton data generator instance
	 **/
	private final DocumentGenerator dataGen = DocumentGenerator.getInstance();

	/** A singleton Mongo instance. */
	private static MongoClient mongoClient;

	/** The default read preference for the test. */
	private static ReadPreference readPreference;

	/** The default write concern for the test. */
	private static WriteConcern writeConcern;

	/** The batch size to use for inserts. */
	private static int batchSize;

	/** If true then use updates with the upsert option for inserts. */
	private static boolean useUpsert;

	/** The bulk inserts pending for the thread. */
	private final List<Document> bulkInserts = new ArrayList<Document>();

	/**
	 * Cleanup any state for this DB. Called once per DB instance; there is one DB
	 * instance per client thread.
	 */
	@Override
	public void cleanup() throws DBException {
		if (INIT_COUNT.decrementAndGet() == 0) {
			try {
				mongoClient.close();
			} catch (Exception e1) {
				System.err.println("Could not close MongoDB connection pool: " + e1.toString());
				e1.printStackTrace();
				return;
			} finally {
				database = null;
				mongoClient = null;
			}
		}
	}

	/**
	 * Delete a record from the database.
	 * 
	 * @param table The name of the table
	 * @param key   The record key of the record to delete.
	 * @return Zero on success, a non-zero error code on error. See the {@link DB}
	 *         class's description for a discussion of error codes.
	 */
	@Override
	public Status delete(String table, String key) {
		try {
			MongoCollection<Document> collection = database.getCollection(table);

			Document query = new Document("_id", key);
			DeleteResult result = collection.withWriteConcern(writeConcern).deleteOne(query);
			if (result.wasAcknowledged() && result.getDeletedCount() == 0) {
				System.err.println("Nothing deleted for key " + key);
				return Status.NOT_FOUND;
			}
			return Status.OK;
		} catch (Exception e) {
			System.err.println(e.toString());
			return Status.ERROR;
		}
	}

	/**
	 * Initialize any state for this DB. Called once per DB instance; there is one
	 * DB instance per client thread.
	 */
	@Override
	public void init() throws DBException {
		INIT_COUNT.incrementAndGet();
		synchronized (INCLUDE) {
			if (mongoClient != null) {
				return;
			}

			Properties props = getProperties();

			// Set insert batchsize, default 1 - to be YCSB-original equivalent
			batchSize = Integer.parseInt(props.getProperty("batchsize", "1"));

			// Set is inserts are done as upserts. Defaults to false.
			useUpsert = Boolean.parseBoolean(props.getProperty("mongodb.upsert", "false"));

			// Just use the standard connection format URL
			// http://docs.mongodb.org/manual/reference/connection-string/
			// to configure the client.
			String url = props.getProperty("mongodb.url", null);
			boolean defaultedUrl = false;
			if (url == null) {
				defaultedUrl = true;
				url = "mongodb://localhost:27017/ycsb?w=1";
			}

			url = OptionsSupport.updateUrl(url, props);

			if (!url.startsWith("mongodb://") && !url.startsWith("mongodb+srv://")) {
				System.err.println("ERROR: Invalid URL: '" + url + "'. Must be of the form "
						+ "'mongodb://<host1>:<port1>,<host2>:<port2>/database?options' "
						+ "or 'mongodb+srv://<host>/database?options'. "
						+ "http://docs.mongodb.org/manual/reference/connection-string/");
				System.exit(1);
			}

			try {
				MongoClientURI uri = new MongoClientURI(url);

				String uriDb = uri.getDatabase();
				if (!defaultedUrl && (uriDb != null) && !uriDb.isEmpty() && !"admin".equals(uriDb)) {
					databaseName = uriDb;
				} else {
					// If no database is specified in URI, use "ycsb"
					databaseName = "ycsb";

				}

				readPreference = uri.getOptions().getReadPreference();
				writeConcern = uri.getOptions().getWriteConcern();

				mongoClient = new MongoClient(uri);
				database = mongoClient.getDatabase(databaseName).withReadPreference(readPreference)
						.withWriteConcern(writeConcern);

				System.out.println("mongo client connection created with " + url);
			} catch (Exception e1) {
				System.err.println("Could not initialize MongoDB connection pool for Loader: " + e1.toString());
				e1.printStackTrace();
				return;
			}
		}
	}

	/**
	 * Insert a record in the database. Any field/value pairs in the specified
	 * values HashMap will be written into the record with the specified record key.
	 * 
	 * @param table  The name of the table
	 * @param key    The record key of the record to insert.
	 * @param values A HashMap of field/value pairs to insert in the record
	 * @return Zero on success, a non-zero error code on error. See the {@link DB}
	 *         class's description for a discussion of error codes.
	 */
	@Override
	public Status insert(String table, String key, Map<String, ByteIterator> values) {
		try {
			Map<String, Set<Document>> recordsPerCollection = dataGen.createRecords();
			
			for(String collectionName: recordsPerCollection.keySet()) {
				MongoCollection<Document> collection = database.getCollection(collectionName);
				
				for(Document recordDocument: recordsPerCollection.get(collectionName)) {
					collection.insertOne(recordDocument);
				}
			}
			
			return Status.OK;

		} catch (Exception e) {
			System.err.println("Exception while trying bulk insert with " + bulkInserts.size());
			e.printStackTrace();
			return Status.ERROR;
		}

	}

	/*
	 * -------------------------------- Rubis generator elements ------------/
	 */

	Random rand = new Random();
	int getItemDescriptionLength = 100;
	float getPercentReservePrice = 10;
	float getPercentBuyNow = 10;
	float getPercentUniqueItems = 10;
	int getMaxItemQty = 10;
	int getCommentMaxLength = 200;
	int getNbOfCategories = 10;
	int getNbOfUsers = 10000;
	int getMaxBidsPerItem = 10;
	int getNbOfRegions = 5;
	int totalItems = 50000;
	// String[] regions= ['test'];

	/**
	 * This method add users to the database according to the parameters given in
	 * the database.properties file.
	 */
	public void generateUsers() {
		String firstname;
		String lastname;
		String nickname;
		String email;
		String password;
		String regionName;
		int i;
		int regionNameId;

		System.out.print("Generating " + getNbOfUsers + " users ");
		for (i = 0; i < getNbOfUsers; i++) {
			firstname = "Great" + (i + 1);
			lastname = "User" + (i + 1);
			nickname = "user" + (i + 1);
			email = firstname + "." + lastname + "@rubis.com";
			password = "password" + (i + 1);
			// regionName = (String) rubis.getRegions().elementAt(i % getNbOfRegions);
			regionNameId = i % getNbOfRegions;
		}
		System.out.println("Done!");
	}

	/**
	 * This method add items to the database according to the parameters given in
	 * the database.properties file.
	 */
	public void generateItems(boolean generateBids, boolean generateComments) {
		// Items specific variables
		String name;
		String description;
		float initialPrice;
		float reservePrice;
		float buyNow;
		int duration;
		int quantity;
		int categoryId;
		int sellerId;

		String staticDescription = "This incredible item is exactly what you need !<br>It has a lot of very nice features including "
				+ "a coffee option.<p>It comes with a free license for the free RUBiS software, that's really cool. But RUBiS even if it "
				+ "is free, is <B>(C) Rice University/INRIA 2001</B>. It is really hard to write an interesting generic description for "
				+ "automatically generated items, but who will really read this ?<p>You can also check some cool software available on "
				+ "http://sci-serv.inrialpes.fr. There is a very cool DSM system called SciFS for SCI clusters, but you will need some "
				+ "SCI adapters to be able to run it ! Else you can still try CART, the amazing 'Cluster Administration and Reservation "
				+ "Tool'. All those software are open source, so don't hesitate ! If you have a SCI Cluster you can also try the Whoops! "
				+ "clustered web server. Actually Whoops! stands for something ! Yes, it is a Web cache with tcp Handoff, On the fly "
				+ "cOmpression, parallel Pull-based lru for Sci clusters !! Ok, that was a lot of fun but now it is starting to be quite late "
				+ "and I'll have to go to bed very soon, so I think if you need more information, just go on <h1>http://sci-serv.inrialpes.fr</h1> "
				+ "or you can even try http://www.cs.rice.edu and try to find where Emmanuel Cecchet or Julie Marguerite are and you will "
				+ "maybe get fresh news about all that !!<p>";

		// Comments specific variables
		int staticDescriptionLength = staticDescription.length();
		String[] staticComment = { "This is a very bad comment. Stay away from this seller !!<p>",
				"This is a comment below average. I don't recommend this user !!<p>",
				"This is a neutral comment. It is neither a good or a bad seller !!<p>",
				"This is a comment above average. You can trust this seller even if it is not the best deal !!<p>",
				"This is an excellent comment. You can make really great deals with this seller !!<p>" };
		int[] staticCommentLength = { staticComment[0].length(), staticComment[1].length(), staticComment[2].length(),
				staticComment[3].length(), staticComment[4].length() };
		int[] ratingValue = { -5, -3, 0, 3, 5 };
		int rating;
		String comment;

		// Bids specific variables
		int nbBids;

		// All purpose variables
		int i, j;
		String HTTPreply;

		// Cache variables
		int BATCH_SIZE = 1000;

		try {
			for (i = 0; i < totalItems; i++) {
				// Generate the item
				name = "RUBiS automatically generated item #" + (i + 1);
				int descriptionLength = rand.nextInt(getItemDescriptionLength) + 1;
				description = "";
				while (staticDescriptionLength < descriptionLength) {
					description = description + staticDescription;
					descriptionLength -= staticDescriptionLength;
				}
				description += staticDescription.substring(0, descriptionLength);
				initialPrice = rand.nextInt(5000) + 1;
				duration = rand.nextInt(7) + 1;

//              categoryId = i % getNbOfCategories;
//              // Hopefully everything is ok and we will not have a deadlock here
//              while (itemsPerCategory[categoryId] == 0)
//                  categoryId = (categoryId + 1) % getNbOfCategories;
//              if (i >= oldItems)
//                  itemsPerCategory[categoryId]--;
				sellerId = rand.nextInt(getNbOfUsers) + 1;

				Calendar now = Calendar.getInstance();

				String start = new SimpleDateFormat("yyyy:MM:dd HH:mm:ss").format(now.getTime());
				now.add(Calendar.DATE, duration);
				String end = new SimpleDateFormat("yyyy:MM:dd HH:mm:ss").format(now.getTime());
//              ps.setInt(1, i+1);
//              ps.setString(2, name);
//              ps.setString(3, description);
//              ps.setFloat(4, initialPrice);
//              ps.setInt(5, quantity);
//              ps.setFloat(6, reservePrice);
//              ps.setFloat(7, buyNow);
//
//              ps.setString(10, start);
//              ps.setString(11, end);
//
//              ps.setInt(12, sellerId);
//              ps.setInt(13, categoryId + 1);

				nbBids = 0;
				float maxBid = 0;
				if (generateBids) { // Now deal with the bids
					nbBids = rand.nextInt(getMaxBidsPerItem);
					for (j = 0; j < nbBids; j++) {
						int addBid = rand.nextInt(10) + 1;

						int itemId = i + 1;
						int userId = rand.nextInt(getNbOfUsers) + 1;
						float bid = initialPrice + addBid;
						maxBid = Math.max(maxBid, bid);
//
//                      ps_bids.setInt(1, userId);
//                      ps_bids.setInt(2, itemId);
//                      ps_bids.setInt(3, rand.nextInt(quantity) + 1); //qty
//                      ps_bids.setFloat(4, bid); //bid
//                      ps_bids.setFloat(5, maxBid);
//
//                      ps_bids.addBatch();

						initialPrice += addBid; // We use initialPrice as minimum bid
					}
				}

//              ps.setInt(8, nbBids);
//              ps.setFloat(9, maxBid);
//              ps.addBatch();

				if (generateComments) { // Generate the comment
					rating = rand.nextInt(5);
					int commentLength = rand.nextInt(getCommentMaxLength) + 1;
					comment = "";
					while (staticCommentLength[rating] < commentLength) {
						comment = comment + staticComment[rating];
						commentLength -= staticCommentLength[rating];
					}
					comment += staticComment[rating].substring(0, commentLength);

					int itemId = i + 1;
					int userId = rand.nextInt(getNbOfUsers) + 1;

//                  ps_comments.setInt(1, userId);
//                  ps_comments.setInt(2, sellerId);
//                  ps_comments.setInt(3, itemId);
//                  ps_comments.setInt(4, ratingValue[rating]);
//                  ps_comments.setString(5, comment);
//                  ps_comments.addBatch();
//
//                  ps_user_update.setInt(1, ratingValue[rating]);
//                  ps_user_update.setInt(2, sellerId);
//                  ps_user_update.addBatch();
				}
			}
		} catch (Exception e) {
			System.err.println("Error while generating items: " + e.getMessage());
		}
		System.out.println(" Done!");
	}

	/**
	 * Read a record from the database. Each field/value pair from the result will
	 * be stored in a HashMap.
	 * 
	 * @param table  The name of the table
	 * @param key    The record key of the record to read.
	 * @param fields The list of fields to read, or null for all of them
	 * @param result A HashMap of field/value pairs for the result
	 * @return Zero on success, a non-zero error code on error or "not found".
	 */
	@Override
	public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
		try {
			MongoCollection<Document> collection = database.getCollection(table);
			Document query = new Document("_id", key);

			FindIterable<Document> findIterable = collection.find(query);

			if (fields != null) {
				Document projection = new Document();
				for (String field : fields) {
					projection.put(field, INCLUDE);
				}
				findIterable.projection(projection);
			}

			Document queryResult = findIterable.first();

			if (queryResult != null) {
				fillMap(result, queryResult);
			}
			return queryResult != null ? Status.OK : Status.NOT_FOUND;
		} catch (Exception e) {
			System.err.println(e.toString());
			return Status.ERROR;
		}
	}

	/**
	 * Perform a range scan for a set of records in the database. Each field/value
	 * pair from the result will be stored in a HashMap.
	 * 
	 * @param table       The name of the table
	 * @param startkey    The record key of the first record to read.
	 * @param recordcount The number of records to read
	 * @param fields      The list of fields to read, or null for all of them
	 * @param result      A Vector of HashMaps, where each HashMap is a set
	 *                    field/value pairs for one record
	 * @return Zero on success, a non-zero error code on error. See the {@link DB}
	 *         class's description for a discussion of error codes.
	 */
	@Override
	public Status scan(String table, String startkey, int recordcount, Set<String> fields,
			Vector<HashMap<String, ByteIterator>> result) {
		MongoCursor<Document> cursor = null;
		try {
			MongoCollection<Document> collection = database.getCollection(table);

			Document scanRange = new Document("$gte", startkey);
			Document query = new Document("_id", scanRange);
			Document sort = new Document("_id", INCLUDE);

			FindIterable<Document> findIterable = collection.find(query).sort(sort).limit(recordcount);

			if (fields != null) {
				Document projection = new Document();
				for (String fieldName : fields) {
					projection.put(fieldName, INCLUDE);
				}
				findIterable.projection(projection);
			}

			cursor = findIterable.iterator();

			if (!cursor.hasNext()) {
				System.err.println("Nothing found in scan for key " + startkey);
				return Status.ERROR;
			}

			result.ensureCapacity(recordcount);

			while (cursor.hasNext()) {
				HashMap<String, ByteIterator> resultMap = new HashMap<String, ByteIterator>();

				Document obj = cursor.next();
				fillMap(resultMap, obj);

				result.add(resultMap);
			}

			return Status.OK;
		} catch (Exception e) {
			System.err.println(e.toString());
			return Status.ERROR;
		} finally {
			if (cursor != null) {
				cursor.close();
			}
		}
	}

	/**
	 * Update a record in the database. Any field/value pairs in the specified
	 * values HashMap will be written into the record with the specified record key,
	 * overwriting any existing values with the same field name.
	 * 
	 * @param table  The name of the table
	 * @param key    The record key of the record to write.
	 * @param values A HashMap of field/value pairs to update in the record
	 * @return Zero on success, a non-zero error code on error. See this class's
	 *         description for a discussion of error codes.
	 */
	@Override
	public Status update(String table, String key, Map<String, ByteIterator> values) {
		try {
			MongoCollection<Document> collection = database.getCollection(table);

			Document query = new Document("_id", key);
			Document fieldsToSet = new Document();
			for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
				fieldsToSet.put(entry.getKey(), entry.getValue().toArray());
			}
			Document update = new Document("$set", fieldsToSet);

			UpdateResult result = collection.updateOne(query, update);
			if (result.wasAcknowledged() && result.getMatchedCount() == 0) {
				System.err.println("Nothing updated for key " + key);
				return Status.NOT_FOUND;
			}
			return Status.OK;
		} catch (Exception e) {
			System.err.println(e.toString());
			return Status.ERROR;
		}
	}

	/**
	 * Fills the map with the values from the DBObject.
	 * 
	 * @param resultMap The map to fill/
	 * @param obj       The object to copy values from.
	 */
	protected void fillMap(Map<String, ByteIterator> resultMap, Document obj) {
		for (Map.Entry<String, Object> entry : obj.entrySet()) {
			if (entry.getValue() instanceof Binary) {
				resultMap.put(entry.getKey(), new ByteArrayByteIterator(((Binary) entry.getValue()).getData()));
			}
		}
	}
}
