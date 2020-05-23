package site.ycsb.db.RUBiS;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;

import org.bson.Document;

import com.mongodb.Block;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;

import site.ycsb.db.RUBiS.DataModel.DocumentGenerator;
import site.ycsb.db.RUBiS.DataModel.User;

/**
 * Defines several query plans that can be tested.
 * 
 * Join sequences:
 * Bids -> Users x O
 * Bids -> Items x O
 * 
 * Users->Regions x O
 * Users->Items X O
 * Users->Bids->items OK
 * Users->Bids->items->User x O
 * Users->Comments x O
 * 
 * Items->Comments->Users x O
 * Items->Comments x O
 * Items->users X O
 * Items->Bids x O
 * 
 * Region -> Users x OK
 * 
 * Recommendations:
        
                 [ items680 [ users-820 ] ] 0.0000  0.0000  0.3333  0.3333  0.2778  0.0000  0.0000  0.3750  0.3750  0.6667  0.6667  w: 56.6667   r: 4   QP size: 14
                 [ bids-351 [ items680 ] ] 0.0000  0.3333  0.0000  0.0000  0.3333  0.6667  0.0000  0.3750  0.1250  0.0000  0.0000  w: 45.0000   r: 3   QP size: 11
                 [ items680 [ bids-351 ] ] 0.0000  0.6667  0.0000  0.0000  0.0000  0.3333  0.0000  0.1250  0.3750  0.0000  0.0000  w: 45.0000   r: 2   QP size: 10
              [ items680 [ comments934 ] ] 0.0000  0.0000  0.5000  0.5000  0.0000  0.0000  1.0000  0.0000  0.0000  0.0000  0.0000  w: 65.0000   r: 5   QP size: 5
[ items680 [ users-820 [ regions-251 ] ] ] 1.0000  0.0000  0.1667  0.1667  0.3889  0.0000  0.0000  0.1250  0.1250  0.3333  0.3333  w: 33.3333   r: 1   QP size: 16
                                          1.0000  1.0000  1.0000  1.0000  1.0000  1.0000  1.0000  1.0000  1.0000  1.0000  1.0000  
 * 
 * @author vincent
 *
 */
public class WorkloadModelFiveDoc {
	
	/**
	 * Workload generator factory.
	 * 
	 * Starts at a user with record ID = 1.
	 * 
	 * @author vincent
	 *
	 */
	public static class WorkloadGenerator {

		private static WorkloadGenerator instance = null;

		private int recordId = 0;

		public static WorkloadGenerator getInstance() {
			if (instance == null)
				instance = new WorkloadGenerator();

			return instance;
		}
		
		public Document executeQuery(MongoDatabase db) {
			int userId = recordId;
			recordId++;
			
			Document result;
			
//			result = getBidsUsers(db, userId);
//			result = getBidsItems(db, userId);
//			result = getUsersRegions(db, userId);
//			result = getUsersItems(db, userId);
//			result = getUsersBidsItems(db, userId);
//			result = getUsersBidsItemsUsers(db, userId);
			result = getUsersComments(db, userId);
//			result = getItemsComments(db, userId);
//			result = getItemsCommentsUsers(db, userId);
//			result = getItemsUsers(db, userId);
//			result = getItemsBids(db, userId);
//			
//			System.out.println("----");
//			result = getNormalizedQuery(db, "Bids", "Users", "id_user", "_id", User.getBidIds(userId).get(0));
//			result = getNormalizedQuery(db, "Bids", "Items", "id_item", "_id", User.getBidIds(userId).get(0));
//			
//			result = getNormalizedQuery(db, "Users", "Regions", "id_region", "_id", userId);
//			result = getNormalizedQuery(db, "Users", "Items", "_id", "id_seller", userId);
//			result = getNormalizedQuery(db, "Users", "Comments", "_id", "id_user", userId);
//			
//			result = getNormalizedQuery(db, "Items", "Users", "id_seller", "_id", User.getItemIds(userId).get(0));
//			result = getNormalizedQuery(db, "Items", "Comments", "_id", "id_item", User.getItemIds(userId).get(0));
//			result = getNormalizedQuery(db, "Items", "Bids", "_id", "id_item", User.getItemIds(userId).get(0));
//			
//			result = getNormalizedQuery(db, "Regions", "Users", "_id", "id_region", User.getRegionId(userId));
//			
//			result = getNormalizedQueryItemsCommentsUser(db, userId);
//			result = getNormalizedQueryUsersBidsItemsUser(db, userId);
//			result = getNormalizedQueryUsersBidsItems(db, userId);
			
			System.out.println(result);

			
			return result;			
		}
	}
	
	/**
	 * ========================================
	 *  QUERY PLAN RECOMMENDATIONS 
	 * ========================================
	 * 
	 * Per sequence use the best path? or compare vs. multiple options to check cost model.
	 * 
	 */
	
	/**
	 * Rank: 3Valid: true, Cost:1845, Sequence: 488629942, 
	 * QueryPlan [candidates=[ bids-351 [ items680 ] ] -> [ items680 [ users-820 ] ]],
	 * queryMapping={0=[Query [bids]], 1=[Query [users]]}, secondaryIndex={1=[ users-820 ]}]
	 * @return
	 */
	public static Document getBidsUsers(MongoDatabase database, int userId) {
		MongoCollection<Document> collection = database.getCollection("BidsItems");
		Document query = new Document("_id", User.getBidIds(userId).get(0));
		
		FindIterable<Document> findIterable = collection.find(query);
		Document queryResult = findIterable.first();
		
//		System.out.println(queryResult);
		
		collection = database.getCollection("ItemsUsers");
		query = new Document("users._id", queryResult.get("id_user"));
		
		// loop over result set to make sure we get all.
		collection.find(query).forEach(printBlock);
		
		return queryResult;
	}
	
	// OK
	public static Document getBidsItems(MongoDatabase database, int userId) {
		MongoCollection<Document> collection = database.getCollection("BidsItems");
		Document query = new Document("_id", User.getBidIds(userId).get(0));
		
		FindIterable<Document> findIterable = collection.find(query);

		Document queryResult = findIterable.first();
		
		return queryResult;
	}
	
	/**
	 * OK.
	 * 
	 * Rank: 1Valid: true, Cost:1925, Sequence: -8863848, 
	 * QueryPlan [candidates=[ items680 [ users-820 [ regions-251 ] ] ]], 
	 * queryMapping={0=[Query [users], Query [regions]]}, secondaryIndex={0=[ users-820 [ regions-251 ] ]}]
	 * @return
	 */
	public static Document getUsersRegions(MongoDatabase database, int userId) {
		MongoCollection<Document> collection = database.getCollection("ItemsUsersRegions");
		Document query = new Document("users._id", userId);
		
		FindIterable<Document> findIterable = collection.find(query);

		Document queryResult = findIterable.first();
		
		return queryResult;
	}
	
	/**
	 * Rank: 2Valid: true, Cost:1875, Sequence: 2118189277, 
	 * QueryPlan [candidates=[ items680 [ users-820 ] ]], queryMapping={0=[Query [users], Query [items]]}, secondaryIndex={0=[ users-820 ]}]
	 * @return
	 */
	public static Document getUsersItems(MongoDatabase database, int userId) {
		MongoCollection<Document> collection = database.getCollection("ItemsUsers");
		Document query = new Document("users._id", userId);
		
		collection.find(query).forEach(printBlock);
		
		return query;
	}
	
	/**
	 * Rank: 4Valid: true, Cost:11575, Sequence: -1058650145, 
	 * QueryPlan [candidates=[ items680 [ users-820 ] ] -> [ bids-351 [ items680 ] ]], 
	 * queryMapping={0=[Query [users]], 1=[Query [bids], Query [items]]}, secondaryIndex={0=[ users-820 ]}]
	 * 
	 * @param database
	 * @param userId
	 * @return
	 */
	public static Document getUsersBidsItems(MongoDatabase database, int userId) {
		MongoCollection<Document> collection = database.getCollection("ItemsUsers");
		Document query = new Document("users._id", userId);
		FindIterable<Document> findIterable = collection.find(query);
		Document queryResult = findIterable.first();
		
//		System.out.println(queryResult);
		
		Object id = ((Document) queryResult.get("users")).get("_id");
		collection = database.getCollection("BidsItems");
		query = new Document("id_user", id);
		
		// loop over result set to make sure we get all.
		collection.find(query).forEach(printBlock);
		
		return queryResult;
	}
	
	/**
	 * Rank: 9Valid: true, Cost:12230, Sequence: 562623424, 
	 * QueryPlan [candidates=[ items680 [ users-820 ] ] -> [ bids-351 [ items680 ] ] -> [ items680 [ users-820 [ regions-251 ] ] ]], (changed last one)
	 * queryMapping={0=[Query [users]], 1=[Query [bids], Query [items]], 2=[Query [users]]}, secondaryIndex={0=[ users-820 ]}]
	 */
	public static Document getUsersBidsItemsUsers(MongoDatabase database, int userId) {
		MongoCollection<Document> collection = database.getCollection("ItemsUsers");
		Document query = new Document("users._id", userId);
		FindIterable<Document> findIterable = collection.find(query);
		Document queryResult = findIterable.first();
		
//		System.out.println(queryResult);
		
		Object id = ((Document) queryResult.get("users")).get("_id");
		collection = database.getCollection("BidsItems");
		query = new Document("id_user", id);
		findIterable = collection.find(query);
		queryResult = findIterable.first();
		
//		System.out.println(queryResult);
		// OK -> Only 1 seller_id anyway (checked)
		
		id = ((Document) queryResult.get("items")).get("id_seller");
		collection = database.getCollection("ItemsUsers");
		query = new Document("users._id", id);
		findIterable = collection.find(query);
		queryResult = findIterable.first();
		
//		System.out.println(queryResult);
		
		return queryResult;
	}
	
	/**
	 * Rank: 2Valid: true, Cost:8650, Sequence: 1390978783, 
	 * QueryPlan [candidates=[ items680 [ users-820 ] ] -> [ items680 [ comments934 ] ]], 
	 * queryMapping={0=[Query [users]], 1=[Query [comments]]}, secondaryIndex={0=[ users-820 ], 1=[ comments934 ]}]
	 * 
	 */
	public static Document getUsersComments(MongoDatabase database, int userId) {
		MongoCollection<Document> collection = database.getCollection("ItemsUsers");
		Document query = new Document("users._id", userId);
		FindIterable<Document> findIterable = collection.find(query);
		Document queryResult = findIterable.first();
		
//		System.out.println(queryResult);
		
		Object id = ((Document) queryResult.get("users")).get("_id");
		collection = database.getCollection("ItemsComments");
		query = new Document("comments.id_user", id);
		
		// loop over result set to make sure we get all.
		collection.find(query).forEach(printBlock);
		
//		System.out.println(queryResult);
		
		return queryResult;
	}
	
	// OK [Items|Comments]
	public static Document getItemsComments(MongoDatabase database, int userId) {
		MongoCollection<Document> collection = database.getCollection("ItemsComments");
		Document query = new Document("_id", User.getItemIds(userId).get(0));
		
		FindIterable<Document> findIterable = collection.find(query);
		Document queryResult = findIterable.first();
		
		return queryResult;
	}
	
	/**
	 * Rank: 2Valid: true, Cost:2155, Sequence: -1619451028, 
	 * QueryPlan [candidates=[ items680 [ comments934 ] ] -> [ items680 [ users-820 ] ]],
	 *  queryMapping={0=[Query [items], Query [comments]], 1=[Query [users]]}, secondaryIndex={}]
	 * @return
	 */
	public static Document getItemsCommentsUsers(MongoDatabase database, int userId) {
		MongoCollection<Document>  collection = database.getCollection("ItemsComments");
		Document query = new Document("_id", User.getCommentIds(userId).get(0));
		FindIterable<Document> findIterable = collection.find(query);
		Document queryResult = findIterable.first();
		
//		System.out.println(queryResult);
		
		// OK Only 1 probably. Item - 2 comments - User has more.
		Object id = ((ArrayList<Document>) queryResult.get("comments")).get(0).get("id_user");
		collection = database.getCollection("ItemsUsers");
		query = new Document("users._id", id);
		findIterable = collection.find(query);
		queryResult = findIterable.first();
		
		System.out.println(queryResult);
		
		return queryResult;
	}
	
	// OK [Items|Users|Regions]
	public static Document getItemsUsers(MongoDatabase database, int userId) {
		MongoCollection<Document> collection = database.getCollection("ItemsUsersRegions");
		Document query = new Document("_id", User.getItemIds(userId).get(0));
		
		FindIterable<Document> findIterable = collection.find(query);
		Document queryResult = findIterable.first();
		
		return queryResult;
	}
	
	/**
	 * OK on [Items|Bids]
	 */
	public static Document getItemsBids(MongoDatabase database, int userId) {
		MongoCollection<Document> collection = database.getCollection("ItemsBids");
		
		int itemId = User.getItemIds(userId).get(0);
		Document query = new Document("_id", itemId);
		
//		System.out.println(query);
		
		FindIterable<Document> findIterable = collection.find(query);

		Document queryResult = findIterable.first();
		
		return queryResult;
	}
	
	/**
	 * Rank: 1Valid: true, Cost:3875500, Sequence: -1642497110, 
	 * QueryPlan [candidates=[ items680 [ users-820 [ regions-251 ] ] ]], 
	 * queryMapping={0=[Query [regions], Query [users]]}, secondaryIndex={0=[ regions-251 ]}]
	 * @return
	 */
	public static Document getRegionsUsers(MongoDatabase database, int userId) {
		MongoCollection<Document> collection = database.getCollection("ItemsUsersRegions");
		
		int regionId = User.getRegionId(userId);
		Document query = new Document("users.regions._id", regionId);
		
		//TODO find all?
		// loop over result set to make sure we get all.
		collection.find(query).forEach(printBlock);
		
		return query;
	}
	
	/**
	 * ===============================
	 * Normalized Query Plans
	 * Bids -> Users x O
	 * Bids -> Items x O
	 * 
	 * Users->Regions x O
	 * Users->Items X O
	 * Users->Bids->items (Missing)
	 * Users->Bids->items->User x O
	 * Users->Comments x O
	 * 
	 * Items->Comments->Users x O
	 * Items->Comments x O
	 * Items->users X O
	 * Items->Bids x O
	 * 
	 * Regions->User 
	 * 
	 * ===============================
	 */
	private static void getNormalizedQueries(MongoDatabase db, int userId) {
		getNormalizedQuery(db, "Bids", "Users", "id_user", "_id", User.getBidIds(userId).get(0));
		getNormalizedQuery(db, "Bids", "Items", "id_item", "_id", User.getBidIds(userId).get(0));
		
		getNormalizedQuery(db, "Users", "Regions", "id_region", "_id", userId);
		getNormalizedQuery(db, "Users", "Items", "_id", "id_seller", userId);
		getNormalizedQuery(db, "Users", "Comments", "_id", "id_user", userId);
		
		getNormalizedQuery(db, "Items", "Users", "id_seller", "_id", User.getItemIds(userId).get(0));
		getNormalizedQuery(db, "Items", "Comments", "_id", "id_item", User.getItemIds(userId).get(0));
		getNormalizedQuery(db, "Items", "Bids", "_id", "id_item", User.getItemIds(userId).get(0));
		
		getNormalizedQuery(db, "Regions", "Users", "_id", "id_region", User.getRegionId(userId));
		
		// Two longer cases.
		System.out.println(getNormalizedQueryItemsCommentsUser(db, userId));
		System.out.println(getNormalizedQueryUsersBidsItemsUser(db, userId));
		System.out.println(getNormalizedQueryUsersBidsItems(db, userId));
	}

	private static Document getNormalizedQuery(MongoDatabase db, String collectionOne, String collectionTwo, String localKey, String foreignKey, int id) {
		MongoCollection<Document> collection = db.getCollection(collectionOne);
		Document query = new Document("_id", id);
		
		FindIterable<Document> findIterable = collection.find(query);
		Document queryResult = findIterable.first();
		
//		System.out.println(queryResult);
		
		collection = db.getCollection(collectionTwo);
		query = new Document(foreignKey, queryResult.get(localKey));
		
		// loop over result set to make sure we get all.
		collection.find(query).forEach(printBlock);
		
		return queryResult;
	}

	
	/**
	 * Items->Comments->User
	 */
	private static Document getNormalizedQueryItemsCommentsUser(MongoDatabase db, int userId) {
		MongoCollection<Document>  collection = db.getCollection("Items");
		Document query = new Document("_id", User.getItemIds(userId).get(0));
		FindIterable<Document> findIterable = collection.find(query);
		Document queryResult = findIterable.first();
		
//		System.out.println(queryResult);
		
		collection = db.getCollection("Comments");
		query = new Document("id_item", queryResult.get("_id"));
		findIterable = collection.find(query);
		queryResult = findIterable.first();
		
//		System.out.println(queryResult);
		
		collection = db.getCollection("Users");
		query = new Document("_id", queryResult.get("id_user"));
		findIterable = collection.find(query);
		queryResult = findIterable.first();
		
//		System.out.println(queryResult);
		
		return queryResult;
	}
	
	/**
	 * 
	 * Users->Bids->Items
	 * 
	 * db.Users.aggregate([ {$match:{_id:0}},   
	 * { $lookup: { from:"Bids", localField:"_id", foreignField:"id_user", as:"UsersBids" } },  
	 * { $lookup: { from:"Items", localField:"UsersBids.id_item", foreignField:"_id", as:"BidsItems"} },  

	 * @param db
	 * @param userId
	 * @return
	 */
	private static Document getNormalizedQueryUsersBidsItems(MongoDatabase db, int userId) {
		MongoCollection<Document>  collection = db.getCollection("Users");
		Document query = new Document("_id", userId);
		FindIterable<Document> findIterable = collection.find(query);
		Document queryResult = findIterable.first();
		
//		System.out.println(queryResult);
		
		collection = db.getCollection("Bids");
		query = new Document("id_user", queryResult.get("_id"));
		findIterable = collection.find(query);
		queryResult = findIterable.first();
		
//		System.out.println(queryResult);
		
		//TODO query all?
		collection = db.getCollection("Items");
		query = new Document("_id", queryResult.get("id_item"));
		findIterable = collection.find(query);
		queryResult = findIterable.first();
		
//		System.out.println(queryResult);
		
		return queryResult;
	}
	
	/**
	 * 
	 * Users->Bids->Items->User
	 * 
	 * db.Users.aggregate([ {$match:{_id:0}},   
	 * { $lookup: { from:"Bids", localField:"_id", foreignField:"id_user", as:"UsersBids" } },  
	 * { $lookup: { from:"Items", localField:"UsersBids.id_item", foreignField:"_id", as:"BidsItems"} },  
	 * { $lookup: { from:"Users", localField:"BidsItems.id_seller", foreignField:"_id", as:"ItemsUsers" }}  ])

	 * @param db
	 * @param userId
	 * @return
	 */
	private static Document getNormalizedQueryUsersBidsItemsUser(MongoDatabase db, int userId) {
		MongoCollection<Document>  collection = db.getCollection("Users");
		Document query = new Document("_id", userId);
		FindIterable<Document> findIterable = collection.find(query);
		Document queryResult = findIterable.first();
		
//		System.out.println(queryResult);
		
		collection = db.getCollection("Bids");
		query = new Document("id_user", queryResult.get("_id"));
		findIterable = collection.find(query);
		queryResult = findIterable.first();
		
//		System.out.println(queryResult);
		
		//TODO query all?
		collection = db.getCollection("Items");
		query = new Document("_id", queryResult.get("id_item"));
		findIterable = collection.find(query);
		queryResult = findIterable.first();
		
//		System.out.println(queryResult);
		
		collection = db.getCollection("Users");
		query = new Document("_id", queryResult.get("id_seller"));
		findIterable = collection.find(query);
		queryResult = findIterable.first();
		
//		System.out.println(queryResult);
		
		
		return queryResult;
	}
	
	
	public static Block<Document> printBlock = new Block<Document>() {
	       @Override
	       public void apply(final Document document) {
//	           System.out.println(document.toJson());
	       }
	};
	
}
