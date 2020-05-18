package site.ycsb.db.RUBiS;

import java.util.Arrays;

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
 * Bids -> Users
 * Bids -> Items
 * 
 * Users->Regions
 * Users->Items
 * Users->Bids->items->User
 * Users->Comments
 * Users->Bids
 * 
 * Items->Comments->Users
 * Items->Comments
 * Items->users
 * Items->Bids
 * 
 * Region -> Users
 * 
 * @author vincent
 *
 */
public class WorkloadModel {
	
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
			
			return getQueryUsersBidsItemsUsers(db, userId);			
		}
	}
	
	/**
	 * Items->Bids on [Items|Bids]
	 */
	public static Document getQueryItemsBids(MongoDatabase database, int userId) {
		MongoCollection<Document> collection = database.getCollection("ItemsBids");
		
		int itemId = User.getItemIds(userId).get(0);
		Document query = new Document("_id", itemId);
		
//		System.out.println(query);
		
		FindIterable<Document> findIterable = collection.find(query);

		Document queryResult = findIterable.first();
		
		return queryResult;
	}
	
	/**
	 * Items->Bids on [Items|Bids|Users]
	 */
	public static Document getQueryItemsBidsLonger(MongoDatabase database, int userId) {
		MongoCollection<Document> collection = database.getCollection("ItemsBidsUsers");
		
		int itemId = User.getItemIds(userId).get(0);
		Document query = new Document("_id", itemId);
		
//		System.out.println(query);
		
		FindIterable<Document> findIterable = collection.find(query);

		Document queryResult = findIterable.first();
		
		return queryResult;
	}
	
	/**
	 * Users->Items on [Users|Items]
	 */
	public static Document getQueryUsersComments(MongoDatabase database, int userId) {
		MongoCollection<Document> collection = database.getCollection("UsersComments");
		Document query = new Document("_id", userId);
		
		FindIterable<Document> findIterable = collection.find(query);

		Document queryResult = findIterable.first();
		
		return queryResult;
	}

	/**
	 * Users->Bids->Items->Users on [ bids [ users [ items ] ] ] -> [ items [ users [ regions ] ] ]]  secondaryIndex={0=[ users [ items ] ]}]
	 * 
	 * Rank: 1Valid: true, Cost:2455, Sequence: -993239990, 
	 * QueryPlan [candidates=[ bids62 [ users-532 [ items672 ] ] ] -> [ items672 [ users-532 [ regions756 ] ] ]], 
	 * queryMapping={0=[Query [users], Query [bids]], 1=[Query [items], Query [users]]}, secondaryIndex={0=[ users-532 [ items672 ] ]}]
	 */
	public static Document getQueryUsersBidsItemsUsers(MongoDatabase database, int userId) {
		MongoCollection<Document> collection = database.getCollection("BidsUsersItems");
		Document query = new Document("users._id", userId);
		
		//  db.BidsUsersItems.aggregate([ { $lookup: { from:"ItemsUsersRegions", localField:"id_item", foreignField:"_id", as:"UsersBidsItemsUsers" } } ] )
		// db.BidsUsersItems.aggregate([ {$match:{"users._id":0}}, { $lookup: { from:"ItemsUsersRegions", localField:"id_item", foreignField:"_id", as:"UsersBidsItemsUsers" } } ] )
		Block<Document> printBlock = new Block<Document>() {
	        @Override
	        public void apply(final Document document) {
	            System.out.println(document.toJson());
	        }
	    };
	    
		AggregateIterable<Document> aggIterable = collection.aggregate(Arrays.asList(
				 Aggregates.match(Filters.eq("users._id", userId)),
	              Aggregates.lookup("ItemsUsersRegions", "id_item", "_id", "UsersBidsItemsUsers")
				)
		);
		
		Document queryResult = aggIterable.first();
		
		System.out.println(queryResult);
		
		return queryResult;
	}
	
	/**
	 *  Users->Bids->Items->Users on [Items|Users|Regions] and [Bids] and [Items|Users|Regions]
	 *  
	 * Rank: 2Valid: true, Cost:3225, Sequence: -993239990, 
	 * QueryPlan [candidates=[ items672 [ users-532 [ regions756 ] ] ] -> [ bids62 ] -> [ items672 [ users-532 [ regions756 ] ] ]], 
	 * queryMapping={0=[Query [users]], 1=[Query [bids]], 2=[Query [items], Query [users]]}, secondaryIndex={0=[ users-532 [ regions756 ] ]}]
	 */
	public static Document getQueryUsersBidsItemsUsersAlt(MongoDatabase database, int userId) {
		MongoCollection<Document> collection = database.getCollection("BidsUsersItems");
		Document query = new Document("users._id", userId);
		
		//  db.BidsUsersItems.aggregate([ { $lookup: { from:"ItemsUsersRegions", localField:"id_item", foreignField:"_id", as:"UsersBidsItemsUsers" } } ] )
		// db.BidsUsersItems.aggregate([ {$match:{"users._id":0}}, { $lookup: { from:"ItemsUsersRegions", localField:"id_item", foreignField:"_id", as:"UsersBidsItemsUsers" } } ] )
		Block<Document> printBlock = new Block<Document>() {
	        @Override
	        public void apply(final Document document) {
	            System.out.println(document.toJson());
	        }
	    };
	    
		AggregateIterable<Document> aggIterable = collection.aggregate(Arrays.asList(
				 Aggregates.match(Filters.eq("users._id", userId)),
	              Aggregates.lookup("ItemsUsersRegions", "id_item", "_id", "UsersBidsItemsUsers")
				)
		);
		
		Document queryResult = aggIterable.first();
		
		System.out.println(queryResult);
		
		return queryResult;
	}
	
	/**
	 * Bids|User
	 */
	private static void getQueryBidsUser() {
		
	}
	
	
	
}
