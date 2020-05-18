package site.ycsb.db;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;

import site.ycsb.ByteIterator;


public class DataModel {

	private static int userItems = 5;

	private static int userBids = 15;

	private static int userComments = 10;

	private static int itemComments = 2;

	private static int itemBids = 3;

	private static int nrOfRegions = 4;

	public static void main(String[] args) {
//		System.out.println(User.getItemIds(0));
//		System.out.println(User.getItemIds(1));
//
//		System.out.println(User.getBidIds(1));
//
//		System.out.println(Item.getUserId(0));
//		System.out.println(Item.getUserId(4));
//		System.out.println(Item.getUserId(10));
		
		DocumentGenerator gen = DocumentGenerator.getInstance();
		
//		System.out.println(User.generateDocument(1));
//		System.out.println(new User(1).generateDocuments());
		
		gen.createUser();
	}

	/**
	 * Document generator factory.
	 * 
	 * Starts at creating a user with record ID = 1.
	 * 	=> Triggers creation of Item, Comment, Bid record creation.
	 * 
	 * @author vincent
	 *
	 */
	public static class DocumentGenerator {

		private static DocumentGenerator instance = null;

		private int recordId = 0;
		
		public DocumentGenerator() {
			// create Regions.
			for(int i=0; i < nrOfRegions; i++)
				persistDocuments(new Region(i).generateDocuments());
		}

		public static DocumentGenerator getInstance() {
			if (instance == null)
				instance = new DocumentGenerator();

			return instance;
		}

		/**
		 * One user triggers the creation of several items, bids, comments, etc.
		 * 
		 * [ items672 [ bids62 [ users-532 ] ] ]
		 * [ items672 [ users-532 [ regions756 ] ] ]
		 * [ comments486 ]
		 * [ bids62 [ users-532 [ items672 ] ] ]
		 * [ users-532 [ comments486 ] ]
		 */
		public void createUser() {
			int userId = recordId;
			
			persistDocuments(new User(userId).generateDocuments());
			
			List<Integer> itemIds = User.getItemIds(userId);
			createItems(itemIds);

			List<Integer> commentIds = User.getCommentIds(userId);
			createComments(commentIds);
			
			List<Integer> bidIds = User.getBidIds(userId);
			createBids(bidIds);
		}

		private void createBids(List<Integer> bidIds) {
			for (int bidId: bidIds)
				persistDocuments(new Bid(bidId).generateDocuments());
		}

		private void createComments(List<Integer> commentIds) {
			for (int commentId : commentIds)
				persistDocuments(new Comment(commentId).generateDocuments());
		}

		private void createItems(List<Integer> itemIds) {
			for (int itemId : itemIds)
				persistDocuments(new Item(itemId).generateDocuments());
		}
		
		/**
		 * Store per collection (key), the correct document in the Database.
		 * 
		 * @param documents
		 */
		private void persistDocuments(Map<String, Document> documents) {
			//TODO persist to DB
			System.out.println(documents);
		}

	}
	
	/**
	 * TODO: naming convention of generate: 
	 * always generateItemsRegions (meervoud) of ook
	 *        generateItemRegion
	 *        
	 * @author vincent
	 *
	 */
	public interface DocumentModel{
		
		/**
		 * Creates all possible variants of the document in various collections, where it is the TOP-level document.
		 * 
		 * Each variant may embed additional documents.
		 */
		public Map<String, Document> generateDocuments();
	
	}
	
	/**
	 * Create all user records with User as top-level node.
	 * Persist into MongoDB.
	 * 
	 * @author vincent
	 */
	public static class User implements DocumentModel{
		
		private final int userId;
		
		public User(int userId) {
			this.userId = userId;
		}
		
		@Override
		public Map<String, Document> generateDocuments() {
			Map<String, Document> documentPerCollection = new HashMap<String, Document>();
			
			// create Users
			documentPerCollection.put("Users", generateDocument(this.userId));
			
			// create UsersComments
			documentPerCollection.put("UserComments", generateDocumentUserComments(this.userId));
			
			return documentPerCollection;
		}
		
		/**
		 * Create 1-level User document
		 */
		public static Document generateDocument(int id) {
			Document doc = new Document("_id", id);
		    
			doc.put("firstName", "name");
			
			//TODO embed references(?)
			
			return doc;
		}
		
		/**
		 * Creates User|Comments 
		 */
		public static Document generateDocumentUserComments(int userId) {
			Document userComments = generateDocument(userId);
			
			// Add comments
			ArrayList<Document> comments = new ArrayList<Document>();
			
			for(int commentId: getCommentIds(userId))
				comments.add(Comment.generateDocument(commentId));
			
			userComments.put("comments", comments);
			
			return userComments;
		}
		
		public static Document generateDocumentUsersRegions(int userId) {
			Document userDoc = generateDocument(userId);
			
			// Add region
			Document region = Region.generateDocument(User.getRegionId(userId));
			userDoc.append("region", region);
			
			return userDoc;
		}
		
		public static Document generateDocumentUserItems(int userId) {
			Document userDoc = generateDocument(userId);
			
			// Add items
			ArrayList<Document> items = new ArrayList<Document>();
			
			for(int itemId: getItemIds(userId)) {
				items.add(Item.generateDocument(itemId));
			}
			
			userDoc.append("items", items);
			
			return userDoc;
		}
		
		public static int getRegionId(int userId) {
			return userId % nrOfRegions;
		}

		public static List<Integer> getItemIds(int userId) {
			ArrayList<Integer> itemIds = new ArrayList<Integer>();

			int startId = userId  * userItems;
			int endId = (userId+1) * (userItems);

			for (int i = startId; i < endId; i++) {
				itemIds.add(i);
			}

			return itemIds;
		}

		public static List<Integer> getBidIds(int userId) {
			ArrayList<Integer> bidIds = new ArrayList<Integer>();

			int startId = userId * userBids;
			int endId = (userId+1) * (userBids);

			for (int i = startId; i < endId; i++) {
				bidIds.add(i);
			}

			return bidIds;
		}

		public static List<Integer> getCommentIds(int userId) {
			ArrayList<Integer> commentIds = new ArrayList<Integer>();

			int startId = (userId) * userComments;
			int endId = (userId+1) * (userComments);

			for (int i = startId; i < endId; i++) {
				commentIds.add(i);
			}

			return commentIds;
		}
	}

	public static class Item implements DocumentModel{
		
		private final int itemId;
		
		public Item(int itemId) {
			this.itemId = itemId;
		}
		
		@Override
		public Map<String, Document> generateDocuments() {
			Map<String, Document> documentPerCollection = new HashMap<String, Document>();
			
			// create Items
			documentPerCollection.put("Items", generateDocument(this.itemId));
			
			// create Items | Bids | Users
			documentPerCollection.put("ItemsBidsUsers", generateDocumentItemsBidsUsers(this.itemId));
			
			// create Items | Users | Regions
			documentPerCollection.put("itemsUsersRegions", generateDocumentItemsUsersRegions(this.itemId));
			
			return documentPerCollection;
		}
		
		public static Document generateDocument(int itemId) {
			Document doc = new Document("_id", itemId);
		    
			doc.put("productName", "name");
			
			return doc;
		}
		
		public static Document generateDocumentItemsBidsUsers(int itemId) {
			Document doc = generateDocument(itemId);
			ArrayList<Document> bidUsers = new ArrayList<Document>();
			
			// for each Bid get BidUser
			for(int bidId: getBidIds(itemId)) {
				bidUsers.add(Bid.generateDocumentBidUsers(bidId));
			}
			
			doc.append("bids", bidUsers);
			
			return doc;
		}
		
		public static Document generateDocumentItemsUsersRegions(int itemId) {
			Document itemDoc = generateDocument(itemId);
			Document userRegion = User.generateDocumentUsersRegions(getUserId(itemId));
			
			itemDoc.append("user", userRegion);
			
			return itemDoc;
		}

		public static int getUserId(int itemId) {

			int userId = itemId / userItems;
			return userId;
		}
		
		public static List<Integer> getBidIds(int itemId) {
			ArrayList<Integer> bidIds = new ArrayList<Integer>();

			int startId = (itemId) * itemBids;
			int endId = (itemId+1) * (itemBids);

			for (int i = startId; i < endId; i++) {
				bidIds.add(i);
			}

			return bidIds;
		}

		public static List<Integer> getCommentIds(int itemId) {
			ArrayList<Integer> commentIds = new ArrayList<Integer>();

			int startId = itemId * itemComments;
			int endId = (itemId+1) * (itemComments);

			for (int i = startId; i < endId; i++) {
				commentIds.add(i);
			}

			return commentIds;
		}

		
	}

	public static class Bid implements DocumentModel {
		
		private final int bidId;
		
		public Bid(int bidId) {
			this.bidId = bidId;
		}
		
		@Override
		public Map<String, Document> generateDocuments() {
			Map<String, Document> documentPerCollection = new HashMap<String, Document>();
			
			// create Bids
			documentPerCollection.put("Bids", generateDocument(this.bidId));
			
			// Create Bids|Users|Items
			documentPerCollection.put("BidsUsersItems", generateDocumentBidsUsersItems(bidId));
			
			return documentPerCollection;
		}
		
		private static Document generateDocument(int bidId) {
			Document doc = new Document("_id", bidId);
		    
			doc.put("price", "randomPrice");
			
			return doc;
		}
		
		public static Document generateDocumentBidUsers(int bidId) {
			Document bidUsers = generateDocument(bidId);
			
			int userId = getUserId(bidId);
			bidUsers.append("user", User.generateDocument(userId));
			
			return bidUsers;
		}
		
		public static Document generateDocumentBidsUsersItems(int bidId) {
			Document bidUserItemsDoc = generateDocument(bidId);
			Document userItems = User.generateDocumentUserItems(getUserId(bidId));
			
			bidUserItemsDoc.append("users", userItems);
			
			return bidUserItemsDoc;
		}

		public static int getUserId(int bidId) {
			int userId = bidId / userBids;
			return userId;
		}

		public static int getItemId(int bidId) {
			int itemId = bidId / itemBids;
			return itemId;
		}
	}

	public static class Comment implements DocumentModel {
		
		private final int commentId;
		
		public Comment(int commentId) {
			this.commentId = commentId;
		}
		
		@Override
		public Map<String, Document> generateDocuments() {
			Map<String, Document> documentPerCollection = new HashMap<String, Document>();
			
			// create Comments
			documentPerCollection.put("Comments", generateDocument(this.commentId));
			
			
			return documentPerCollection;
		}
		
		/**
		 * Create 1-level comment document
		 */
		public static Document generateDocument(int id) {
			Document doc = new Document("_id", id);
		    
			doc.put("commentText", "randomText");
			
			return doc;
		}

		public static int getUserId(int commentId) {
			int userId = commentId / userComments;
			return userId;
		}		

		public static int getItemId(int commentId) {
			int itemId = commentId / itemComments;
			return itemId;
		}
	}
	
	public static class Region implements DocumentModel {
		
		private final int regionId;
		
		public Region(int regionId) {
			this.regionId = regionId;
		}
		
		@Override
		public Map<String, Document> generateDocuments() {
			Map<String, Document> documentPerCollection = new HashMap<String, Document>();
			
			// create Regions
			documentPerCollection.put("Regions", generateDocument(this.regionId));
			
			
			return documentPerCollection;
		}
		
		/**
		 * Create 1-level comment document
		 */
		public static Document generateDocument(int id) {
			Document doc = new Document("_id", id);
		    
			doc.put("regionName", "randomRegion");
			
			return doc;
		}
	}
}
