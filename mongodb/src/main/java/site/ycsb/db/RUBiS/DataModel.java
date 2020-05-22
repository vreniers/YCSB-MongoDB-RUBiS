package site.ycsb.db.RUBiS;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.RandomStringUtils;
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

		gen.createRecords();
	}

	/**
	 * Document generator factory.
	 * 
	 * Starts at creating a user with record ID = 1. => Triggers creation of Item,
	 * Comment, Bid record creation.
	 * 
	 * @author vincent
	 *
	 */
	public static class DocumentGenerator {

		private static DocumentGenerator instance = null;

		private int recordId = 0;

		public static DocumentGenerator getInstance() {
			if (instance == null)
				instance = new DocumentGenerator();

			return instance;
		}

		/**
		 * Should only be created once on initialization.
		 * 
		 * @return
		 */
		public Map<String, Set<Document>> getRegions() {
			Map<String, Set<Document>> regionDoc = new HashMap<String, Set<Document>>();
			regionDoc.put("Regions", new HashSet<Document>());

			// create Regions.
			for (int i = 0; i < nrOfRegions; i++)
				merge(regionDoc, new Region(i).generateDocuments());

			return regionDoc;
		}

		/**
		 * Starts with a single user creation.
		 * 
		 * One user creation triggers the creation of several items, bids, comments,
		 * etc.
		 * 
		 * [ items672 [ bids62 [ users-532 ] ] ] [ items672 [ users-532 [ regions756 ] ]
		 * ] [ comments486 ] [ bids62 [ users-532 [ items672 ] ] ] [ users-532 [
		 * comments486 ] ]
		 */
		public Map<String, Set<Document>> createRecords() {
			Map<String, Set<Document>> generatedDocuments = new HashMap<String, Set<Document>>();

			int userId = recordId;
			recordId++;

			createUser(userId, generatedDocuments);

			List<Integer> itemIds = User.getItemIds(userId);
			createItems(itemIds, generatedDocuments);

			List<Integer> commentIds = User.getCommentIds(userId);
			createComments(commentIds, generatedDocuments);

			List<Integer> bidIds = User.getBidIds(userId);
			createBids(bidIds, generatedDocuments);

//			System.out.println(generatedDocuments);

			return generatedDocuments;
		}

		/**
		 * Merge document results in similar collections.
		 */
		private void merge(Map<String, Set<Document>> generatedDocuments, Map<String, Document> generatedDoc) {
			for (String collection : generatedDoc.keySet()) {
				if (!generatedDocuments.containsKey(collection))
					generatedDocuments.put(collection, new HashSet<Document>());

				generatedDocuments.get(collection).add(generatedDoc.get(collection));
			}
		}

		private void createUser(int userId, Map<String, Set<Document>> generatedDocuments) {
			merge(generatedDocuments, new User(userId).generateDocuments());
		}

		private void createBids(List<Integer> bidIds, Map<String, Set<Document>> generatedDocuments) {
			for (int bidId : bidIds)
				merge(generatedDocuments, new Bid(bidId).generateDocuments());
		}

		private void createComments(List<Integer> commentIds, Map<String, Set<Document>> generatedDocuments) {
			for (int commentId : commentIds)
				merge(generatedDocuments, new Comment(commentId).generateDocuments());
		}

		private void createItems(List<Integer> itemIds, Map<String, Set<Document>> generatedDocuments) {
			for (int itemId : itemIds)
				merge(generatedDocuments, new Item(itemId).generateDocuments());
		}

	}

	/**
	 * TODO: naming convention of generate: always generateItemsRegions (meervoud)
	 * of ook generateItemRegion
	 * 
	 * @author vincent
	 *
	 */
	public interface DocumentModel {

		/**
		 * Creates all possible variants of the document in various collections, where
		 * it is the TOP-level document.
		 * 
		 * Each variant may embed additional documents.
		 */
		public Map<String, Document> generateDocuments();

	}

	/**
	 * Create all user records with User as top-level node. Persist into MongoDB.
	 * 
	 * @author vincent
	 */
	public static class User implements DocumentModel {

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
			documentPerCollection.put("UsersComments", generateDocumentUserComments(this.userId));

			return documentPerCollection;
		}

		/**
		 * Create 1-level User document
		 */
		public static Document generateDocument(int id) {
			Document doc = new Document("_id", id);

			doc.put("firstName", RandomStringUtils.randomAlphanumeric(20)); // 20
			doc.put("lastName", RandomStringUtils.randomAlphanumeric(25)); // 25

			doc.put("about", RandomStringUtils.randomAlphanumeric(70));

			doc.put("id_region", getRegionId(id));

			return doc;
		}

		/**
		 * Creates User|Comments
		 */
		public static Document generateDocumentUserComments(int userId) {
			Document userComments = generateDocument(userId);

			// Add comments
			ArrayList<Document> comments = new ArrayList<Document>();

			for (int commentId : getCommentIds(userId))
				comments.add(Comment.generateDocument(commentId));

			userComments.put("comments", comments);

			return userComments;
		}

		public static Document generateDocumentUsersRegions(int userId) {
			Document userDoc = generateDocument(userId);

			// Add region
			Document region = Region.generateDocument(User.getRegionId(userId));
			userDoc.append("regions", region);

			return userDoc;
		}

		public static Document generateDocumentUserItems(int userId) {
			Document userDoc = generateDocument(userId);

			// Add items
			ArrayList<Document> items = new ArrayList<Document>();

			for (int itemId : getItemIds(userId)) {
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

			int startId = userId * userItems;
			int endId = (userId + 1) * (userItems);

			for (int i = startId; i < endId; i++) {
				itemIds.add(i);
			}

			return itemIds;
		}

		public static List<Integer> getBidIds(int userId) {
			ArrayList<Integer> bidIds = new ArrayList<Integer>();

			int startId = userId * userBids;
			int endId = (userId + 1) * (userBids);

			for (int i = startId; i < endId; i++) {
				bidIds.add(i);
			}

			return bidIds;
		}

		public static List<Integer> getCommentIds(int userId) {
			ArrayList<Integer> commentIds = new ArrayList<Integer>();

			int startId = (userId) * userComments;
			int endId = (userId + 1) * (userComments);

			for (int i = startId; i < endId; i++) {
				commentIds.add(i);
			}

			return commentIds;
		}
	}

	public static class Item implements DocumentModel {

		private final int itemId;

		public Item(int itemId) {
			this.itemId = itemId;
		}

		@Override
		public Map<String, Document> generateDocuments() {
			Map<String, Document> documentPerCollection = new HashMap<String, Document>();

			// create Items
			documentPerCollection.put("Items", generateDocument(this.itemId));

			// create Items | Bids
			documentPerCollection.put("ItemsBids", generateDocumentItemsBids(this.itemId));

			// create Items | Bids | Users
			documentPerCollection.put("ItemsBidsUsers", generateDocumentItemsBidsUsers(this.itemId));

			// create Items | Users | Regions
			documentPerCollection.put("ItemsUsersRegions", generateDocumentItemsUsersRegions(this.itemId));

			// create Items | Comments
			documentPerCollection.put("ItemsComments", generateDocumentItemsComments(this.itemId));

			// create Items | Users
			documentPerCollection.put("ItemsUsers", generateDocumentItemsUsers(this.itemId));

			return documentPerCollection;
		}

		public static Document generateDocument(int itemId) {
			Document doc = new Document("_id", itemId);

			doc.put("productTitle", RandomStringUtils.randomAlphanumeric(20)); // 20
			doc.put("price", RandomStringUtils.randomAlphanumeric(5)); // 5
			doc.put("type", RandomStringUtils.randomAlphanumeric(5)); // 5
			doc.put("date", RandomStringUtils.randomAlphanumeric(20)); // 20

			doc.put("description", RandomStringUtils.randomAlphanumeric(80));

			doc.put("id_seller", getUserId(itemId));

			return doc;
		}

		public static Document generateDocumentItemsUsers(int itemId) {
			Document itemDoc = generateDocument(itemId);

			itemDoc.append("users", User.generateDocument(getUserId(itemId)));

			return itemDoc;
		}

		public static Document generateDocumentItemsComments(int itemId) {
			Document itemDoc = generateDocument(itemId);
			ArrayList<Document> comments = new ArrayList<Document>();

			// Add each bid
			for (int commentId : getCommentIds(itemId)) {
				comments.add(Comment.generateDocument(commentId));
			}

			itemDoc.append("comments", comments);

			return itemDoc;
		}

		public static Document generateDocumentItemsBids(int itemId) {
			Document itemDoc = generateDocument(itemId);
			ArrayList<Document> bids = new ArrayList<Document>();

			// Add each bid
			for (int bidId : getBidIds(itemId)) {
				bids.add(Bid.generateDocument(bidId));
			}

			itemDoc.append("bids", bids);

			return itemDoc;
		}

		public static Document generateDocumentItemsBidsUsers(int itemId) {
			Document doc = generateDocument(itemId);
			ArrayList<Document> bidUsers = new ArrayList<Document>();

			// for each Bid get BidUser
			for (int bidId : getBidIds(itemId)) {
				bidUsers.add(Bid.generateDocumentBidsUsers(bidId));
			}

			doc.append("bids", bidUsers);

			return doc;
		}

		public static Document generateDocumentItemsUsersRegions(int itemId) {
			Document itemDoc = generateDocument(itemId);
			Document userRegion = User.generateDocumentUsersRegions(getUserId(itemId));

			itemDoc.append("users", userRegion);

			return itemDoc;
		}

		public static int getUserId(int itemId) {

			int userId = itemId / userItems;
			return userId;
		}

		public static List<Integer> getBidIds(int itemId) {
			ArrayList<Integer> bidIds = new ArrayList<Integer>();

			int startId = (itemId) * itemBids;
			int endId = (itemId + 1) * (itemBids);

			for (int i = startId; i < endId; i++) {
				bidIds.add(i);
			}

			return bidIds;
		}

		public static List<Integer> getCommentIds(int itemId) {
			ArrayList<Integer> commentIds = new ArrayList<Integer>();

			int startId = itemId * itemComments;
			int endId = (itemId + 1) * (itemComments);

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

			// create Bids|Users
			documentPerCollection.put("BidsUsers", generateDocumentBidsUsers(this.bidId));

			// Create Bids|Users|Items
			documentPerCollection.put("BidsUsersItems", generateDocumentBidsUsersItems(bidId));

			return documentPerCollection;
		}

		private static Document generateDocument(int bidId) {
			Document doc = new Document("_id", bidId);

			// amount - 5
			// date - 30

			doc.put("price", RandomStringUtils.randomAlphanumeric(5));
			doc.put("date", RandomStringUtils.randomAlphanumeric(30));

			doc.put("id_user", getUserId(bidId));
			doc.put("id_item", getItemId(bidId));

			return doc;
		}

		public static Document generateDocumentBidsUsers(int bidId) {
			Document bidUsers = generateDocument(bidId);

			int userId = getUserId(bidId);
			bidUsers.append("users", User.generateDocument(userId));

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
		public static Document generateDocument(int commentId) {
			Document doc = new Document("_id", commentId);

			doc.put("commentTitle", RandomStringUtils.randomAlphanumeric(20)); // 20
			doc.put("commentText", RandomStringUtils.randomAlphanumeric(70)); // 70
			doc.put("date", RandomStringUtils.randomAlphanumeric(20)); // 20
			doc.put("id_user", getUserId(commentId));
			doc.put("id_item", getItemId(commentId));

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

			doc.put("regionName", RandomStringUtils.randomAlphanumeric(10)); // 10

			return doc;
		}
	}
}
