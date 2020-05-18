package site.ycsb.db;

import java.util.ArrayList;
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
		System.out.println(User.getItemIds(1));
		System.out.println(User.getItemIds(2));

		System.out.println(User.getBidIds(1));

		System.out.println(Item.getUserId(0));
		System.out.println(Item.getUserId(4));
		System.out.println(Item.getUserId(10));
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

		public static DocumentGenerator getInstance() {
			if (instance == null)
				instance = new DocumentGenerator();

			return instance;
		}

		/**
		 * One user triggers the creation of several items, bids, comments, etc.
		 */
		public void createUser() {
			int userId = recordId;
			
			new User(userId).persistDocuments();

			List<Integer> itemIds = User.getItemIds(userId);
			createItems(itemIds);

			List<Integer> commentIds = User.getCommentIds(userId);
			createComments(commentIds);
			
			List<Integer> bidIds = User.getBidIds(userId);
			createBids(bidIds);
		}

		private void createBids(List<Integer> bidIds) {
			for (int bidId: bidIds)
				new Bid(bidId).persistDocuments();
		}

		private void createComments(List<Integer> commentIds) {
			for (int commentId : commentIds) {
				new Comment(commentId).persistDocuments();
			}
		}

		public void createItems(List<Integer> itemIds) {
			for (int itemId : itemIds)
				new Item(itemId).persistDocuments();
		}

	}
	
	public interface DocumentModel{
		
		/**
		 * Persist all possible variants of the document and its children.
		 */
		public void persistDocuments();
	
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
		public void persistDocuments() {
			
		}
		
		/**
		 * Create 1-level user document
		 */
		public static Document generateDocument(int userId) {
			Document toInsert = new Document("_id", userId);
		    
			toInsert.put("firstName", "name");
			
			return toInsert;
		}
		
		public int getRegionId(int userId) {
			return userId % nrOfRegions;
		}

		public static List<Integer> getItemIds(int userId) {
			ArrayList<Integer> itemIds = new ArrayList<Integer>();

			int startId = (userId - 1) * userItems;
			int endId = userId * (userItems);

			for (int i = startId; i < endId; i++) {
				itemIds.add(i);
			}

			return itemIds;
		}

		public static List<Integer> getBidIds(int userId) {
			ArrayList<Integer> bidIds = new ArrayList<Integer>();

			int startId = (userId - 1) * userBids;
			int endId = userId * (userBids);

			for (int i = startId; i < endId; i++) {
				bidIds.add(i);
			}

			return bidIds;
		}

		public static List<Integer> getCommentIds(int userId) {
			ArrayList<Integer> commentIds = new ArrayList<Integer>();

			int startId = (userId - 1) * userComments;
			int endId = userId * (userComments);

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
		public void persistDocuments() {
			// TODO Auto-generated method stub
			
		}
		
		public static int getUserId(int itemId) {

			int userId = itemId / userItems;
			return userId;
		}
		
		public static List<Integer> getBidIds(int itemId) {
			ArrayList<Integer> bidIds = new ArrayList<Integer>();

			int startId = (itemId - 1) * itemBids;
			int endId = itemId * (itemBids);

			for (int i = startId; i < endId; i++) {
				bidIds.add(i);
			}

			return bidIds;
		}

		public static List<Integer> getCommentIds(int itemId) {
			ArrayList<Integer> commentIds = new ArrayList<Integer>();

			int startId = (itemId - 1) * itemComments;
			int endId = itemId * (itemComments);

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
		public void persistDocuments() {
			
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
		public void persistDocuments() {
			
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
}
