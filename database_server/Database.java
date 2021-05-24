package database_server;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.Queue;

public class Database implements Runnable {
	
	private ArrayList<Event> database;
	private Queue<String[]> input;
	private boolean running;

	public Database() {
		database = new ArrayList<Event>();
		input = new LinkedList<String[]>();
	}
	
	/**
	 * Adds an entry to the queue, is added to the database later by the main thread.
	 */
	public synchronized void addEntry(String[] split) {
		input.add(split);
	}
	
	/**
	 * Gets a list of all entries in the database past a certain date.
	 */
	public synchronized ArrayList<String> getEntries(String[] split) {
		ArrayList<String> events = new ArrayList<String>();
		try {
			DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd:hh'h'mm'm'ss's'SSS'Z'");
			Date date = (Date)formatter.parse(split[2]);
			
			for(Event event:database) {
				if(event.isAfter(date)) {
					events.add(event.toString());
				}
			}
			
			return events;
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return new ArrayList<String>();
	}
	
	/**
	 * Main database control thread- adds new entries and saves as needed.
	 */
	@Override
	public void run() {
		running = true;
		while(running) {
			boolean hasChange = false;
			while(input.peek() != null) {
				try {
					Event event = new Event(input.poll());
					if(!hasEvent(event)) {
						database.add(event);
						hasChange = true;
					}
				} catch (ParseException e) {}
			}
			if(hasChange) {
				Database.saveDatabase(this, Server.getDatabaseFile());
			}
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {}
		}
	}
	
	private boolean hasEvent(Event event) {
		for(Event e:database)
			if(e.equals(event))
				return true;
		return false;
	}
	
	public void stop() {
		running = false;
	}
	
	/**
	 * Loads specified file as a database. If the file does not contain a database, returns a new database object.
	 */
	public static Database loadDatabase(File file) {
		Database out = new Database();
		if(file.exists()) {
			try {
				FileReader fileIn = new FileReader(file);
				BufferedReader in = new BufferedReader(fileIn);
				
				String line;
				while((line = in.readLine()) != null) {
					try {
						out.database.add(new Event(line.split(";")));
					} catch (ParseException e) {}
				}
				
				in.close();
				fileIn.close();
			} catch (IOException e) {}
		}
		return out;
	}
	
	/**
	 * Saves database object to specified file.
	 */
	public static void saveDatabase (Database database, File file) {
		try {
			FileWriter fileOut = new FileWriter(file);
			BufferedWriter out = new BufferedWriter(fileOut);
			
			for(Event event: database.database) {
				out.write(event.toString());
				out.newLine();
			}
			
			out.close();
			fileOut.close();
		} catch (IOException e) {}
	}
	
	private static class Event {

		private final String group;
		private final String desc;
		private final String time;
		private final Date date;
		
		private Event(String[] def) throws ParseException {
			if(def.length < 4)
				throw new ParseException("Invalid Event Format",0);
			group = def[3];
			desc = def[2];
			time = def[1];
			DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd:hh'h'mm'm'ss's'SSS'Z'");
			date = (Date)formatter.parse(def[1]);
		}
		
		public boolean isAfter(Date date) {
			return this.date.after(date);
		}
		
		public String toString() {
			return "EVENT_DEFINITION;" + time + ";" + desc + ";" + group;
		}
		
		@Override
		public boolean equals(Object obj) {
			if(obj instanceof Event) {
				Event event = (Event)obj;
				return this.group.equals(event.group) 
						&& this.desc.equals(event.desc) 
						&& this.time.equals(event.time);
			} else {
				return false;
			}
		}
		
	}
	
}

