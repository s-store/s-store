import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.*;
import java.util.ArrayList;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import org.voltdb.VoltTable;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Site;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.HStoreConstants;
import edu.brown.utils.CollectionUtil;


public class BikeShareClient {
	final int port;
	String hostname;
	Catalog catalog;
	Client client;
	InputClientConnection icc;
	ServerSocket serverSocket;
	Socket api; //Connection to the Rest API
	InputStreamReader apiCall;
	OutputStreamWriter out;
	public static final long FAILED_CHECKOUT = -1;
	public static final long FAILED_CHECKIN = -2;
	public static final long FAILED_SIGNUP = -3;
	public static final long FAILED_POINT_ADD = -4;
	public static final long FAILED_ACCEPT_DISCOUNT = -5;
	public static final long NO_BIKE_CHECKED_OUT = -6;
	public static final long USER_ALREADY_HAS_BIKE = -7;
	public static final long USER_DOESNT_EXIST = -8;

	BikeShareClient(String hostname) {
		this.hostname = hostname;
		this.port = HStoreConstants.DEFAULT_PORT; //21212
		this.catalog = new Catalog();
		this.reconnect(hostname);
		try {
			this.serverSocket = new ServerSocket(6000);
		} catch (IOException e) {
			System.out.println("Error creating socket on port 6000");
			e.printStackTrace();
		}
	}

	public void reconnect(String hostname) {
		boolean connected = false;
		while (!connected) {
			try {
				this.icc = this.getClientConnection(hostname);
				connected = true;
			}
			catch (RuntimeException e) {
				connected = false;
			}
		}
		this.client = this.icc.client;
	}

	//Take a json message from the socket and parse it for the procedure
	//name and arguments.  Make a call to the specified procedure and return
	//the array of VoltTables.
	public VoltTable [] callStoredProcedure(JSONObject proc) throws JSONException {
		VoltTable [] results;
		System.out.println("Calling stored procedure");
		try {
			String procedureName = proc.getString("proc");
			ArrayList<Object> conversionList = new ArrayList<Object>();
			JSONArray args = proc.getJSONArray("args");
			for (int i = 0; i < args.length(); i++) {
				conversionList.add(args.get(i));
			}
			Object [] argumentList = conversionList.toArray();
			results = client.callProcedure(procedureName, argumentList).getResults();
			return results;
		} catch (JSONException e) {
			System.out.println("JSON object missing expected fields");
			e.printStackTrace();
		} catch (NoConnectionsException e) {
			System.out.println("Connection to S-Store was lost");
			e.printStackTrace();
			this.reconnect(hostname);
			return this.callStoredProcedure(proc);
		} catch (IOException e) {
			if (e.getMessage() != null)
				System.out.println(e.getMessage());
			e.printStackTrace();
		} catch (ProcCallException e) {
			System.out.println(e.getMessage());
			JSONObject error = new JSONObject();
			error.put("data", "");
			error.put("error", e.getMessage().split("\n")[0]);
			error.put("success", 0);
			sendJSON(error);
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println(e.getMessage());
			JSONObject error = new JSONObject();
			error.put("data", "");
			error.put("error", e.getMessage());
			error.put("success", 0);
			sendJSON(error);
			e.printStackTrace();
		}
		return null;
	}

	public ArrayList<String> parseResults(VoltTable vt) {
		System.out.println("Parsing results from S-Store");
		JSONObject j = new JSONObject();
		ArrayList<String> s = new ArrayList<String>();
		final int colCount = vt.getColumnCount();
		vt.resetRowPosition();
		while (vt.advanceRow()) {
			for (int col = 0; col < colCount; col++) {
				switch(vt.getColumnType(col)) {
				case INTEGER: case BIGINT:
					try {
						j.put(vt.getColumnName(col), vt.getLong(col));
					} catch (JSONException e) {
						System.out.println("Table column name is null");
						e.printStackTrace();
					}
					break;
				case STRING:
					try {
						j.put(vt.getColumnName(col), vt.getString(col));
					} catch (JSONException e) {
						System.out.println("Table column name is null");
						e.printStackTrace();
					}
					break;
				case DECIMAL:
					try {
						j.put(vt.getColumnName(col), vt.getDecimalAsBigDecimal(col));
					} catch (JSONException e) {
						System.out.println("Table column name is null");
						e.printStackTrace();
					}
					break;
				case FLOAT:
					try {
						j.put(vt.getColumnName(col), vt.getDouble(col));
					} catch (JSONException e) {
						System.out.println("Table column name is null");
						e.printStackTrace();
					}
					break;
				}
			}
			s.add(j.toString());
		}
		return s;
	}

	public String readString() {
		String procedureName;
		System.out.println("Reading in a line from " + api.toString());
		try {
			while ((procedureName = findNewLine()) == null) {
				System.out.println("Received a null string");
				api.close();
				api = serverSocket.accept(); //Client likely disconnected
				out = new OutputStreamWriter(api.getOutputStream(), "UTF-8");
				System.out.println("Connected to " + api.getInetAddress());
				apiCall = new InputStreamReader(api.getInputStream(), "UTF-8");
			}
			return procedureName;
		} catch (IOException e) {
			System.out.println("Unable to read from the Rest client");
			e.printStackTrace();
		}
		return null;
	}

	public String findNewLine() {
		String procedureName = "";
		int c;
		boolean found = false;
		try {
			while (!found) {
				c = apiCall.read();
				if ((char) c == '\n') {
					return procedureName += (char) c;
				}
				if (c == -1) {
					if (procedureName == "") 
						return null;
					return procedureName;
				}
				procedureName += (char) c;
			}
			return procedureName;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

	public void sendJSON(JSONObject j) {
		String jsonMessage = (j.toString() + "\n");
		try {
			out.write(jsonMessage, 0, jsonMessage.length());
			out.flush();
		} catch (IOException e) {
			System.out.println("Unable to write to the Rest client");
			System.out.println(api.toString());
			e.printStackTrace();
		}
	}

	public String errorMessage(long err, JSONObject proc) {
		try {
			if (err == FAILED_CHECKOUT)
				return "Rider: " + proc.getJSONArray("args").getInt(0) + " was unable to checkout a bike";
			if (err == FAILED_CHECKIN)
				return "Rider: " + proc.getJSONArray("args").getInt(0) + " was unable to checkin a bike";
			if (err == FAILED_SIGNUP)
				return "Unable to sign up rider";
			if (err == FAILED_POINT_ADD)
				return "Unable to add point";
			if (err == FAILED_ACCEPT_DISCOUNT)
				return "Unable to accept the discount";
			if (err == NO_BIKE_CHECKED_OUT)
				return "Rider: " + proc.getJSONArray("args").getInt(0) + " does not have a bike checked out";
			if (err == USER_ALREADY_HAS_BIKE)
				return "Rider: " + proc.getJSONArray("args").getInt(0) + " already has a bike checked out";
			if (err == USER_DOESNT_EXIST)
				return "Rider: " + proc.getJSONArray("args").getInt(0) + " does not exist";
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return "FATAL ERROR";
	}

	//Used to grab information about where S-Store is running and establish a
	//connection to the db
	private class InputClientConnection {
		final Client client;
		final String hostname;
		final int port;

		public InputClientConnection(Client client, String hostname, int port) {
			this.client = client;
			this.hostname = hostname;
			this.port = port;
		}
	} // CLASS

	private InputClientConnection getClientConnection(String host) {
		String hostname = null;
		int port = -1;

		// Fixed hostname
		if (host != null) {	
			if (host.contains(":")) {
				String split[] = host.split("\\:", 2);
				hostname = split[0];
				port = Integer.valueOf(split[1]);
			} else {
				hostname = host;
				port = this.port;
			}
		}
		// Connect to random host and using a random port that it's listening on
		else if (this.catalog != null) {
			Site catalog_site = CollectionUtil.random(CatalogUtil.getAllSites(this.catalog));
			hostname = catalog_site.getHost().getIpaddr();
			port = catalog_site.getProc_port();
		}
		assert(hostname != null);
		assert(port > 0);

		Client client = ClientFactory.createClient(128, null, false, null);
		try {
			client.createConnection(null, hostname, port, "user", "password");
			System.out.println("BatchRunner: connection is ok ... ");
		} catch (Exception ex) {
			String msg = String.format("Failed to connect to HStoreSite at %s:%d", hostname, port);
			try {
				client.close();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			throw new RuntimeException(msg);
		}
		return new InputClientConnection(client, hostname, port);
	}

	public static void main(String [] args) {
		String proc;
		JSONObject j;
		VoltTable [] results;
		BikeShareClient myc;
		if (args[0] != null) {
			myc = new BikeShareClient(args[0]);
		} else {
			myc = new BikeShareClient("localhost");
		}
		try {
			myc.api = myc.serverSocket.accept();
			myc.out = new OutputStreamWriter(myc.api.getOutputStream(), "UTF-8");
			System.out.println("Connected to " + myc.api.getInetAddress());
			myc.apiCall = new InputStreamReader(myc.api.getInputStream(), "UTF-8");
			while (true) {
				ArrayList<String> rows = new ArrayList<String>();
				JSONArray jsonArray = new JSONArray();
				proc = myc.readString();
				JSONObject calledProc = new JSONObject(proc);
				System.out.println("Received input stream");
				while ((results = myc.callStoredProcedure(calledProc)) == null) {
					proc = myc.readString();
					System.out.println("Creating new json object");
					calledProc = new JSONObject(proc);
				}
				j = new JSONObject();
				for (VoltTable vt: results) {
					if (vt.hasColumn("")) {
						long error = vt.asScalarLong();
						if (error < 0) {
							j.put("error", myc.errorMessage(error, calledProc));
							j.put("success", 0);
						} else if (error > 0){
							j.put("error", "");
							j.put("success", 1);
							jsonArray.put(error);
						} else {
							j.put("error", "");
							j.put("success", 1);
						}
						j.put("data", jsonArray);
						rows.add(String.valueOf(vt.asScalarLong()));
					} else {
						for (String s: myc.parseResults(vt)) {
							jsonArray.put(new JSONObject(s));
						}
						j.put("data", jsonArray);
						j.put("error", "");
						j.put("success", 1);
					}
					System.out.println("Sending json to " + myc.api.toString());
					System.out.println("Called procedure " + proc);
					myc.sendJSON(j);
					System.out.println("Done sending rows");
				}
			}
		} catch (NoConnectionsException e) {
			System.out.println("Failure to create S-Store client connection");
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("Unable to connect to Rest client");
			e.printStackTrace();
		} catch (JSONException e) {
			// This exception should never get thrown.
			// put() throws this in the event of a null string or an incorrect type being passed
			// as an argument.  The arguments are hard coded, so something catastrophic would have
			// to occur.
			e.printStackTrace();
		}
	}
}
