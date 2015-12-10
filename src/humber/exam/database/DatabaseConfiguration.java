package humber.exam.database;

/**
 * Created by Jonathan on 10/5/2015.
 */
public final class DatabaseConfiguration {

	/**
	 * Default database configuration.
	 */
	public static final DatabaseConfiguration DEFAULT = new DatabaseConfiguration("dilbert.humber.ca", 1521, "AUTHOR", "N00456617", "oracle");

	/**
	 * The database server host
	 */
	private final String host;

	/**
	 * The database server port
	 */
	private final int port;

	/**
	 * The database on the server
	 */
	private final String database;

	/**
	 * The username of the server
	 */
	private final String username;

	/**
	 * The password of the server
	 */
	private final String password;

	/**
	 * Create a new configuration
	 *
	 * @param host     The host
	 * @param port     The port
	 * @param database The database
	 * @param username The username
	 * @param password The password
	 */
	public DatabaseConfiguration(String host, int port, String database, String username, String password) {
		this.host = host;
		this.port = port;
		this.database = database;
		this.username = username;
		this.password = password;
	}

	/**
	 * Get the database host
	 *
	 * @return The database host
	 */
	public String getHost() {
		return host;
	}

	/**
	 * Get the database port
	 *
	 * @return The port
	 */
	public int getPort() {
		return port;
	}

	/**
	 * Get the database name
	 *
	 * @return The database
	 */
	public String getDatabase() {
		return database;
	}

	/**
	 * Get the database username
	 *
	 * @return The database username
	 */
	public String getUsername() {
		return username;
	}


	/**
	 * Get the database password
	 *
	 * @return The database password
	 */
	public String getPassword() {
		return password;
	}

}