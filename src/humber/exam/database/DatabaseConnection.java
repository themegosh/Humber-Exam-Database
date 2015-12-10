package humber.exam.database;

import java.sql.*;

/**
 * Created by Jonathan on 10/5/2015.
 */
public final class DatabaseConnection {

	/**
	 * Create a new instance of the DatabaseConnection class.
	 *
	 * @return the connection
	 */
	public static DatabaseConnection open() {
		return new DatabaseConnection(DatabaseConfiguration.DEFAULT);
	}

	public void close() {
		try {
			connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * The configuration for the database.
	 */
	private final DatabaseConfiguration config;

	/**
	 * The database connection
	 */
	private Connection connection;

	/**
	 * Creates a <code>Statement</code> object for sending
	 * SQL statements to the database.
	 *
	 * @param config the database configuration
	 */
	private DatabaseConnection(DatabaseConfiguration config) {
		this.config = config;
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public Connection getConnection() {
		return connection;
	}

	/**
	 * Opens a new connection to the database.
	 */
	public void connect() {
		try {
			connection = DriverManager.getConnection("jdbc:oracle:thin:@" + config.getHost() + ":" + config.getPort() + ":grok", config.getUsername(), config.getPassword());
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Executes a SQL query to the server
	 *
	 * @param query the string to be executed
	 * @return result of the query
	 */
	public Result execute(String query) {
		if (connection == null) {
			connect();
		}
		try {
			Statement stmt = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			return new Result(stmt.executeQuery(query));
		} catch (Throwable e) {
			e.printStackTrace();
		}
		return null;
	}

	public boolean addStudent(String firstname, String lastname, int studentId) {
		if (hasStudent(studentId)) {
			System.err.println("Student with that StudentID already exists!");
			return false;
		}
		String query = String.format("INSERT INTO STUDENT (FIRST_NAME, LAST_NAME, STUDENT_ID) VALUES (%s, %s, %d)", quo(firstname), quo(lastname), studentId);
		return execute(query) != null;
	}

	public boolean hasStudent(int studentId) {
		String query = "SELECT * FROM STUDENT WHERE STUDENT_ID = " + studentId;
		try {
			return execute(query).set().isBeforeFirst();
		} catch (Throwable e) {
			// e.printStackTrace();
		}
		return false;
	}

	public boolean removeStudent(int studentId) {
		if (!hasStudent(studentId)) {
			return false;
		}
		String query = "DELETE FROM STUDENT WHERE STUDENT_ID = " + studentId;
		return execute(query) != null;
	}

	public boolean hasCourse(String course_code) {
		String query = "SELECT * FROM COURSE WHERE COURSE_CODE = " + quo(course_code);
		try {
			return execute(query).set().isBeforeFirst();
		} catch (Throwable e) {
			// e.printStackTrace();
		}
		return false;
	}

	public boolean addCourse(String course_code, String course_name, String course_desc, String exam, String room_num, String program_code, int teacher_id) {
		if (hasCourse(course_code)) {
			System.err.println("Course with that course code already exists!");
			return false;
		}
		String query = String.format("INSERT INTO PROGRAM VALUES (%s, %s, %s, %s, %s,%s, %d)", quo(course_code), quo(course_name), quo(course_desc), quo(exam), quo(room_num), quo(program_code), teacher_id);
		return execute(query) != null;
	}

	public boolean removeCourse(String course_code) {
		if (!hasCourse(course_code)) {
			return false;
		}
		String query = "DELETE FROM COURSE WHERE COURSE_CODE = " + quo(course_code);
		return execute(query) != null;
	}

	public boolean hasProgram(String programCode) {
		String query = "SELECT * FROM PROGRAM WHERE PROGRAM_CODE = " + quo(programCode);
		try {
			return execute(query).set().isBeforeFirst();
		} catch (Throwable e) {
			// e.printStackTrace();
		}
		return false;
	}

	public boolean addProgram(String program_code, String program_name) {
		if (hasProgram(program_code)) {
			System.err.println("Program already exists!");
			return false;
		}
		String query = String.format("INSERT INTO PROGRAM VALUES (%s, %s)", quo(program_code), quo(program_name));
		return execute(query) != null;
	}

	public boolean removeProgram(String programCode) {
		if (!hasProgram(programCode)) {
			return false;
		}
		String query = "DELETE FROM PROGRAM WHERE PROGRAM_CODE = " + quo(programCode);
		return execute(query) != null;
	}

	public boolean changePeriodTime(String period_id, Timestamp start, Timestamp end) {
		String query = "UPDATE PERIOD SET START_TIME = " + toDate(start) + ", END_TIME = " + toDate(end) + " WHERE PERIOD_ID = " + quo(period_id);
		return execute(query) != null;
	}

	public Result getExamFor(String day_of_week, String period_id) {
		String query = "SELECT E.COURSE_CODE, R.ROOM_NUM, E.DAY_OF_WEEK, P.START_TIME, P.END_TIME " +
				"FROM EXAM E, PERIOD P, ROOM R " +
				"WHERE E.DAY_OF_WEEK = " + quo(day_of_week) + " " +
				"AND E.PERIOD_ID = " + quo(period_id) + " " +
				"AND E.ROOM_NUM = R.ROOM_NUM " +
				"AND E.PERIOD_ID = P.PERIOD_ID";
		return execute(query);
	}

	public Result getExamFor(String day_of_week, String period_id, int teacher_id) {
		String query = "SELECT E.COURSE_CODE, T.TEACHER_ID, E.PERIOD_ID, E.DAY_OF_WEEK, E.START_TIME, E.END_TIME, R.ROOM_NUM, R.BUILDING_ID " +
				"FROM EXAM E, DAY_SCHEDULE DS, ROOM R, TEACHER T, COURSE C " +
				"WHERE DS.DAY_OF_WEEK = " + quo(day_of_week) + " " +
				"AND E.PERIOD_ID = " + quo(period_id) + " " +
				"AND T.TEACHER_ID = " + teacher_id + " " +
				"AND T.TEACHER_ID = C.TEACHER_ID " +
				"AND C.COURSE_CODE = E.COURSE_CODE " +
				"AND E.PERIOD_ID = DS.PERIOD_ID " +
				"AND E.ROOM_NUM = R.ROOM_NUM " +
				"AND E.DAY_OF_WEEK = DS.DAY_OF_WEEK";
		return execute(query);
	}

	public Result getExamForProgram(String program) {
		String query = "SELECT E.COURSE_CODE " +
				"FROM EXAM E, COURSE C, PROGRAM P " +
				"WHERE P.PROGRAM_CODE =  " + quo(program) + " " +
				"AND C.PROGRAM_CODE = P.PROGRAM_CODE " +
				"AND C.COURSE_CODE = E.COURSE_CODE";
		return execute(query);
	}

	public Result getExamForCourseCode(String course_code) {
		String query = "SELECT E.COURSE_CODE " +
				"FROM EXAM E, COURSE C, PROGRAM P " +
				"WHERE C.COURSE_CODE =  " + quo(course_code) + " " +
				"AND C.PROGRAM_CODE = P.PROGRAM_CODE " +
				"AND C.COURSE_CODE = E.COURSE_CODE";
		return execute(query);
	}

	public Result getPeriods() {
		String query = "SELECT PERIOD_ID, START_TIME, END_TIME FROM PERIOD ORDER BY PERIOD_ID";
		return execute(query);
	}

	public Result getPrograms() {
		String query = "SELECT * FROM PROGRAM";
		return execute(query);
	}

	public Result getCourses() {
		String query = "SELECT * FROM COURSE";
		return execute(query);
	}

	public Result getPeriod(String period_id) {
		String query = "SELECT * FROM PERIOD WHERE PERIOD_ID = " + quo(period_id);
		return execute(query);
	}

	public Result getProgram(String program_code) {
		String query = "SELECT * FROM PROGRAM WHERE PROGRAM_CODE = " + quo(program_code);
		return execute(query);
	}

	public Result getRoom(String room_num) {
		String query = "SELECT * FROM ROOM WHERE ROOM_NUM = " + quo(room_num);
		return execute(query);
	}

	public Result getStudent(int student_id) {
		String query = "SELECT * FROM STUDENT WHERE STUDENT_ID = " + student_id;
		return execute(query);
	}

	public Result getUserById(int user_id) {
		String query = "SELECT * FROM USER WHERE ID = " + user_id;
		return execute(query);
	}

	public Result getCourseForCourseCode(String course_code) {
		String query = "SELECT * FROM COURSE WHERE COURSE_CODE = " + quo(course_code);
		return execute(query);
	}

	public Result getDay(String day_of_week) {
		String query = "SELECT * FROM DAY WHERE DAY_OF_WEEK = " + quo(day_of_week);
		return execute(query);
	}

	public Result getDaySchedule(String period_id) {
		String query = "SELECT * FROM DAY_SCHEDULE WHERE PERIOD_ID = " + quo(period_id);
		return execute(query);
	}

	public Result getDayScheduleForDayOfWeek(String day_of_week) {
		String query = "SELECT * FROM DAY_SCHEDULE WHERE DAY_OF_WEEK = " + quo(day_of_week);
		return execute(query);
	}

	public Result getDaySchedule(String period_id, String day_of_week) {
		String query = "SELECT * FROM DAY_SCHEDULE WHERE PERIOD_ID  = " + quo(period_id) + "AND DAY_OF_WEEK = " + quo(day_of_week);
		return execute(query);
	}

	public Result getEnrollment(int student_id) {
		String query = "SELECT * FROM ENROLLMENT WHERE STUDENT_ID = " + student_id;
		return execute(query);
	}

	public Result getExam(String room_num, String period_id, String day_of_week) {
		String query = "SELECT * FROM EXAM WHERE ROOM_NUM = " + quo(room_num) + " AND PERIOD_ID = " + quo(period_id) + " AND DAY_OF_WEEK = " + quo(day_of_week);
		return execute(query);
	}

	public Result getExamsOnDay(String day_of_week) {
		String query = "SELECT E.COURSE_CODE, E.PERIOD_ID, E.DAY_OF_WEEK FROM EXAM E, DAY_SCHEDULE DS WHERE DS.DAY_OF_WEEK = " + quo(day_of_week) + " AND E.PERIOD_ID = DS.PERIOD_ID AND E.DAY_OF_WEEK = DS.DAY_OF_WEEK";
		return execute(query);
	}

	public Result searchForExam(String day_of_week, String period_id) {
		String query = "SELECT E.COURSE_CODE FROM EXAM E, DAY_SCHEDULE DS WHERE DS.DAY_OF_WEEK = " + quo(day_of_week) + " AND DS.PERIOD_ID = " + quo(period_id) + " AND E.PERIOD_ID = DS.PERIOD_ID AND E.DAY_OF_WEEK = DS.DAY_OF_WEEK";
		return execute(query);
	}

	public Result getExamsForRoom(String roomNum) {
		String query = "SELECT E.COURSE_CODE " +
				"FROM EXAM E, COURSE C, PROGRAM P " +
				"WHERE C.ROOM_NUM =  " + quo(roomNum) + " " +
				"AND C.PROGRAM_CODE = P.PROGRAM_CODE " +
				"AND C.COURSE_CODE = E.COURSE_CODE";
		return execute(query);
	}

	public Result getExamsForTeacher(int teacherId) {
		String query = "SELECT E.COURSE_CODE, START_TIME, END_TIME FROM EXAM E, COURSE C WHERE E.COURSE_CODE = C.COURSE_CODE AND TEACHER_ID = " + teacherId;
		return execute(query);
	}

	public boolean hasExam(String courseCode, String roomNumber, String day_of_week, Timestamp start_time, Timestamp end_time) {
		String query = "SELECT * FROM EXAM WHERE COURSE_CODE = " + quo(courseCode) + " AND ROOM_NUM = " + quo(roomNumber) + " AND DAY_OF_WEEK = " + quo(day_of_week) + "  AND END_TIME = " + toDate(end_time) + " AND START_TIME = " + toDate(start_time);
		try {
			return execute(query).set().isBeforeFirst();
		} catch (Throwable e) {
			// e.printStackTrace();
		}
		return false;
	}

	public boolean addExam(String courseCode, String roomNumber, String day_of_week, Timestamp start_time, Timestamp end_time) {
		if (hasExam(courseCode, roomNumber, day_of_week, start_time, end_time)) {
			System.err.println("There is already an exam in that room!");
			return false;
		}
		String query = String.format("INSERT INTO EXAM VALUES (%s, %s, %s, %s, %s)", quo(courseCode), quo(roomNumber), quo(day_of_week), toDate(start_time), toDate(end_time));
		return execute(query) != null;
	}

	public boolean removeExam(String courseCode, String roomNumber, String periodId, String day_of_week, Timestamp start_time, Timestamp end_time) {
		if (!hasExam(courseCode, roomNumber, day_of_week, start_time, end_time)) {
			return false;
		}
		String query = "DELETE FROM EXAM WHERE COURSE_CODE = " + quo(courseCode) + " AND ROOM_NUM = " + quo(roomNumber) + " AND PERIOD_ID = " + quo(periodId) + " AND DAY_OF_WEEK = " + quo(day_of_week) + "  AND END_TIME = " + toDate(end_time) + " AND START_TIME = " + toDate(start_time);
		return execute(query) != null;
	}

	public boolean hasRoom(String room_num, String room_desc, String building_id) {
		String query = "SELECT * FROM ROOM WHERE ROOM_NUM = " + quo(room_num) + " AND ROOM_DESC = " + quo(room_desc) + " AND BUILDING_ID = " + quo(building_id);
		try {
			return execute(query).set().isBeforeFirst();
		} catch (Throwable e) {
			// e.printStackTrace();
		}
		return false;
	}

	public boolean addRoom(String room_num, String room_desc, String building_id) {
		if (hasRoom(room_num, room_desc, building_id)) {
			System.err.println("That room already exists in the database!");
			return false;
		}
		String query = String.format("INSERT INTO ROOM VALUES (%s, %s, %s)", quo(room_num), quo(room_desc), quo(building_id));
		return execute(query) != null;
	}

	public boolean removeRoom(String room_num, String room_desc, String building_id) {
		if (!hasRoom(room_num, room_desc, building_id)) {
			return false;
		}
		String query = "DELETE FROM ROOM WHERE ROOM_NUM = " + quo(room_num) + " AND ROOM_DESC = " + quo(room_desc) + " AND BUILDING_ID = " + quo(building_id);
		return execute(query) != null;
	}

	public boolean hasEnrollment(int student_id, String program_code, String semester, String section) {
		String query = "SELECT * FROM ENROLLMENT WHERE STUDENT_ID = " + student_id + " AND PROGRAM_CODE = " + quo(program_code) + " AND SEMESTER = " + quo(semester) + " AND SECTION = " + quo(section);
		try {
			return execute(query).set().isBeforeFirst();
		} catch (Throwable e) {
			// e.printStackTrace();
		}
		return false;
	}

	public boolean addEnrollment(int student_id, String program_code, String semester, String section) {
		if (hasEnrollment(student_id, program_code, semester, section)) {
			System.err.println("Duplicate enrollment!");
			return false;
		}
		String query = String.format("INSERT INTO ENROLLMENT VALUES (%d, %s, %s, %s, %s, %s)", student_id, quo(program_code), quo(semester), quo(section));
		return execute(query) != null;
	}

	public boolean removeEnrollment(int student_id, String program_code, String semester, String section) {
		if (!hasEnrollment(student_id, program_code, semester, section)) {
			return false;
		}
		String query = "DELETE FROM ENROLLMENT WHERE STUDENT_ID = " + student_id + " AND PROGRAM_CODE = " + quo(program_code) + " AND SEMESTER = " + quo(semester) + " AND SECTION = " + quo(section);
		return execute(query) != null;
	}
        
	public boolean hasUser(int id) {
		String query = "SELECT * FROM USERS WHERE ID = " + id;
		try {
			return execute(query).set().isBeforeFirst();
		} catch (Throwable e) {
			// e.printStackTrace();
		}
		return false;
	}
        
	public boolean addUser(int id, String password, String first_name, String last_name, int access) {
		if (hasUser(id)) {
			System.err.println("That user already exists!");
			return false;
		}
		String query = String.format("INSERT INTO USERS VALUES (%d, %s, %s, %s, %d)", id, quo(password), quo(first_name), quo(last_name), access);
		return execute(query) != null;
	}
        
        public Result getUser(String user_id, String user_password) {
            String query = "SELECT * FROM USERS WHERE ID = " + quo(user_id) + " AND PASSWORD = " + quo(user_password);
            return execute(query);
        }

	public boolean removeUser(int id) {
		if (!hasUser(id)) {
			return false;
		}
		String query = "DELETE FROM USERS WHERE ID = " + id;
		return execute(query) != null;
	}

	public boolean hasDaySchedule(String period_id, String day_of_week) {
		String query = "SELECT * FROM DAY_SCHEDULE WHERE PERIOD_ID = " + quo(period_id) + " AND DAY_OF_WEEK = " + quo(day_of_week);
		try {
			return execute(query).set().isBeforeFirst();
		} catch (Throwable e) {
			// e.printStackTrace();
		}
		return false;
	}

	public boolean addDaySchedule(String period_id, String day_of_week) {
		if (hasDaySchedule(period_id, day_of_week)) {
			System.err.println("That day schedule already exists!");
			return false;
		}
		String query = String.format("INSERT INTO DAY_SCHEDULE VALUES (%s, %s)", quo(period_id), quo(day_of_week));
		return execute(query) != null;
	}

	public boolean removeDaySchedule(String period_id, String day_of_week) {
		if (!hasDaySchedule(period_id, day_of_week)) {
			return false;
		}
		String query = "DELETE FROM DAY_SCHEDULE WHERE PERIOD_ID = " + quo(period_id) + " AND DAY_OF_WEEK = " + quo(day_of_week);
		return execute(query) != null;
	}

	public boolean hasDay(String day_of_week) {
		String query = "SELECT * FROM DAY WHERE DAY_OF_WEEK = " + quo(day_of_week);
		try {
			return execute(query).set().isBeforeFirst();
		} catch (Throwable e) {
			// e.printStackTrace();
		}
		return false;
	}

	public boolean addDay(String day_of_week) {
		if (hasDay(day_of_week)) {
			System.err.println("That day already exists!");
			return false;
		}
		String query = String.format("INSERT INTO DAY VALUES (%s)", quo(day_of_week));
		return execute(query) != null;
	}

	public boolean removeDay(String day_of_week) {
		if (!hasDay(day_of_week)) {
			return false;
		}
		String query = "DELETE FROM DAY WHERE DAY_OF_WEEK = " + quo(day_of_week);
		return execute(query) != null;
	}

	public boolean hasPeriod(String period_id) {
		String query = "SELECT * FROM PERIOD WHERE PERIOD_ID = " + quo(period_id);
		try {
			return execute(query).set().isBeforeFirst();
		} catch (Throwable e) {
			// e.printStackTrace();
		}
		return false;
	}

	public boolean addPeriod(String period_id, Timestamp start_time, Timestamp end_time) {
		if (hasPeriod(period_id)) {
			System.err.println("That period already exists!");
			return false;
		}
		String query = String.format("INSERT INTO PERIOD VALUES (%s, %s, %s)", quo(period_id), toDate(start_time), toDate(end_time));
		return execute(query) != null;
	}

	public boolean removePeriod(String period_id) {
		if (!hasPeriod(period_id)) {
			return false;
		}
		String query = "DELETE FROM PERIOD WHERE PERIOD_ID = " + quo(period_id);
		return execute(query) != null;
	}
        
	public String quo(Object s) {
		return "'" + s + "'";
	}

	public String toDate(Timestamp ts) {
		return "to_date(" + quo(ts.toString().replaceAll("\\.[^.]*$", "")) + ", 'YYYY/MM/DD HH24:MI:SS')";
	}
}

