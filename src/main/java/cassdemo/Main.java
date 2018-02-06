package cassdemo;

import java.io.IOException;
import java.util.Properties;

import cassdemo.backend.BackendException;
import cassdemo.backend.BackendSession;
import cassdemo.backend.Flight;
import cassdemo.backend.FlightConsole;

public class Main {

	private static final String PROPERTIES_FILENAME = "config.properties";

	public static void main(String[] args) throws IOException, BackendException {
		String contactPoint = null;
		String keyspace = null;

		Properties properties = new Properties();
		try {
			properties.load(Main.class.getClassLoader().getResourceAsStream(PROPERTIES_FILENAME));

			contactPoint = properties.getProperty("contact_point");
			keyspace = properties.getProperty("keyspace");
		} catch (IOException ex) {
			ex.printStackTrace();
		}
			
		FlightConsole.backendSession = new BackendSession(contactPoint, keyspace);

		//session.addFlight(1,10);

		//session.bookSeats(1, "krzys", 6);

		//Flight fl = session.getFlight(1);
		//System.out.println(fl.Print());

		//session.unBookSeats(1, "krzys", 2);

		//fl = session.getFlight(1);
        FlightConsole.stressTest();
		//System.out.println(fl.Print());
        FlightConsole.runConsole();

		System.exit(0);
	}
}
