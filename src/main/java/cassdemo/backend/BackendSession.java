package cassdemo.backend;

//import com.sun.rowset.internal.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

//import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/*
 * For error handling done right see: 
 * https://www.datastax.com/dev/blog/cassandra-error-handling-done-right
 * 
 * Performing stress tests often results in numerous WriteTimeoutExceptions, 
 * ReadTimeoutExceptions (thrown by Cassandra replicas) and 
 * OpetationTimedOutExceptions (thrown by the client). Remember to retry
 * failed operations until success (it can be done through the RetryPolicy mechanism:
 * https://stackoverflow.com/questions/30329956/cassandra-datastax-driver-retry-policy )
 */

public class BackendSession {

	private static final Logger logger = LoggerFactory.getLogger(BackendSession.class);

	public static BackendSession instance = null;

	private Session session;

	public BackendSession(String contactPoint, String keyspace) throws BackendException {

		Cluster cluster = Cluster.builder().addContactPoint(contactPoint).build();
		try {
			session = cluster.connect(keyspace);
		} catch (Exception e) {
			throw new BackendException("Could not connect to the cluster. " + e.getMessage() + ".", e);
		}
		prepareStatements();
	}

	private static PreparedStatement SELECT_FROM_FREE_SEATS_BY_FLIGHT;
	private static PreparedStatement INSERT_INTO_FREE_SEATS;
	private static PreparedStatement DELETE_FROM_FREE_SEATS;

	private static PreparedStatement INSERT_INTO_OCCUPIED_SEATS;
	private static PreparedStatement SELECT_FROM_OCCUPIED_SEATS_BY_FLIGHT;
	private static PreparedStatement DELETE_FROM_OCCUPIED_SEATS;

	private static PreparedStatement INSERT_INTO_CUSTOMERS_RESERVATIONS;
	private static PreparedStatement SELECT_FROM_CUSTOMERS_RESERVATIONS_BY_CUSTOMER;
	private static PreparedStatement SELECT_FROM_CUSTOMERS_RESERVATIONS_BY_CUSTOMER_AND_FLIGHT;
	private static PreparedStatement DELETE_FROM_CUSTOMERS_RESERVATIONS;

	private static PreparedStatement INSERT_INTO_FLIGHTS;
    private static PreparedStatement SELECT_FROM_FLIGHTS;

    private static PreparedStatement SELECT_FROM_AIRPLANES;



	private void prepareStatements() throws BackendException {
		try {
			SELECT_FROM_FREE_SEATS_BY_FLIGHT = session.prepare(
					"SELECT * FROM FreeSeats WHERE flightId = ?;");
			INSERT_INTO_FREE_SEATS = session.prepare(
					"INSERT INTO FreeSeats (flightId, seat) VALUES (?, ?);");
			DELETE_FROM_FREE_SEATS = session.prepare(
					"DELETE FROM FreeSeats WHERE flightId = ? AND seat = ?;");

			SELECT_FROM_OCCUPIED_SEATS_BY_FLIGHT = session.prepare(
					"SELECT * FROM OccupiedSeats WHERE flightId = ?;");
			INSERT_INTO_OCCUPIED_SEATS = session.prepare(
					"INSERT INTO OccupiedSeats (flightId, seat, customer) VALUES (?, ?, ?);");
			DELETE_FROM_OCCUPIED_SEATS = session.prepare(
					"DELETE FROM OccupiedSeats WHERE flightId = ? AND seat = ?;");

			SELECT_FROM_CUSTOMERS_RESERVATIONS_BY_CUSTOMER_AND_FLIGHT = session.prepare(
					"SELECT * FROM CustomersReservations WHERE customer = ? AND flightId = ?;");
            SELECT_FROM_CUSTOMERS_RESERVATIONS_BY_CUSTOMER = session.prepare(
                    "SELECT * FROM CustomersReservations WHERE customer = ?;");
			INSERT_INTO_CUSTOMERS_RESERVATIONS = session.prepare(
					"INSERT INTO CustomersReservations (customer, flightId, seat) VALUES (?, ?, ?);");
			DELETE_FROM_CUSTOMERS_RESERVATIONS = session.prepare(
					"DELETE FROM CustomersReservations WHERE customer = ? AND flightId = ? AND seat = ?;");

            INSERT_INTO_FLIGHTS = session.prepare(
                    "INSERT INTO Flights (Id, Name, Origin, Destination) VALUES (?, ?, ?, ?);");
            SELECT_FROM_FLIGHTS = session.prepare(
                "SELECT * FROM Flights;");
            SELECT_FROM_AIRPLANES = session.prepare(
                    "SELECT * FROM Airplanes;");
		} catch (Exception e) {
			throw new BackendException("Could not prepare statements. " + e.getMessage() + ".", e);
		}

		logger.info("Statements prepared");
	}

	public Flight getFlight(int filghtId) throws BackendException {
		StringBuilder builder = new StringBuilder();
		BoundStatement bs = new BoundStatement(SELECT_FROM_FREE_SEATS_BY_FLIGHT);
		bs.bind(filghtId);

		ResultSet rs = null;

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

		Flight flight = new Flight();
		for (Row row : rs) {
			int rSeat = row.getInt("seat");
			flight.getFreeSeats().add(rSeat);
		}

		bs = new BoundStatement(SELECT_FROM_OCCUPIED_SEATS_BY_FLIGHT);
		bs.bind(filghtId);

		rs = null;

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

		for (Row row : rs) {
			int rSeat = row.getInt("seat");
			String rCustomer = row.getString("customer");
			flight.getOccupiedSeats().put(rSeat, rCustomer);
		}

		return flight;
	}

	public boolean bookSeats(int flightId, String customer, int count) throws BackendException
	{
		StringBuilder builder = new StringBuilder();
		BoundStatement bs = new BoundStatement(SELECT_FROM_FREE_SEATS_BY_FLIGHT);
		bs.bind(flightId);

		ResultSet rs = null;

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

		Flight flight = new Flight();
		for (Row row : rs) {
			int rSeat = row.getInt("seat");
			flight.getFreeSeats().add(rSeat);
		}

		if(flight.getFreeSeats().size() < count)
			return false;

		int processed = 0;
		for (Integer rSeat : flight.getFreeSeats()) {
			if(processed >= count)
				break;

			bs = new BoundStatement(DELETE_FROM_FREE_SEATS);
			bs.bind(flightId, rSeat);
			try {
				session.execute(bs);
			} catch (Exception e) {
				throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
			}

			bs = new BoundStatement(INSERT_INTO_OCCUPIED_SEATS);
			bs.bind(flightId, rSeat, customer);
			try {
				session.execute(bs);
			} catch (Exception e) {
				throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
			}

			bs = new BoundStatement(INSERT_INTO_CUSTOMERS_RESERVATIONS);
			bs.bind(customer, flightId, rSeat);
			try {
				session.execute(bs);
			} catch (Exception e) {
				throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
			}

			processed++;
		}


		return true;
	}

	public List<SeatID> getAllCustomerReservations(String customer) throws BackendException {
        BoundStatement bs = new BoundStatement(
                SELECT_FROM_CUSTOMERS_RESERVATIONS_BY_CUSTOMER
        );
        bs.bind(customer);

        ResultSet rs = null;

        try {
            rs = session.execute(bs);
        } catch (Exception e) {
            throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
        }

        List<SeatID> reservations = new ArrayList<>();
        for(Row row : rs) {
            reservations.add(new SeatID(row.getInt("flightid"), row.getInt("seat")));
        }
        return reservations;
    }

	public boolean unBookSeats(int flightId, String customer, int count) throws BackendException
	{
		StringBuilder builder = new StringBuilder();
		BoundStatement bs = new BoundStatement(SELECT_FROM_CUSTOMERS_RESERVATIONS_BY_CUSTOMER_AND_FLIGHT);
		bs.bind(customer, flightId);

		ResultSet rs = null;

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

		List<Integer> seats = new ArrayList<>();
		int processed = 0;
		for (Row row : rs) {
			if(processed >= count)
				break;
			seats.add(row.getInt("seat"));


			processed++;
		}

		if(seats.size() < count)
			return false;


		for (Integer rSeat : seats) {
			bs = new BoundStatement(DELETE_FROM_OCCUPIED_SEATS);
			bs.bind(flightId, rSeat);
			try {
				session.execute(bs);
			} catch (Exception e) {
				throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
			}

			bs = new BoundStatement(INSERT_INTO_FREE_SEATS);
			bs.bind(flightId, rSeat);
			try {
				session.execute(bs);
			} catch (Exception e) {
				throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
			}

			bs = new BoundStatement(DELETE_FROM_CUSTOMERS_RESERVATIONS);
			bs.bind(customer, flightId, rSeat);
			try {
				session.execute(bs);
			} catch (Exception e) {
				throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
			}
		}
		return true;
	}

	public List<Airplane> getAvailableAirplanes() throws BackendException {
        BoundStatement bs = new BoundStatement(SELECT_FROM_AIRPLANES);
        ResultSet rs = null;
        List<Airplane> result = new ArrayList<>();
        try {
            rs = session.execute(bs);
        } catch(Exception e) {
            throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
        }
        for (Row row : rs){
            result.add(new Airplane(row.getString("name"), row.getInt("seatCount")));
        }
        return result;
    }

    public List<Integer> getAllFlights() throws BackendException {
        BoundStatement bs = new BoundStatement(SELECT_FROM_FLIGHTS);
        ResultSet rs = null;
        List<Integer> result = new ArrayList<>();
        try {
            rs = session.execute(bs);
        } catch(Exception e) {
            throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
        }
        for (Row row : rs){
            result.add(row.getInt("Id"));
        }
        return result;
    }

	public void addFlight(int id, String name, int seatCount, String origin, String destination) throws BackendException {
        BoundStatement bs = new BoundStatement(INSERT_INTO_FLIGHTS);
        bs.bind(id, name, origin, destination);
        try {
            session.execute(bs);
        } catch(Exception e) {
            throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
        }

        for (int i = 0; i < seatCount; i++) {
            bs = new BoundStatement(INSERT_INTO_FREE_SEATS);
            bs.bind(id, i);

            try {
                session.execute(bs);
            } catch (Exception e) {
                throw new BackendException("Could not perform an upsert. " + e.getMessage() + ".", e);
            }

            logger.info("Free seat" + i + " upserted");
        }
    }

	protected void finalize() {
		try {
			if (session != null) {
				session.getCluster().close();
			}
		} catch (Exception e) {
			logger.error("Could not close existing cluster", e);
		}
	}

}
