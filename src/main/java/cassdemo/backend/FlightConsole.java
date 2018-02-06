package cassdemo.backend;

import java.util.*;

public class FlightConsole {
    public static BackendSession backendSession;
    public static final int stressedFlightID = 777;

    private static Role role;

    private enum Role {
        Officer,
        Customer
    }

    public static void runConsole() throws BackendException {
        Scanner reader = new Scanner(System.in);
        //
        //

        boolean run = true;
        while(run) {
            System.out.println("What is your role? (officer / customer)");
            String role = reader.nextLine();
            switch (role) {
                case "officer":
                    FlightConsole.officerConsole(reader);
                    break;
                case "customer":
                    FlightConsole.customerConsole(reader);
                    break;
                default:
                    System.out.println("Role not recognized");
                    break;
            }
        }
    }

    private static void officerConsole(Scanner reader) throws BackendException {
        String command = "";
        while (!command.equals("logout")) {
            System.out.println("Available commands: *add flight*, *get boarding list*, *logout*");
            System.out.println("Enter command");
            command = reader.nextLine();

            switch (command) {
                case "add flight":
                    FlightConsole.addFlight(reader);
                case "get boarding list":
                    FlightConsole.getBoardingList(reader);
                default:
                    System.out.println("Command not recognized");
                    break;
            }
        }
    }

    private static void customerConsole(Scanner reader) throws BackendException {
        String command = "";
        while (!command.equals("logout")) {
            System.out.println("Available commands: *book*, *unbook*, *get all reservations*, *logout*");
            System.out.println("Enter command");
            command = reader.nextLine();

            switch (command) {
                case "book":
                    FlightConsole.book(reader);
                    break;
                case "unbook":
                    FlightConsole.unbook(reader);
                    break;
                case "get all reservations":
                    FlightConsole.getAllReservations(reader);
                    break;
                default:
                    System.out.println("Command not recognized");
                    break;
            }
        }
    }

    private static void addFlight(Scanner reader) throws BackendException
    {
        List<Airplane> planes =  backendSession.getAvailableAirplanes();
        Map<String, Integer> planesMap = new HashMap<>();
        for (Airplane plane : planes){
            System.out.println(String.format("%s %d", plane.name, plane.seatCount));
            planesMap.put(plane.name, plane.seatCount);
        }

        // print available airplanes
        System.out.println("Issue: id name origin destination");
        String[] command = reader.nextLine().split(" ");
        backendSession.addFlight(
                Integer.parseInt(command[0]), command[1],
                planesMap.get(command[1]), command[2], command[3]);
    }

    private static void getBoardingList(Scanner reader) throws BackendException
    {
        // print available airplanes
        List<Integer> flights = backendSession.getAllFlights();
        for (Integer id : flights){
            System.out.println(id);
        }
        System.out.println("Issue: flightId");
        Flight flight = backendSession.getFlight(Integer.parseInt(reader.nextLine()));
        System.out.println(flight.Print());
    }

    private static void book(Scanner reader) throws BackendException
    {
        List<Integer> flights = backendSession.getAllFlights();
        System.out.printf("Available flights: ");
        for (Integer id : flights){
            System.out.println(id);
        }
        System.out.println("Issue: flightId, customerName, count");
        String[] command = reader.nextLine().split(" ");
        backendSession.bookSeats(Integer.parseInt(command[0]),
                command[1], Integer.parseInt(command[2]));
    }

    private static void unbook(Scanner reader) throws BackendException {
        getAllReservations(reader);
        System.out.println("Issue: flightId, customerName, count");
        String[] command = reader.nextLine().split(" ");
        backendSession.unBookSeats(Integer.parseInt(command[0]),
                command[1], Integer.parseInt(command[2]));
    }

    private static void getAllReservations(Scanner reader) throws BackendException
    {
        System.out.println("Issue: customerName");
        List<SeatID> reservations = backendSession.getAllCustomerReservations(reader.nextLine());
        for(SeatID seat : reservations) {
            System.out.println(
                    String.format(
                            "Flight: %d, Seat: %d", seat.FlightId, seat.SeatNo));
        }
        // add flight
    }

    public static void runCustomer() {
        String name = UUID.randomUUID().toString();
        for(int i = 0; i < 3; i++) {
            try {
                if (backendSession.bookSeats(
                        stressedFlightID, name, new Random().nextInt(6) + 1)) {
                    System.out.println(String.format("Customer [%s] booked seat(s)", name));
                } else {
                    System.out.println(String.format("Customer [%s] could not book seats :((", name));
                }
            } catch (BackendException e) {
                e.printStackTrace();
            }
        }

        List<SeatID> reservations = null;
        try {
            reservations = backendSession.getAllCustomerReservations(name);
        } catch (BackendException e) {
            e.printStackTrace();
        }

        Map<Integer, List<Integer>> seatsByFlight = new HashMap<>();
        for(SeatID seat : reservations) {
            if(seatsByFlight.containsKey(seat.FlightId))
                seatsByFlight.get(seat.FlightId).add(seat.SeatNo);
            else
                seatsByFlight.put(seat.FlightId, new ArrayList<Integer>() {{
                    add(seat.SeatNo);
                }});
        }

        for(Map.Entry<Integer, List<Integer>> flight : seatsByFlight.entrySet()) {
            try {
                Flight fl = backendSession.getFlight(flight.getKey());
                for(Integer seat : flight.getValue()) {
                    if(!fl.getOccupiedSeats().containsKey(seat)
                            || !fl.getOccupiedSeats().get(seat).equals(name)) {
                        System.out.println(
                                String.format(
                                        "ERROR Flight: %d, Seat: %d is not occupied by %s",
                                        flight.getKey(), seat, name));
                    }
                }
            } catch (BackendException e) {
                e.printStackTrace();
            }
        }
    }

    private static void createStressedFlight() throws BackendException {
        backendSession.addFlight(stressedFlightID, "stressTest", 300,
                "Washington", "Chicago");
    }


    public static void stressTest() {
        //UUID.randomUUID().toString();
        try {
            createStressedFlight();
            for(int i = 0; i < 500; i++) {
                Thread t = new Thread() {
                    public void run() {
                        runCustomer();
                    }
                };
                t.start();
            }
        } catch (BackendException e) {
            e.printStackTrace();
        }

    }
}