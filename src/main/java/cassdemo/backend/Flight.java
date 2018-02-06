package cassdemo.backend;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Flight {
    //private int filghtId;
    private List<Integer> freeSeats = new ArrayList<>();
    private Map<Integer, String> occupiedSeats = new HashMap<>();

    public List<Integer> getFreeSeats() {
        return freeSeats;
    }

    public Map<Integer, String> getOccupiedSeats() {
        return occupiedSeats;
    }

    public String Print() {
        StringBuilder builder = new StringBuilder();
        builder.append("Free seats:\n");
        for (Integer seat : freeSeats) {
            builder.append(seat);
            builder.append(" ");
        }
        builder.append("\nOccupied seats:\n");
        for (Map.Entry<Integer, String> seat : occupiedSeats.entrySet()) {
            builder.append(seat.getKey());
            builder.append(" ");
            builder.append(seat.getValue());
            builder.append("\n");
        }
        return builder.toString();
    }


}
