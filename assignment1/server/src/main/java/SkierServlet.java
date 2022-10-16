import com.google.gson.Gson;
import lombok.SneakyThrows;
import model.LiftRide;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

@WebServlet(name = "SkierServlet", value = "/SkierServlet")
public class SkierServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        response.getWriter().write("Construction in progress");
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) {
        response.setContentType("application/json");
        response.setCharacterEncoding("UTF-8");

        String urlPath = request.getPathInfo();

        // check if we have a URL
        checkIfUrlEmpty(response, urlPath);
        checkIfUrlCorrectlyFormatted(request, response, urlPath);
    }

    @SneakyThrows
    private void checkIfUrlEmpty(HttpServletResponse response, String urlPath) {
        if (urlPath == null || urlPath.isEmpty()) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            response.getWriter().write("The URL supplied is empty or is missing parameters");
        }
    }

    @SneakyThrows
    private void checkIfUrlCorrectlyFormatted(HttpServletRequest request, HttpServletResponse response, String urlPath) {
        LiftRide liftRide = new Gson().fromJson(request.getReader(), LiftRide.class);

        if (!isUrlValid(urlPath)) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            response.getWriter().write("The URL supplied contains one or more errors. Please fix and try again");
        } else {
            response.setStatus(HttpServletResponse.SC_CREATED);
            short time = liftRide.getTime(), liftId = liftRide.getLiftId();
            response.getWriter().write("POST request successful, with liftId=" + String.valueOf(liftId) + " and time=" + String.valueOf(time));
        }
    }

    @SneakyThrows
    private boolean isUrlValid(String urlPath) {
        String[] urlParts = urlPath.split("/");

        // an appropriate urlParts should have a length that's at least 8 - avoiding out-of-bounds exception down the road
        if (urlParts.length < 8) return false;

        final String SEASONS = "seasons", DAYS = "days", SKIERS = "skiers";

        // field names must be exactly "seasons", "days", and "skiers"
        if (!urlParts[2].equals(SEASONS) || !urlParts[4].equals(DAYS) || !urlParts[6].equals(SKIERS)) return false;

        // parameters must conform to formats specified in the Swagger API
        try {
            Integer.parseInt(urlParts[1]);
        } catch (NumberFormatException nfe) {
            return false;
        }

        try {
            int skiDay = Integer.parseInt(urlParts[5]);
            if (skiDay < 1 || skiDay > 366) return false;
        } catch (NumberFormatException nfe) {
            return false;
        }

        try {
            Integer.parseInt(urlParts[7]);
        } catch (NumberFormatException nfe) {
            return false;
        }

        return true;
    }
}
