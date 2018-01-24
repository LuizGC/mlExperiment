import javax.imageio.ImageIO;
import javax.imageio.stream.ImageInputStream;
import java.awt.image.BufferedImage;
import java.awt.image.Raster;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.stream.IntStream;

public class Teste3_Reader {

	/*
Band    name	Resolution (m)	Purpose
B01	    60	    Aerosol detection
B09	    60	    Water vapour
B10	    60	    Cirrus

B05	    20	    Vegetation classification
B06	    20	    Vegetation classification
B07	    20	    Vegetation classification
B08A	20	    Vegetation classification
B11	    20	    Snow / ice / cloud discrimination
B12	    20	    Snow / ice / cloud discrimination

B02	    10	    Blue
B03	    10	    Green
B04	    10	    Red
B08	    10	    Near infrared

	 */


	private static String url;
	private static String user;
	private static String password;


	public static void main( String[] args ) throws IOException {

		url = args[0];
		user = args[1];
		password = args[3];


		InputStream stream10m = Files.newInputStream(Paths.get(args[4]));
		InputStream stream20m = Files.newInputStream(Paths.get(args[5]));
		InputStream stream60m = Files.newInputStream(Paths.get(args[6]));


		Raster image10m = getBufferedImage(stream10m);
		Raster image20m = getBufferedImage(stream20m);
		Raster image60m = getBufferedImage(stream60m);

		getRangeParallelStream(image10m.getWidth())
				.forEach(w->
						getRangeParallelStream(image10m.getHeight())
								.forEach(h -> {

									int[] values10m = new int[4];
									int[] values20m = new int[6];
									int[] values60m = new int[3];

									image10m.getPixel(w, h, values10m);
									image20m.getPixel(Math.floorDiv(w,2), Math.floorDiv(h,2), values20m);
									image60m.getPixel(Math.floorDiv(w,6), Math.floorDiv(h,6), values60m);

									String teste = "INSERT INTO teste_chema.pixel_value VALUES ( " +
											values10m[0] + "," +
											values10m[1] + "," +
											values10m[2] + "," +
											values10m[3] + "," +

											values20m[0] + "," +
											values20m[1] + "," +
											values20m[2] + "," +
											values20m[3]+ "," +
											values20m[4] + "," +
											values20m[5]  + "," +

											values60m[0] + "," +
											values60m[1]  + "," +
											values60m[2] + ")";

									writeDB(teste);


								})
				);


	}

	public static Connection connect() {
		Connection conn = null;
		try {
			conn = DriverManager.getConnection(url, user, password);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}

		return conn;
	}

	private static IntStream getRangeParallelStream(int width) {
		return IntStream.range(0, width - 1).parallel();
	}

	private static void writeDB(String sql) {
		try {
			Connection conn = connect();
			PreparedStatement pstmt = conn.prepareStatement(sql);
			pstmt.execute();
			pstmt.close();
			conn.close();

		} catch (SQLException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	private static Raster getBufferedImage(InputStream stream) {
		try {
			ImageInputStream imageStream = ImageIO.createImageInputStream(stream);
			BufferedImage image = ImageIO.read(imageStream);
			return image.getData();
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}
}
