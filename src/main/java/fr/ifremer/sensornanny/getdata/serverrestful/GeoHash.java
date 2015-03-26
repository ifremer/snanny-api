package fr.ifremer.sensornanny.getdata.serverrestful;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class GeoHash {

//	http://fr.wikipedia.org/wiki/Geohash
	
//	https://github.com/davetroy/geohash-js/blob/master/geohash.js
	
	private static final int[] BITS = new int[] { 16, 8, 4, 2, 1 };
	private static final String BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz";
	
	private static void refine_interval(double interval[], int cd, int mask) {
		if ((cd & mask) != 0) {
			interval[0] = (interval[0] + interval[1])/2;
		} else {
			interval[1] = (interval[0] + interval[1])/2;
		}
	}
	
	public static double[][] decodeGeoHash(String geohash) {
		double[][] ret = new double[2][3];
		boolean is_even = true;
		double lat[] = ret[0];
		double lon[] = ret[1];
		
		lat[0] = -90.0;  lat[1] = 90.0;
		lon[0] = -180.0; lon[1] = 180.0;
		
		double lat_err = 90.0;
		double lon_err = 180.0;
		
		for (int i = 0; i < geohash.length(); i++) {
			char c = geohash.charAt(i);
			int cd = BASE32.indexOf(c);
			for (int j = 0; j < 5; j++) {
				int mask = BITS[j];
				if (is_even) {
					lon_err /= 2;
					refine_interval(lon, cd, mask);
				} else {
					lat_err /= 2;
					refine_interval(lat, cd, mask);
				}
				is_even = !is_even;
			}
		}
		lat[2] = (lat[0] + lat[1])/2;
		lon[2] = (lon[0] + lon[1])/2;

		return ret;
	}
	
	private static String[] getParts(String string, int partitionSize) {
        List<String> parts = new ArrayList<String>();
        int len = string.length();
        for (int i=0; i<len; i+=partitionSize)
        {
            parts.add(string.substring(i, Math.min(len, i + partitionSize)));
        }
        return parts.toArray(new String[parts.size()]);
    }
	
	public static String[] split(String geohash) {
		return getParts(geohash, 1);
	}
	
	public static String commonPrefix(String from, String to) {
		StringBuilder ret = new StringBuilder();
		
		String[] fromParts = getParts(from, 1);
		String[] toParts   = getParts(to, 1);
		
		for (int i = 0; i < fromParts.length && i < toParts.length; i++) {
			if (fromParts[i].equals(toParts[i])) {
				ret.append(fromParts[i]);
			} else {
				break;
			}
		}
		
		return ret.toString();
	}
	
	public static String encodeGeoHash(double latitude, double longitude) {
		boolean is_even = true;
		int i = 0;
		double lat[] = new double[2];
		double lon[] = new double[2];
		int bit = 0;
		int ch = 0;
		int precision = 12;
		StringBuilder geohash = new StringBuilder();

		lat[0] = -90.0;  lat[1] = 90.0;
		lon[0] = -180.0; lon[1] = 180.0;
		
		while (geohash.length() < precision) {
			if (is_even) {
				double mid = (lon[0] + lon[1]) / 2;
				if (longitude > mid) {
					ch |= BITS[bit];
					lon[0] = mid;
				} else
					lon[1] = mid;
			} else {
				double mid = (lat[0] + lat[1]) / 2;
				if (latitude > mid) {
					ch |= BITS[bit];
					lat[0] = mid;
				} else
					lat[1] = mid;
			}

			is_even = !is_even;
			if (bit < 4)
				bit++;
			else {
				geohash.append(BASE32.charAt(ch));
				bit = 0;
				ch = 0;
			}
		}
		
		return geohash.toString();
	}

	public static void main(String[] args) {
		
		System.out.println(decodeGeoHash("eder")[0][2] + " " + decodeGeoHash("eder")[1][2]);
		System.out.println(decodeGeoHash("edez")[0][2] + " " + decodeGeoHash("edez")[1][2]);
		System.out.println(decodeGeoHash("edg2")[0][2] + " " + decodeGeoHash("edg2")[1][2]);
		
		if (true) return;
		
		System.out.println(Arrays.toString(decodeGeoHash("0p")[0]) + " " + Arrays.toString(decodeGeoHash("0p")[1]));
		System.out.println(Arrays.toString(decodeGeoHash("0p7")[0]) + " " + Arrays.toString(decodeGeoHash("0p7")[1]));
		System.out.println(Arrays.toString(decodeGeoHash("0p7t")[0]) + " " + Arrays.toString(decodeGeoHash("0p7t")[1]));
		
		System.out.println(Arrays.toString(decodeGeoHash("cd11e0aaac1d")[0]) + " " + Arrays.toString(decodeGeoHash("cd11e0aaac1d")[1]));
		
		System.out.println(Arrays.toString(decodeGeoHash("cd11e0zzzc1d")[0]) + " " + Arrays.toString(decodeGeoHash("cd11e0zzzc1d")[1]));
		System.out.println(encodeGeoHash(56.51912799105048, -110.95093611627817));
		
		System.out.println(Arrays.toString(decodeGeoHash("g")[0]) + " " + Arrays.toString(decodeGeoHash("g")[1]));
		System.out.println(Arrays.toString(decodeGeoHash("gb76m5r620e6")[0]) + " " + Arrays.toString(decodeGeoHash("gb76m5r620e6")[1]));
		
		System.out.println(commonPrefix("e708qzrrntns", "u653kzrfx5db").length());
		System.out.println(commonPrefix("1cfw5bqcp5r2", "ynj3up9nbu8x").length());
		System.out.println(commonPrefix("u18wncrmw8qe", "u44uh7h6kcer").length());
		System.out.println(commonPrefix("u18wncrmw8qe", "u14uh7h6kcer").length());
		System.out.println(commonPrefix("u18wncrmw8qe", "u18wncrmw8qe").length());
		
		
	}

}
