package observatory

import com.sksamuel.scrimage.{Image, Pixel}
import scala.math.{abs, sin, cos, sqrt, pow, toRadians, asin}

/**
  * 2nd milestone: basic visualization
  */
object Visualization {

  val earth_radius_km = 6371.088
  val width = 360
  val height = 180
  val power_num = 4


  /**
    * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
    * @param location Location where to predict the temperature
    * @return The predicted temperature at `location`
    */
  def predictTemperature(temperatures: Iterable[(Location, Double)], location: Location): Double = {
    val dists = temperatures.map(entry => (find_distance(location, entry._1), entry._2))

    val min = dists.reduce((a, b) => if(a._1 < b._1) a else b)
    if(min._1 < 1) {
      min._2
    }
    else{
      // interpolate
      val weights = dists.map(entry => (1 / pow(entry._1, power_num), entry._2))
      val normalizer = weights.map(_._1).sum
      weights.map(entry => entry._1 * entry._2).sum  / normalizer
    }
  }

  def check_antipodes(a: Location, b: Location): Boolean = {
    (a.lat == -b.lat) && (abs(a.lon - b.lon) == 180)
  }

  def find_distance(a: Location, b: Location): Double = {
    if(a == b){
      0
    }
    else if(check_antipodes(a, b)){
      earth_radius_km * math.Pi
    }
    else {
      val delta_lon = toRadians(abs(a.lon - b.lon))
      val alat = toRadians(a.lat)
      val blat = toRadians(b.lat)
      val delta_lat = abs(alat - blat)
      val delta_sigma =   2 * asin(sqrt( pow(sin(delta_lat/2), 2) + cos(alat) * cos(blat) * pow(sin(delta_lon/2), 2) ))
      earth_radius_km * delta_sigma
    }
  }

  /**
    * @param points Pairs containing a value and its associated color
    * @param value The value to interpolate
    * @return The color that corresponds to `value`, according to the color scale defined by `points`
    */
  def interpolateColor(points: Iterable[(Double, Color)], value: Double): Color = {
    val sameCol = points.find(_._1 == value)
    sameCol match {
      case Some((_, col)) => col
      case _ => val (smaller, bigger) = points.partition(_._1 < value)
        if (smaller.isEmpty) {
          bigger.minBy(_._1)._2
        }
        else {
          val a = smaller.maxBy(_._1)
          if (bigger.isEmpty) {
            a._2
          }
          else {
            val b = bigger.minBy(_._1)
            val wa = 1 / abs(a._1 - value)
            val wb = 1 / abs(b._1 - value)
            def interp(x: Int, y: Int): Int = ((wa * x + wb * y) / (wa + wb)).round.toInt
            val ca = a._2
            val cb = b._2
            Color(interp(ca.red, cb.red), interp(ca.green, cb.green), interp(ca.blue, cb.blue))
          }
        }
    }
  }

  /**
    * @param temperatures Known temperatures
    * @param colors Color scale
    * @return A 360×180 image where each pixel shows the predicted temperature at its location
    */
  def visualize(temperatures: Iterable[(Location, Double)], colors: Iterable[(Double, Color)]): Image = {
    val coordinates = for {
      i <- 0 until height
      j <- 0 until width
    } yield (i, j)
    val pixels = coordinates.par
      .map(trans_coordinates)
      .map(predictTemperature(temperatures, _))
      .map(interpolateColor(colors, _))
      .map(col => Pixel(col.red, col.green, col.blue, 255))
      .toArray
    Image(width, height, pixels)
  }

  def trans_coordinates(coord: (Int, Int)): Location = {
    val lon = (coord._2 - width/2) * (360 / width)
    val lat = -(coord._1 - height/2) * (180 / height)
    Location(lat, lon)
  }

}