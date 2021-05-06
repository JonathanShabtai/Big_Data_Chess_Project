import scala.reflect.runtime.universe._


case class KafkaFlightRecord(
                              fide_id: String,
                              games_count:Long)