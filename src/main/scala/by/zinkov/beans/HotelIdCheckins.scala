package by.zinkov.beans

import java.time.LocalDate
import java.time.temporal.ChronoUnit

case class HotelIdCheckins(hotel_id: Long, checkins: List[String]) {
  def idleDaysIsValid(): Boolean = {
    var tail: String = null
    for (current <- checkins) {
      val currentDate = LocalDate.parse(current)
      if (tail != null) {
        val tailDate = LocalDate.parse(tail)
        val result = ChronoUnit.DAYS.between(tailDate, currentDate)
        if (result >= 2 && result < 30) {
          return false
        }
      }
      tail = current
    }
    true
  }
}
