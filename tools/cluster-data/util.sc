case class Summary(
  p25: BigDecimal,
  p50: BigDecimal,
  p75: BigDecimal,
  p90: BigDecimal,
  p95: BigDecimal,
  p99: BigDecimal,
  max: BigDecimal)

object Summary {
  private val percentiles: Seq[Double] = Seq(0.25, 0.5, 0.75, 0.90, 0.95, 0.99, 1.0)
  def ofInt(data: Seq[Int]): Summary = {
    val result = getPercentilesInt(data, percentiles)
    Summary(result(0), result(1), result(2), result(3), result(4), result(5), result(6))
  }
  def ofLong(data: Seq[Long]): Summary = {
    val result = getPercentilesLong(data, percentiles)
    Summary(result(0), result(1), result(2), result(3), result(4), result(5), result(6))
  }
  def ofDouble(data: Seq[Double]): Summary = {
    val result = getPercentilesDouble(data, percentiles)
    Summary(result(0), result(1), result(2), result(3), result(4), result(5), result(6))
  }

  private def getPercentilesInt(data: Seq[Int], percentiles: Seq[Double]): Seq[Int] = {
    val sorted = data.sorted
    val lastElement = data.sorted.length - 1
    percentiles.map { percentile =>
      sorted((percentile * lastElement).toInt)
    }
  }

  private def getPercentilesLong(data: Seq[Long], percentiles: Seq[Double]): Seq[Long] = {
    val sorted = data.sorted
    val lastElement = data.sorted.length - 1
    percentiles.map { percentile =>
      sorted((percentile * lastElement).toInt)
    }
  }

  private def getPercentilesDouble(data: Seq[Double], percentiles: Seq[Double]): Seq[Double] = {
    val sorted = data.sorted
    val lastElement = data.sorted.length - 1
    percentiles.map { percentile =>
      sorted((percentile * lastElement).toInt)
    }
  }
}
