package org.mimirdb.iskra.util

trait Timing
{
  protected def logger: com.typesafe.scalalogging.Logger

  def time[T](expr: => T):T = time(getClass().getSimpleName()) { expr }
  def time[T](ctx: String)(expr: => T):T = 
  {
    Timing.logTime(t => logger.debug(s"$ctx took $t ms")) { expr }
  }
}

object Timing
{
  def time[T](expr: => T): (T, Long) = 
  {
    val start = System.currentTimeMillis()
    val result = expr
    val end = System.currentTimeMillis()
    return (result, end - start)
  }

  def logTime[T](output: Long => Unit)(expr: => T): T =
  {
    val (ret, t) = time { expr }
    output(t)
    return ret
  }
}