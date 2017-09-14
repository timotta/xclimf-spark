package com.timotta.rec.xclimf

case class Rating[T] (user: T, item: T, rating: Double)
