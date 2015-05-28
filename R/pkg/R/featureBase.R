#' get features.
#'
#' @param sc SparkContext to use
#' @param args args
#' @return RDD containing serialized R objects.
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- getFeats(sc, c(""))
#'}
getFeats <- function(sc, args) {
  jrdd <- callJStatic("com.wanodujia.ti.sparkr.FeatBaseService",
                      "getFeats", sc, args)
  # Assume the RDD contains serialized R objects.
  RDD(jrdd, "byte")
}
