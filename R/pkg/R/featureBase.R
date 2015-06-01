#' get features.
#'
#' @param sc SparkContext to use
#' @param args args
#' @return RDD containing serialized R objects.
#' @export
#' @examples
#'\dontrun{
#' rdd <- getFeats(sc, "", "", "20150501,20150503", 10, 1)
#'}
getFeats <- function(sc, featList, featTypeList, dateRange, num, seed = base::sample(1:1000000, 1)) {

  jrdd <- callJStatic("com.wanodujia.ti.sparkr.FeatBaseService",
                      "getFeats", sc, featList, dateRange, num, seed)

  # Assume the RDD contains serialized R objects.
  # type of element of RDD is charactor vector.
  sampledRDD <- RDD(jrdd, "string")

  colTypes <- unlist(strsplit(featTypeList, ','))
  colCount <- length(colTypes)

  print("running lapply...")
  typedRDD <- lapply(sampledRDD, function(x) {
    y <- list()
    y[1] <- x[1] # udid

    # append typed features
    for (i in 1 : colCount) {
        if(colTypes[i] == "string") {
            y[i + 1] <- x[i + 1]  # + 1 because x[1] is udid
        } else if (colTypes[i] == "num") {
            if (x[i + 1] != "") {
                y[i + 1] <- as.numeric(x[i + 1])
            } else {
                y[i + 1] <- 0
            }
        }
    }
    y
  })
  print("lapply done.")

  return(collect(typedRDD))
}
