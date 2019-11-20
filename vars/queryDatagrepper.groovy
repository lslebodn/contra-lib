/**
 * Library to query Datagrepper for UMB messages
 * and return a list of matched messages.
 *
 * @param parameters Map parameters to include in the query
 * @return messages List parsed from DataGrepper
 */
def call(Map parameters = [:]) {
    if (!('queryParameters' in parameters) && parameters.queryParameters instanceof Map) {
        error "ERROR: queryParameters are required and it has to be a Map!"
    }
    def dataGrepperUrl = parameters.get('dataGrepperUrl', "https://apps.fedoraproject.org/datagrepper")
    def page = parameters.get('page')
    def insecure = parameters.get('insecure', false)
    def withMetadata = parameters.get('withMetadata', false)
    def queryParameters = parameters.queryParameters

    // When using contains, we must specify a start, use delta of 3 days if not specified
    if (queryParameters.containsKey('contains')
        && (!queryParameters.containsKey('delta') || !queryParameters.containsKey('start'))) {
        queryParameters.delta = 259200
    }
    // If rows_per_page not supplied, use the maximum number
    if (!queryParameters.containsKey('rows_per_page')) {
        queryParameters.rows_per_page = 100
    }

    // Query datagrepper with supplied parameters
    def urlParameterList = []
    queryParameters.each{ k, v ->
        if (v != "") {
            urlParameterList += "${k}=${v}"
        }
    }

    String resultsFileName = "${UUID.randomUUID().toString()}.json"
    String queryUrl = "${dataGrepperUrl}/raw?${urlParameterList.join('&')}"
    String statusCode = sh (returnStdout: true, script: """
        curl ${insecure ? "-k" : ""} --output ${resultsFileName} --write-out '%{http_code}' '${queryUrl}'
        """).trim()

    // Check http status code of the query
    if (statusCode == "404") {
        error "ERROR: No page found for ${queryUrl}"
    } else if (statusCode.startsWith("5")) {
        echo "$statusCode"
        sh "cat ${resultsFileName}"
        error "ERROR: internal datagrepper server error... (URL: ${queryUrl})"
    } else if (statusCode.startsWith("0")) {
        error "ERROR: Error querying datagrepper on url (URL: ${queryUrl})"
    }

    // Read the resulting JSON file and parse it into Map
    def jsonText = readFile file: resultsFileName
    def parsedData = readJSON text: jsonText
    def parsedMessages = []

    int numberOfPages = parsedData?.pages

    // Get all pages if parameter page was not specified.
    if (numberOfPages != null && numberOfPages > 1 && page == null) {
        def parsedPageMessages = []
        def pageParameters = [:]
        float startTime = parsedData?.arguments?.start
        float endTime = parsedData?.arguments?.end
        if (startTime != null) {
            pageParameters['start'] = String.format("%.1f", startTime)
        }
        if (endTime != null) {
            pageParameters['end'] = String.format("%.1f", endTime)
        }
        pageParameters << queryParameters

        for (int i = 1; i <= numberOfPages; i++) {
            pageParameters['page'] = i
            parsedPageMessages = queryDataGrepper(pageParameters)
            parsedPageMessages.each() {
                parsedMessages.add(it)
            }
        }
    } else {
        parsedMessages = parsedData ?: []
    }

    if (!withMetadata) {
        parsedMessages = parsedMessages.collect {it.raw_messages}
    }

    return parsedMessages
}
