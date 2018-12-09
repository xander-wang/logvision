package processing

/*
* @project: logv_streaming
* @description: NCSA log structure
* @author: Xander Wang
* @create: 2018-11-20 23:49
*/
case class log(host: String,
               rfc931: String,
               username: String,
               datetime: String,
               req_method: String,
               req_url: String,
               req_protocol: String,
               statuscode: String,
               bytes: String,
               referrer: String,
               user_agent: String)
