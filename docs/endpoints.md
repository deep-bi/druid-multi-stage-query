# Native query resource endpoints

| Method   | Endpoint                                         | Parameters                      | Data                                                                                                                 | Description                                                                                            |
|----------|--------------------------------------------------|---------------------------------|----------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------|
| [POST]   | `/druid/v2/native/statements/`                   |                                 | Native query. Example:`{"queryType": "scan","dataSource": {"type": "table","name": "bar"}, <...>, "context": {...}}` | Accepts standard Druid Native Queries. Possible query types accepted: GroupBy, Scan(with legacy=false) |
| [GET]    | `/druid/v2/native/statements/{queryId}/`         | `queryId`                       |                                                                                                                      | Returns status of query with selected id                                                               |
| [GET]    | `/druid/v2/native/statements/{queryId}/results/` | `queryId` `page` `resultFormat` |                                                                                                                      | Returns the results of the query with the given id.                                                    |
| [DELETE] | `/druid/v2/native/statements/{queryId}/`         | `queryId`                       |                                                                                                                      | Cancelles query with selected id if it's in `ACCEPTED` or `RUNNING` statuses, else do nothing          |

#### Optional `get results` query parameters:
* page (optional)
    * Type: `Int`
    * Fetch results based on page numbers. If not specified, all results are returned sequentially starting from page 0 to N in the same response.
* resultFormat (optional)
    * Type: `String`
    * Defines the format in which the results are presented. The following options are supported `arrayLines`,`objectLines`,`array`,`object`, and `csv`. The default is `object`.

#### Example `get results` query with parameters:
`GET https://ROUTER:8888/druid/v2/native/statements/{queryId}/results?page=PAGENUMBER&resultFormat=FORMAT`