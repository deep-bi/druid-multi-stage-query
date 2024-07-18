# Example requests:

#### Run query:

```bash
 curl -X POST -H "Content-Type:application/json" http://localhost:8888/druid/v2/native/statements -d  '{"queryType":"groupBy","dataSource":{"type":"table","name":"wikipedia"},"intervals":{"type":"intervals","intervals":["-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z"]},"granularity":{"type":"all"},"dimensions":[{"type":"default","dimension":"channel","outputName":"d0","outputType":"STRING"}],"aggregations":[{"type":"count","name":"a0"}],"limitSpec":{"type":"default","columns":[{"dimension":"a0","direction":"ascending","dimensionOrder":{"type":"numeric"}}],"limit":40},"context":{"executionMode":"async","sqlStringifyArrays":false}}'
```

Sample response:

```json
{
  "queryId": "query-76873083-86e8-420b-b4c8-fe252767e020",
  "state": "ACCEPTED",
  "createdAt": "2024-07-11T12:26:57.709Z",
  "schema": {
    "d0": "STRING",
    "a0": "LONG"
  },
  "durationMs": -1
}
```

#### Get query status:

```bash
 curl http://localhost:8888/druid/v2/native/statements/query-76873083-86e8-420b-b4c8-fe252767e020
 ```

Sample response:

```json
{
  "queryId": "query-76873083-86e8-420b-b4c8-fe252767e020",
  "state": "SUCCESS",
  "createdAt": "2024-07-11T12:26:57.709Z",
  "schema": {
    "d0": "STRING",
    "a0": "LONG"
  },
  "durationMs": 26717,
  "result": {
    "numTotalRows": 40,
    "totalSizeInBytes": 2199,
    "dataSource": "__query_select",
    "sampleRecords": [
      [
        "#uz.wikipedia",
        4
      ],
      [
        "#eo.wikipedia",
        8
      ],
      [
        "#kk.wikipedia",
        10
      ],
      [
        "#lt.wikipedia",
        10
      ],
      [
        "#nn.wikipedia",
        11
      ],
      [
        "#hi.wikipedia",
        13
      ],
      [
        "#hr.wikipedia",
        13
      ],
      [
        "#war.wikipedia",
        13
      ],
      [
        "#et.wikipedia",
        14
      ],
      [
        "#be.wikipedia",
        15
      ],
      [
        "#ce.wikipedia",
        16
      ],
      [
        "#bg.wikipedia",
        17
      ],
      [
        "#sl.wikipedia",
        19
      ],
      [
        "#simple.wikipedia",
        21
      ],
      [
        "#la.wikipedia",
        23
      ],
      [
        "#ms.wikipedia",
        23
      ],
      [
        "#el.wikipedia",
        34
      ],
      [
        "#sr.wikipedia",
        34
      ],
      [
        "#gl.wikipedia",
        37
      ],
      [
        "#sk.wikipedia",
        48
      ],
      [
        "#da.wikipedia",
        53
      ],
      [
        "#no.wikipedia",
        73
      ],
      [
        "#vi.wikipedia",
        82
      ],
      [
        "#fi.wikipedia",
        83
      ],
      [
        "#tr.wikipedia",
        88
      ],
      [
        "#eu.wikipedia",
        92
      ],
      [
        "#id.wikipedia",
        92
      ],
      [
        "#hu.wikipedia",
        105
      ],
      [
        "#hy.wikipedia",
        108
      ],
      [
        "#ro.wikipedia",
        118
      ],
      [
        "#ca.wikipedia",
        122
      ],
      [
        "#ko.wikipedia",
        157
      ],
      [
        "#uk.wikipedia",
        227
      ],
      [
        "#cs.wikipedia",
        240
      ],
      [
        "#he.wikipedia",
        264
      ],
      [
        "#pl.wikipedia",
        282
      ],
      [
        "#nl.wikipedia",
        310
      ],
      [
        "#fa.wikipedia",
        311
      ],
      [
        "#pt.wikipedia",
        361
      ],
      [
        "#zh.wikipedia",
        397
      ]
    ],
    "pages": [
      {
        "id": 0,
        "numRows": 40,
        "sizeInBytes": 2199
      }
    ]
  }
}
```

#### Get query results:

```bash
 curl http://localhost:8888/druid/v2/native/statements/query-76873083-86e8-420b-b4c8-fe252767e020/results
 ```

Sample response:

```json
[
  {
    "d0": "#uz.wikipedia",
    "a0": 4
  },
  {
    "d0": "#eo.wikipedia",
    "a0": 8
  },
  {
    "d0": "#kk.wikipedia",
    "a0": 10
  },
  {
    "d0": "#lt.wikipedia",
    "a0": 10
  },
  {
    "d0": "#nn.wikipedia",
    "a0": 11
  },
  {
    "d0": "#hi.wikipedia",
    "a0": 13
  },
  {
    "d0": "#hr.wikipedia",
    "a0": 13
  },
  {
    "d0": "#war.wikipedia",
    "a0": 13
  },
  {
    "d0": "#et.wikipedia",
    "a0": 14
  },
  {
    "d0": "#be.wikipedia",
    "a0": 15
  },
  {
    "d0": "#ce.wikipedia",
    "a0": 16
  },
  {
    "d0": "#bg.wikipedia",
    "a0": 17
  },
  {
    "d0": "#sl.wikipedia",
    "a0": 19
  },
  {
    "d0": "#simple.wikipedia",
    "a0": 21
  },
  {
    "d0": "#la.wikipedia",
    "a0": 23
  },
  {
    "d0": "#ms.wikipedia",
    "a0": 23
  },
  {
    "d0": "#el.wikipedia",
    "a0": 34
  },
  {
    "d0": "#sr.wikipedia",
    "a0": 34
  },
  {
    "d0": "#gl.wikipedia",
    "a0": 37
  },
  {
    "d0": "#sk.wikipedia",
    "a0": 48
  },
  {
    "d0": "#da.wikipedia",
    "a0": 53
  },
  {
    "d0": "#no.wikipedia",
    "a0": 73
  },
  {
    "d0": "#vi.wikipedia",
    "a0": 82
  },
  {
    "d0": "#fi.wikipedia",
    "a0": 83
  },
  {
    "d0": "#tr.wikipedia",
    "a0": 88
  },
  {
    "d0": "#eu.wikipedia",
    "a0": 92
  },
  {
    "d0": "#id.wikipedia",
    "a0": 92
  },
  {
    "d0": "#hu.wikipedia",
    "a0": 105
  },
  {
    "d0": "#hy.wikipedia",
    "a0": 108
  },
  {
    "d0": "#ro.wikipedia",
    "a0": 118
  },
  {
    "d0": "#ca.wikipedia",
    "a0": 122
  },
  {
    "d0": "#ko.wikipedia",
    "a0": 157
  },
  {
    "d0": "#uk.wikipedia",
    "a0": 227
  },
  {
    "d0": "#cs.wikipedia",
    "a0": 240
  },
  {
    "d0": "#he.wikipedia",
    "a0": 264
  },
  {
    "d0": "#pl.wikipedia",
    "a0": 282
  },
  {
    "d0": "#nl.wikipedia",
    "a0": 310
  },
  {
    "d0": "#fa.wikipedia",
    "a0": 311
  },
  {
    "d0": "#pt.wikipedia",
    "a0": 361
  },
  {
    "d0": "#zh.wikipedia",
    "a0": 397
  }
]
```

#### Cancel query:

```bash
curl -X DELETE  http://localhost:8888/druid/v2/native/statements/query-73d3f311-69b6-47b4-bc10-f4d65c180467
 ```

Sample response:

If query was in `ACCEPTED` or `RUNNING` status and was actually cancelled returns empty response with 202 status.

If query was in `SUCCESS` or `FAILED` status and don't need to be cancelled returns empty response with 200 status.