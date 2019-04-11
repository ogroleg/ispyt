# ispyt


### Documentation

#### Requests

[GET] / - get general filtering info

--request

--response

```
{
     "tags": {"tag_id":"string"}
     "knowledge_areas": [{"id": "string"}],
     "regions": [{"id":"string"}],
     "univs": [ {
        "univ_title": "string",
        "univ_id": "string"
     }]
}
```

[POST] / - filter universities by various parameters

--request
```
{
	"filters": {
		"univ_id": ["id"],
		"knowledge_areas":["string"],
		"part_top_applicants": {
			"type": "gov_exams"/"school_score"
			"value": int
		},
		"regions": ["string"],
		"years": [int],
		"enrolled_only": "true"/"false"
	}, 
	"sort_by": ["string"]
}
```

--response

```
[
    {
        "univ_id": "id",
        "univ_title": "string",
        "univ_type": "string",
        "is_state_owned": "true" / "false",
        "location" : "string",
        "analytics": bytes,
        "average_overall_score": float,
        "average_ZNO_score": float,
        "passing_overall_score": float,
    }
]
```