package pkg

import _ "github.com/mailru/easyjson/gen"

//easyjson:json
type Result struct {
	Users map[string]int64 `json:"users"`
}

//easyjson:json
type Results map[string]Result
