package pkg

func UserKey(password string) string {
	return ":1:" + password
}

func DataKey(password string) string {
	return ":2:" + password
}

type User struct {
	UUID   string `json:"uuid" db:"id"`
	APIKey string `json:"api_key" db:"proxy_password"`
	Data   int64  `json:"data" db:"data"`
}

type NewData struct {
	Transferred int64   `json:"transferred" db:"data"`
	NodeUUID    string  `json:"node_uuid" db:"recipient"`
	UserUUID    string  `json:"user_uuid" db:"sender"`
	APIKey      string  `json:"api_key" db:"proxy_password"`
	Earning     float64 `json:"earnings" db:"earnings"`
}
