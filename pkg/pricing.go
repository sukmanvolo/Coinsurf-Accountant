package pkg

type Pricing interface {
	Price(bytes int64) (price float64)
}
