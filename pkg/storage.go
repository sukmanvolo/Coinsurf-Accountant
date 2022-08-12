package pkg

import "context"

type Storage interface {
	Name() string
	Save(context.Context, []NewData, []User) error
}
