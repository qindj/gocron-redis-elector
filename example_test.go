package elector

import (
	"context"
)

func ExampleNewElector() {
	cfg := Config{
		InitAddress: []string{"127.0.0.1:6379"},
		Username:    "",
		Password:    "woIuu1208",
		SelectDB:    1,
	}

	_, err := NewElector(context.Background(), cfg)
	if err != nil {
		panic(err)
	}
}
