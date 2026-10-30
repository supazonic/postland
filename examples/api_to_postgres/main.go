// Load users (with nested address/company) and their todos from
// jsonplaceholder.typicode.com into PostgreSQL.
//
//	export DATABASE_URL=postgres://user:pass@localhost:5432/mydb
//	go run ./examples/api_to_postgres
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/supazonic/postland"
	"github.com/supazonic/postland/apisource"
	"github.com/supazonic/postland/postgres"
)

// 1. Define the tables (independent of where data comes from) ---------------

var users = postland.NewTable("users",
	postland.Column{Name: "id", Type: postland.BigInt, PrimaryKey: true},
	postland.Column{Name: "name", Type: postland.Text, NotNull: true},
	postland.Column{Name: "email", Type: postland.Text},
	postland.Column{Name: "city", Type: postland.Text},
	postland.Column{Name: "lat", Type: postland.Double},
	postland.Column{Name: "company", Type: postland.Text},
	postland.Column{Name: "raw", Type: postland.JSONB},
	postland.Column{Name: "loaded_at", Type: postland.TimestampTZ, Default: "now()"},
).InSchema("demo").Index("email")

var todos = postland.NewTable("todos",
	postland.Column{Name: "id", Type: postland.BigInt, PrimaryKey: true},
	postland.Column{Name: "user_id", Type: postland.BigInt, NotNull: true},
	postland.Column{Name: "title", Type: postland.Text},
	postland.Column{Name: "completed", Type: postland.Boolean},
).InSchema("demo").Index("user_id")

// 2. Describe how a record becomes rows (paths into the JSON) ---------------

var userMapping = &postland.TableMapping{
	Table: users,
	Fields: map[string]postland.Field{
		"city": postland.From("address.city"),
		"lat":  postland.From("address.geo.lat"), // "-37.3159" (string) -> DOUBLE PRECISION
		"company": postland.From("company.name").Then(func(v any) (any, error) {
			if s, ok := v.(string); ok {
				return strings.TrimSpace(s), nil
			}
			return v, nil
		}),
		"raw": postland.Whole, // keep the whole original record
	},
}

var todoMapping = &postland.TableMapping{
	Table:  todos,
	Fields: map[string]postland.Field{"user_id": postland.From("userId")},
}

// 3. Plug sources into a sink ------------------------------------------------

func main() {
	ctx := context.Background()
	api := env("API_BASE", "https://jsonplaceholder.typicode.com")

	db, err := postgres.Open(ctx, env("DATABASE_URL", "postgres://postgres@localhost:5432/postgres"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	sink := postgres.NewSink(db) // upsert by primary key

	pipelines := []*postland.Pipeline{
		{
			Source:   apisource.New(api + "/users"),
			Sink:     sink,
			Mappings: []*postland.TableMapping{userMapping},
		},
		{
			Source: &apisource.Source{
				URL:       api + "/todos",
				Paginator: &apisource.PageNumber{Param: "_page", SizeParam: "_limit", PageSize: 50},
			},
			Sink:     sink,
			Mappings: []*postland.TableMapping{todoMapping},
			OnError:  postland.Skip,
		},
	}
	for _, p := range pipelines {
		res, err := p.Run(ctx)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(res)
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
