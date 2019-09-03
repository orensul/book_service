package main

import (
	"context"
	"encoding/json"
	"fmt"
	errors "github.com/fiverr/go_errors"
	"gopkg.in/olivere/elastic.v5"
	"gopkg.in/redis.v5"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Book is a structure used for serializing/deserializing data in Elasticsearch.
type Book struct {
	Title          string    `json:"title"`
	AuthorName     string    `json:"author_name"`
	Price          int       `json:"price"`
	EbookAvailable bool      `json:"ebook_available"`
	PublishDate    time.Time `json:"publish_date"`
}

type AggsRes struct {
	Books   int `json:"total books"`
	Authors int `json:"distinct authors"`
}

type Range struct {
	From int
	To   int
}

const (
	mapping = `
{
	"settings":{
		"number_of_shards": 1,
		"number_of_replicas": 0
	},
	"mappings":{
		"book":{
			"properties": { 
				"title":    { "type": "text"  , "fielddata": true}, 
				"author_name":     { "type": "text"  , "fielddata": true}, 
				"price":      { "type": "float" },
				"ebook_available": {"type": "boolean"},
				"publish_date": {"type": "date"}
			  }
		}
	}
}`
	URL        string = "http://10.200.10.1:9200"
	USER_INDEX        = "books"
	USER_TYPE         = "book"
)

func connectElasticSearch() (*elastic.Client, context.Context, error) {
	// Starting with elastic.v5, you must pass a context to execute each service
	ctx := context.Background()
	// Obtain a client and connect to the  Elasticsearch installation on URL
	client, err := elastic.NewSimpleClient(elastic.SetURL(URL))
	if err != nil {
		return client, ctx, errors.Wrap(err, "cannot connect to elastic search")
	}
	return client, ctx, nil
}
func connectRedis() (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: "", DB: 0})
	pong, err := client.Ping().Result()
	fmt.Println(pong, err)
	return client, err
}

func addBook(client *elastic.Client, ctx context.Context, id string, book Book) (string, error) {
	put, err := client.Index().Index("books").Type("book").Id(id).BodyJson(book).Do(ctx)
	if err != nil {
		return "", errors.Wrap(err, "cannot add the book")
	}
	s := fmt.Sprintf("Indexed book %s to index %s, type %s\n", put.Id, put.Index, put.Type)
	return s, nil
}

func deleteBook(client *elastic.Client, ctx context.Context, id string) (string, error) {
	del, err := client.Delete().Index("books").Type("book").Id(id).Do(ctx)
	if err != nil {
		return "", errors.Wrap(err, "cannot delete the book")
	}
	if del.Found {
		s := fmt.Sprintf("Delete document %s in version %d from index %s, type %s\n", del.Id, del.Version, del.Index, del.Type)
		return s, nil
	}
	return "", nil
}

func getBook(client *elastic.Client, ctx context.Context, id string) (string, error) {
	get, err := client.Get().Index("books").Type("book").Id(id).Do(ctx)
	if err != nil {
		return "", errors.Wrap(err, "Cannot GET a book")
	}
	if get.Found {
		return string(*get.Source), nil
	}

	return "", nil
}

func writeToRedis(userID string, route string, method string) error {
	client, err := connectRedis()
	if err != nil {
		return errors.Wrap(err, "cannot connect to Redis")

	} else {
		value := "route=" + route + ", method=" + method
		score := float64(time.Now().Nanosecond())
		// add userId request to ordered set in redis, score according to time
		_, err = client.ZAdd(userID, redis.Z{score, value}).Result()
		if err != nil {
			return errors.Wrap(err, "cannot set key in Redis")
		}
		return nil
	}
}

func updateBook(client *elastic.Client, ctx context.Context, id string, title string) (string, error) {
	update, err := client.Update().Index(USER_INDEX).Type(USER_TYPE).Id(id).Doc(map[string]interface{}{"title": title}).Do(ctx)
	if err != nil {
		return "", err
	}
	s := fmt.Sprintf("New version of book %q is now %d\n", update.Id, update.Version)
	return s, nil
}

func searchBook(client *elastic.Client, ctx context.Context, title string, authorName string, priceRange Range) (string, error) {
	q := make([]elastic.Query, 0)
	if title != "" {
		q = append(q, elastic.NewMatchQuery("title", title))
	}
	if authorName != "" {
		q = append(q, elastic.NewMatchQuery("author_name", authorName))
	}
	if !(priceRange.From == -1 && priceRange.To == -1) {
		q = append(q, elastic.NewRangeQuery("price").From(priceRange.From).To(priceRange.To))
	}
	query := elastic.NewBoolQuery().Must(q...)

	searchResult, _ := client.Search().Index("books").Query(query).Sort("title", true).
		From(0).Size(10).Pretty(true).Do(ctx)

	var booksResult = make([]string, 0)
	if len(searchResult.Hits.Hits) > 0 {
		fmt.Printf("Found a total of %d books\n", searchResult.Hits.TotalHits)
		// Iterate through results
		for _, hit := range searchResult.Hits.Hits {
			booksResult = append(booksResult, string(*hit.Source))
		}
		s := fmt.Sprintf("%s", booksResult)
		return s, nil
	} else {
		// No hits
		return "", nil
	}
}

func getParamValue(req *http.Request, paramName string) string {
	if len(req.URL.Query()[paramName]) >= 1 {
		return req.URL.Query()[paramName][0]
	} else {
		return ""
	}

}

func activity(w http.ResponseWriter, req *http.Request) {
	var err error
	client, err := connectRedis()
	if err != nil {
		err = errors.Wrap(err, "cannot connect to Redis")
		fmt.Fprintf(w, "%s", err)
		return
	} else {
		switch req.Method {
		case "GET":
			userId := getParamValue(req, "user_id")
			// range on top 3 requests of the user from redis according to userId key
			if userId != "" {
				resultSet, err := client.ZRevRangeWithScores(userId, 0, 2).Result()
				if err != nil {
					err = errors.Wrap(err, "cannot get key from Redis")
					fmt.Fprintf(w, "%s", err)
					return
				} else {
					// iterate the three items in the ordered set
					for _, zItem := range resultSet {
						fmt.Fprintf(w, "%v\n", zItem.Member)
					}
				}
			}
		default:
			msg := "Unsupported request for /activity " + req.Method
			err = errors.New(msg)
			fmt.Fprintf(w, "%s", err)
		}
	}
}

func book(w http.ResponseWriter, req *http.Request) {
	var err error
	var id, title, authorName, result, userId string
	var price int
	var publishDate time.Time
	var ebookAvailable bool

	client, ctx, err := connectElasticSearch()
	if err != nil {
		errors.Wrap(err, "error in connecting to ES")
		fmt.Fprintf(w, "%s", err)
		return
	}
	// extract param values to variables and parse to the correct data type
	id = getParamValue(req, "id")
	title = getParamValue(req, "title")
	authorName = getParamValue(req, "author_name")
	userId = getParamValue(req, "user_id")
	tempeBookAvailable := getParamValue(req, "ebook_available")

	if tempeBookAvailable != "" {
		ebookAvailable, err = strconv.ParseBool(tempeBookAvailable)
		if err != nil {
			fmt.Println("conversion from string to bool for field ebookAvailable failed")
			err = errors.Wrap(err, "conversion from string to bool for field ebookAvailable failed")
			fmt.Fprintf(w, "%s", err)
			return
		}
	}
	tempPrice := getParamValue(req, "price")
	if tempPrice != "" {
		price, err = strconv.Atoi(tempPrice)
		if err != nil {
			fmt.Println("conversion from string to int for field price failed")
			err = errors.Wrap(err, "conversion from string to int for field price failed")
			fmt.Fprintf(w, "%s", err)
			return
		}
	}
	tempPublishDate := getParamValue(req, "publish_date")
	if tempPublishDate != "" {
		publishDate, err = time.Parse(time.RFC3339, tempPublishDate)
		if err != nil {
			fmt.Println("conversion from string to time for field publishDate failed")
			err = errors.Wrap(err, "conversion from string to time for field publishDate failed")
			fmt.Fprintf(w, "%s", err)
			return
		}
	}

	// handle different request types
	switch req.Method {
	case "GET":
		result, err = getBook(client, ctx, id)
	case "DELETE":
		result, err = deleteBook(client, ctx, id)
	case "POST":
		result, err = updateBook(client, ctx, id, title)
	case "PUT":
		result, err = addBook(client, ctx, id, Book{Title: title, AuthorName: authorName, Price: price, EbookAvailable: ebookAvailable, PublishDate: publishDate})
	default:
		msg := "Unsupported request for /book " + req.Method
		err = errors.New(msg)
	}
	if err != nil {
		fmt.Fprintf(w, "%s", err)
	} else {
		fmt.Fprintf(w, "%s", result)
		// handle write to redis
		if userId != "" {
			err = writeToRedis(userId, "book", req.Method)
			if err != nil {
				fmt.Fprintf(w, "%s", err)
			}
		}
	}
}

func storeBook(client *elastic.Client, ctx context.Context) (string, error) {
	cardinalityAgg := elastic.NewCardinalityAggregation().Field("author_name")
	searchResult, err := client.Search().Index(USER_INDEX).Pretty(true).Aggregation("distinctAuthors", cardinalityAgg).Do(ctx)
	distinctAuthors, found := searchResult.Aggregations.Cardinality("distinctAuthors")
	if !found {
		return "", nil
	}
	numOfBooks := searchResult.Hits.TotalHits

	buf, err := json.Marshal(AggsRes{Books: int(numOfBooks), Authors: int(math.Round(*distinctAuthors.Value))})
	if err != nil {
		return "", errors.Wrap(err, "cannot create json result of aggregation query")
	}
	return string(buf), nil

}
func store(w http.ResponseWriter, req *http.Request) {
	var err error
	var result string
	client, ctx, err := connectElasticSearch()
	if err != nil {
		errors.Wrap(err, "error in connecting to ES")
		fmt.Fprintf(w, "%s", err)
		return
	}
	userId := getParamValue(req, "user_id")
	switch req.Method {
	case "GET":
		result, err = storeBook(client, ctx)
	default:
		msg := "Unsupported request for /store " + req.Method
		err = errors.New(msg)
	}
	if err != nil {
		fmt.Fprintf(w, "%s", err)

	} else {
		if userId != "" {
			err = writeToRedis(userId, "store", req.Method)
			if err != nil {
				fmt.Fprintf(w, "%s", err)
			}
		}
		fmt.Fprintf(w, "%s", result)
	}
}
func search(w http.ResponseWriter, req *http.Request) {
	var err error
	var result string
	client, ctx, err := connectElasticSearch()
	if err != nil {
		errors.Wrap(err, "error in connecting to ES")
		fmt.Fprintf(w, "%s", err)
		return
	}
	// extract param values and parse them to the correct data type
	title, authorName, priceRange := getParamValue(req, "title"), getParamValue(req, "author_name"), getParamValue(req, "price_range")
	userId := getParamValue(req, "user_id")

	// extract from and to from price_range param
	var from, to int
	r := strings.Split(priceRange, "-")
	if len(r) == 2 {
		from, err = strconv.Atoi(r[0])
		if err != nil {
			fmt.Println("price range conversion failed")
			err = errors.Wrap(err, "price range conversion failed")
			fmt.Fprintf(w, "%s", err)
			return
		}
		to, err = strconv.Atoi(r[1])
		if err != nil {
			fmt.Println("price range conversion failed")
			err = errors.Wrap(err, "price range conversion failed")
			fmt.Fprintf(w, "%s", err)
			return
		}
	} else {
		from, to = -1, -1
	}
	// handle different requests
	switch req.Method {
	case "GET":
		result, err = searchBook(client, ctx, title, authorName, Range{from, to})
	default:
		msg := "Unsupported request for /search " + req.Method
		err = errors.New(msg)
	}
	if err != nil {
		fmt.Fprintf(w, "%s", err)
	} else {
		// handle write to redis
		if userId != "" {
			err = writeToRedis(userId, "search", req.Method)
			if err != nil {
				fmt.Fprintf(w, "%s", err)
			}
		}
		fmt.Fprintf(w, "%s", result)
	}
}
func main() {
	// handle different routes
	http.HandleFunc("/book", book)
	http.HandleFunc("/search", search)
	http.HandleFunc("/store", store)
	http.HandleFunc("/activity", activity)
	// listen and serve
	http.ListenAndServe(":8080", nil)
}
