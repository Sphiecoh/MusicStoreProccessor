package main

import (
	"encoding/json"
	"flag"
	"log"

	"fmt"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	"github.com/streadway/amqp"
)

type Order struct {
	gorm.Model
	Address     string `json:"address"`
	Ordernumber int    `json:"ordernumber"`
	Userid      string `json:"userid"`
	Status      string
}
type NewOrder struct {
	Id     uint
	Status string
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

var (
	uri         = flag.String("uri", "amqp://guest:guest@localhost:5672/", "AMQP URI")
	queue       = flag.String("queue", "musicstore-orders", "Ephemeral AMQP queue name")
	replyqueue  = flag.String("replyqueue", "musicstore-orders-reply", "Ephemeral AMQP queue name")
	consumerTag = flag.String("consumer-tag", "musicstore-orders-goworker", "AMQP consumer tag (should not be blank)")
	dbhost      = flag.String("dbhost", "localhost", "Database host")
	dbname      = flag.String("dbname", "shippingservice-development", "Database name")
	dbpassword  = flag.String("dbpassword", "skhokho", "Database password")
	dbuser      = flag.String("dbuser", "postgres", "Database user")
)

func init() {
	flag.Parse()
}
func main() {
	conn, err := amqp.Dial(*uri)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()
	db, err := gorm.Open("postgres", fmt.Sprintf("host=%s user=%s dbname=%s sslmode=disable password=%s", *dbhost, *dbuser, *dbname, *dbpassword))
	failOnError(db.Error, "cannot open connection to database")
	db.AutoMigrate(&Order{})
	defer db.Close()
	ch, err := conn.Channel()
	replych, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()
	defer replych.Close()

	q, err := ch.QueueDeclare(
		*queue, // name
		true,   // durable
		false,  // delete when unused
		false,  // exclusive
		false,  // no-wait
		nil,    // arguments
	)

	failOnError(err, "Failed to declare a queue")

	err = ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	failOnError(err, "Failed to set QoS")

	msgs, err := ch.Consume(
		q.Name,       // queue
		*consumerTag, // consumer
		false,        // auto-ack
		false,        // exclusive
		false,        // no-local
		false,        // no-wait
		nil,          // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for msg := range msgs {
			var storeorder = Order{Status: "recieved"}
			err := json.Unmarshal(msg.Body, &storeorder)
			if err != nil {
				msg.Nack(false, true)
			}
			failOnError(err, "Failed to desirialise message")

			db.Create(&storeorder)
			failOnError(db.Error, "Failed to save order")
			log.Printf("Recieved message %s for order %d", msg.CorrelationId, storeorder.Ordernumber)
			msg.Ack(true)

			message, er := json.Marshal(storeorder)
			failOnError(er, "Failed to serialize order status")
			err = replych.Publish(
				"",          // exchange
				*replyqueue, // routing key
				false,       // mandatory
				false,       // immediate
				amqp.Publishing{
					ContentType:   "text/plain",
					Body:          message,
					CorrelationId: msg.CorrelationId,
				})
			log.Printf("Published message %s for order %d with shipping %d", msg.CorrelationId, storeorder.Ordernumber, storeorder.ID)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
