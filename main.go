package main

import (
	"github.com/DusanKasan/parsemail"
	"github.com/Shopify/sarama"
	orient "gopkg.in/istreamdata/orientgo.v2"
	_ "gopkg.in/istreamdata/orientgo.v2/obinary"
	"log"
	"strings"
)

const (
	ORIENTDB_URL           = ""
	ORIENTDB_DATABASE_NAME = ""
	ORIENTDB_USER          = ""
	ORIENTDB_PASSWORD      = ""

	APACHE_KAFKA_URL   = ""
	APACHE_KAFKA_TOPIC = ""
)

func main() {

	brokers := []string{APACHE_KAFKA_URL}

	consumer, err := sarama.NewConsumer(brokers, nil)
	defer consumer.Close()
	if err != nil {
		log.Fatalln(err)
	}

	partitionList, err := consumer.Partitions(APACHE_KAFKA_TOPIC) //get all partitions
	if err != nil {
		log.Fatalln(err)
	}

	messages := make(chan *sarama.ConsumerMessage, 256)

	for _, partition := range partitionList {
		pc, _ := consumer.ConsumePartition(APACHE_KAFKA_TOPIC, partition, sarama.OffsetOldest)
		go func(pc sarama.PartitionConsumer) {
			for message := range pc.Messages() {
				messages <- message //or call a function that writes to disk
			}
		}(pc)
	}

	dbConn, err := GetOrientDbDatabaseConnection()
	defer dbConn.Close()
	if err != nil {
		log.Fatalln(err)
	}

	counter := 0
	for message := range messages {
		counter++

		r := strings.NewReader(string(message.Value))
		email, err := parsemail.Parse(r) // returns Email struct and error
		if err != nil {
			log.Println(err.Error())
		}

		err = insertEmail(dbConn, email)
		if err != nil {
			log.Println(err.Error())
		}
		println(counter)
	}
}

func insertEmail(dbConn *orient.Database, emailContent parsemail.Email) error {

	//create Account from FROM address
	var fromVertex *orient.Document
	if len(emailContent.From) > 0 {
		//check if account exists
		_ = dbConn.Command(orient.NewSQLCommand("SELECT FROM Account where email_address = ?", emailContent.From[0].Address)).All(&fromVertex)

		if fromVertex.RID.IsValid() && fromVertex.RID.String() == "#0:0" {
			_ = dbConn.Command(orient.NewSQLCommand("INSERT INTO Account (email_address) VALUES (?)", emailContent.From[0].Address)).All(&fromVertex)
		}
	}

	//create Account from TO address
	var toVertex *orient.Document
	if len(emailContent.To) > 0 {
		//check if account exists
		_ = dbConn.Command(orient.NewSQLCommand("SELECT FROM Account where email_address = ?", emailContent.To[0].Address)).All(&toVertex)

		if toVertex.RID.IsValid() && toVertex.RID.String() == "#0:0" {
			_ = dbConn.Command(orient.NewSQLCommand("INSERT INTO Account (email_address) VALUES (?)", emailContent.To[0].Address)).All(&toVertex)
		}
	}

	//create Account from CC address
	var ccVertex *orient.Document
	if len(emailContent.Cc) > 0 {
		//check if account exists
		_ = dbConn.Command(orient.NewSQLCommand("SELECT FROM Account where email_address = ?", emailContent.Cc[0].Address)).All(&ccVertex)

		if ccVertex.RID.IsValid() && ccVertex.RID.String() == "#0:0" {
			_ = dbConn.Command(orient.NewSQLCommand("INSERT INTO Account (email_address) VALUES (?)", emailContent.Cc[0].Address)).All(&toVertex)
		}
	}

	//create Account from BCC address
	var bccVertex *orient.Document
	if len(emailContent.Bcc) > 0 {
		//check if account exists
		_ = dbConn.Command(orient.NewSQLCommand("SELECT FROM Account where email_address = ?", emailContent.Bcc[0].Address)).All(&bccVertex)

		if bccVertex.RID.IsValid() && bccVertex.RID.String() == "#0:0" {
			_ = dbConn.Command(orient.NewSQLCommand("INSERT INTO Account (email_address) VALUES (?)", emailContent.Bcc[0].Address)).All(&bccVertex)
		}
	}

	//create Email vertex
	var emailVertex *orient.Document
	//check if Email exists
	_ = dbConn.Command(orient.NewSQLCommand("SELECT FROM Email where message_id = ?", emailContent.MessageID)).All(&emailVertex)

	if emailVertex.RID.IsValid() && emailVertex.RID.String() == "#0:0" {
		_ = dbConn.Command(orient.NewSQLCommand("INSERT INTO Email (content, date, message_id, subject) VALUES (?, ?, ?, ?)", emailContent.TextBody, emailContent.Date, emailContent.MessageID, emailContent.Subject)).All(&emailVertex)
	}

	//create Sent edge
	if fromVertex != nil && fromVertex.RID.IsValid() && emailVertex.RID.IsValid() {
		_ = dbConn.Command(orient.NewSQLCommand("CREATE EDGE Sent FROM ? TO ?", fromVertex.RID, emailVertex.RID))
	}

	//create Sent_to edge
	if toVertex != nil && toVertex.RID.IsValid() && emailVertex.RID.IsValid() {
		_ = dbConn.Command(orient.NewSQLCommand("CREATE EDGE Sent_to FROM ? TO ?", emailVertex.RID, toVertex.RID))
	}

	//create Cc edge
	if ccVertex != nil && ccVertex.RID.IsValid() && emailVertex.RID.IsValid() {
		_ = dbConn.Command(orient.NewSQLCommand("CREATE EDGE Cc FROM ? TO ?", emailVertex.RID, ccVertex.RID))
	}

	//create Bcc edge
	if bccVertex != nil && bccVertex.RID.IsValid() && emailVertex.RID.IsValid() {
		_ = dbConn.Command(orient.NewSQLCommand("CREATE EDGE Bcc FROM ? TO ?", emailVertex.RID, bccVertex.RID))
	}

	//create Reply_to edge
	if len(emailContent.InReplyTo) > 0 && emailVertex != nil && emailVertex.RID.IsValid() {
		//check if reply to email exists
		_ = dbConn.Command(orient.NewSQLCommand("CREATE EDGE Reply FROM ? TO (SELECT FROM Email where message_id = ?)", emailVertex.RID, emailContent.InReplyTo[0]))
	}

	return nil
}

func GetOrientDbDatabaseConnection() (*orient.Database, error) {

	client, err := orient.Dial(ORIENTDB_URL)
	if err != nil {
		log.Println(err.Error())
	}

	admin, err := client.Auth(ORIENTDB_USER, ORIENTDB_PASSWORD)
	if err != nil {
		log.Println(err.Error())
	}

	_, err = admin.DatabaseExists(ORIENTDB_DATABASE_NAME, orient.Persistent)
	if err != nil {
		log.Println(err.Error())
	}

	database, err := client.Open(ORIENTDB_DATABASE_NAME, orient.GraphDB, ORIENTDB_USER, ORIENTDB_PASSWORD)
	if err != nil {
		log.Println(err.Error())
	}

	return database, err
}
