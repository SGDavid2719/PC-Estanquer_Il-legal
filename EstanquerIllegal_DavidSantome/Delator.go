// Autor: David Santomé Galván

package main

import (
	"log"
	"time"

	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

// Delator al llegar informa sobre la llegada de la policía y se va
func main() {

	// GESTIÓN DE ERRORES

	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	// PETICIONES

	chPeticionsClients, err := conn.Channel()
	failOnError(err, "Failed to open a channel (chPeticionsClients)")
	defer chPeticionsClients.Close()

	s, err := chPeticionsClients.QueueDeclare("pQueue", false, true, false, false, nil)
	failOnError(err, "Failed to declare a queue (chPeticionsClients)")

	// INICIO DEL PROCESO

	log.Printf("No sóm fumador. ALERTA! Que ve la policia!")

	bodyDelator := "pD"

	err = chPeticionsClients.Publish("", s.Name, false, false, amqp.Publishing{DeliveryMode: amqp.Persistent, ContentType: "text/plain", Body: []byte(bodyDelator)})
	failOnError(err, "Failed to publish a message (chPeticionsClients)")

	time.Sleep(3 * time.Second)

	for i := 0; i < 3; i++ {
		log.Printf("·")
		time.Sleep(1 * time.Second)
	}
}
