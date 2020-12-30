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

// Fumador mistos envía una petición para mistos y al cogerlo fuma,
// espera unos instantes y solicita otro más. En caso de recibir un mensaje
// sobre la policía se va.
func main() {

	// GESTIÓN DE ERRORES

	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	// MISTOS

	chMistos, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer chMistos.Close()

	r, err := chMistos.QueueDeclare("mQueue", false, true, false, false, nil)
	failOnError(err, "Failed to declare a queue (chMistos)")

	err = chMistos.Qos(1, 0, false)
	failOnError(err, "Failed to set QoS (chMistos)")

	msgsM, err := chMistos.Consume(r.Name, "", false, false, false, false, nil)
	failOnError(err, "Failed to register a consumer (chMistos)")

	// PETICIONES

	chPeticionsClients, err := conn.Channel()
	failOnError(err, "Failed to open a channel (chPeticionsClients)")
	defer chPeticionsClients.Close()

	s, err := chPeticionsClients.QueueDeclare("pQueue", false, true, false, false, nil)
	failOnError(err, "Failed to declare a queue (chPeticionsClients)")

	// VARIABLES

	salir := false

	// INICIO DEL PROCESO

	log.Printf("Sóm fumador. Tinc tabac però me falten mistos")

	forever := make(chan bool)

	for {

		bodyDMistos := "dM"

		err = chPeticionsClients.Publish("", s.Name, false, false, amqp.Publishing{DeliveryMode: amqp.Persistent, ContentType: "text/plain", Body: []byte(bodyDMistos)})
		failOnError(err, "Failed to publish a message (chPeticionsClients)")

		// Espera a recibir el Misto
		for m := range msgsM {

			if string(m.Body) != "policia" {

				m.Ack(false)
				log.Printf("He hagafat el misto %s. Gràcies!", m.Body)

				for i := 0; i < 3; i++ {
					log.Printf("·")
					time.Sleep(1 * time.Second)
				}
				log.Printf("Me dones un altre misto?")

			} else {

				// No realizamos m.Ack(false) para no consumir el mensaje en caso de que haya más de un consumidor

				salir = true

				time.Sleep(3 * time.Second)

				log.Printf("Anem que ve la policia!")
			}
			break
		}

		// Rompemos el bucle en caso de salida
		if salir {
			break
		}
	}

	// CONDICIÓN DE SALIDA

	if !salir {
		<-forever
	}
}
