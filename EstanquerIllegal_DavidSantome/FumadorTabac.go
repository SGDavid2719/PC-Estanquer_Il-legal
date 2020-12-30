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

// Fumador tabaco envía una petición para tabaco y al cogerlo fuma,
// espera unos instantes y solicita otro más. En caso de recibir un mensaje
// sobre la policía se va.
func main() {

	// GESTIÓN DE ERRORES

	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	// TABACO

	chTabac, err := conn.Channel()
	failOnError(err, "Failed to open a channel (chTabac)")
	defer chTabac.Close()

	q, err := chTabac.QueueDeclare("tQueue", false, true, false, false, nil)
	failOnError(err, "Failed to declare a queue (chTabac)")

	// Avisa al RabbitMQ de no dar más de un tabaco (mensaje) a un Fumador cada vez
	err = chTabac.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	failOnError(err, "Failed to set QoS (chTabac")

	msgsT, err := chTabac.Consume(q.Name, "", false, false, false, false, nil)
	failOnError(err, "Failed to register a consumer (chTabac")

	// PETICIONES

	chPeticionsClients, err := conn.Channel()
	failOnError(err, "Failed to open a channel (chPeticionsClients)")
	defer chPeticionsClients.Close()

	s, err := chPeticionsClients.QueueDeclare("pQueue", false, true, false, false, nil)
	failOnError(err, "Failed to declare a queue (chPeticionsClients)")

	// VARIABLES

	salir := false

	// INICIO DEL PROCESO

	log.Printf("Sóm fumador. Tinc mistos però me falta tabac")

	forever := make(chan bool)

	for {

		bodyDTabac := "dT"

		err = chPeticionsClients.Publish("", s.Name, false, false, amqp.Publishing{DeliveryMode: amqp.Persistent, ContentType: "text/plain", Body: []byte(bodyDTabac)})
		failOnError(err, "Failed to publish a message (chPeticionsClients)")

		// Espera a recibir el Tabac
		for d := range msgsT {

			if string(d.Body) != "policia" {

				d.Ack(false)
				log.Printf("He hagafat el tabac %s. Gràcies!", d.Body)

				// Mensaje previo a realizar otra petición
				for i := 0; i < 3; i++ {
					log.Printf("·")
					time.Sleep(1 * time.Second)
				}
				log.Printf("Me dones més tabac?")

			} else {

				// No realizamos d.Ack(false) para no consumir el mensaje en caso de que haya más de un consumidor

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
