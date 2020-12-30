// Autor: David Santomé Galván
// Enlace al vídeo: https://youtu.be/TEfyMYu_bak

package main

import (
	"log"
	"strconv"
	"time"

	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

// Tercera práctica de programación concurrente. En este ejercicio se pide simular
// el problema de los fumadores. Hay un "Estanquer" que pone encima de la mesa
// (canal) tabaco si se lo solicita un fumador sin tabaco o mistos en caso de un fumador
// sin mistos. Además, hay un delator que avisa cuando viene la policía, recogen la mesa
// y se van todos (terminan)

func main() {

	// GESTIÓN DE ERRORES

	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	// TABACO

	chTabac, err := conn.Channel()
	failOnError(err, "Failed to open a channel (chTabac)")
	defer chTabac.Close()

	// Declaramos un canal para poder enviar tabaco
	q, err := chTabac.QueueDeclare(
		"tQueue", // name
		false,    // durable
		true,     // delete when unused
		false,    // exclusive
		false,    // no-wait
		nil,      // arguments
	)
	failOnError(err, "Failed to declare a queue (chTabac)")

	// MISTOS

	chMistos, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer chMistos.Close()

	// Declaramos un canal para poder enviar mistos
	r, err := chMistos.QueueDeclare("mQueue", false, true, false, false, nil)
	failOnError(err, "Failed to declare a queue (chMistos)")

	// PETICIONES

	chPeticionsClients, err := conn.Channel()
	failOnError(err, "Failed to open a channel (chPeticionsClients)")
	defer chPeticionsClients.Close()

	// Declaramos un canal para poder recibir peticiones
	s, err := chPeticionsClients.QueueDeclare("pQueue", false, true, false, false, nil)
	failOnError(err, "Failed to declare a queue (chPeticionsClients)")

	// Declaramos un canal para poder recoger peticiones
	msgsP, err := chPeticionsClients.Consume(
		s.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)

	// VARIABLES

	indexT := 0
	indexM := 0
	salir := false

	// INICIO DEL PROCESO

	log.Printf("Hola, som l'estanquer il·legal")

	forever := make(chan bool)

	// Espera a tener una petición
	for f := range msgsP {

		// Observa si la petición es de Tabac, de Mistos o el aviso del Delator
		if string(f.Body) == "dT" {

			// Incrementamos el contador de tabaco
			indexT++

			// Avisa que ha recibido el mensaje correctamente y únicamente uno
			f.Ack(false)

			bodyT := strconv.Itoa(indexT)

			// Definimos la publicación del mensaje en el canal
			err = chTabac.Publish(
				"",     // exchange
				q.Name, // routing key
				false,  // mandatory
				false,  // immediate
				amqp.Publishing{
					// Si RabbitMQ cierra o crashea no olvidará ni los mensajes ni las colas mediante Persistent
					DeliveryMode: amqp.Persistent,
					ContentType:  "text/plain",
					Body:         []byte(bodyT),
				})
			failOnError(err, "Failed to publish a message (chTabac)")

			// Avisa que ha puesto el tabaco encima de la mesa
			log.Printf("He posat el tabac %s damunt la taula", bodyT)

		} else if string(f.Body) == "dM" {

			// Incrementamos el contador de mistos
			indexM++

			f.Ack(false)

			bodyM := strconv.Itoa(indexM)

			err = chMistos.Publish("", r.Name, false, false, amqp.Publishing{DeliveryMode: amqp.Persistent, ContentType: "text/plain", Body: []byte(bodyM)})
			failOnError(err, "Failed to publish a message (chMistos)")

			log.Printf("He posat el misto %s damunt la taula", bodyM)

		} else {

			f.Ack(false)

			salir = true

			body := "policia"

			// Definimos la publicación del mensaje sobre la policia en el canal de mistos y tabac
			err = chTabac.Publish("", q.Name, false, false, amqp.Publishing{DeliveryMode: amqp.Persistent, ContentType: "text/plain", Body: []byte(body)})
			failOnError(err, "Failed to publish a message (chMistos)")
			err = chMistos.Publish("", r.Name, false, false, amqp.Publishing{DeliveryMode: amqp.Persistent, ContentType: "text/plain", Body: []byte(body)})
			failOnError(err, "Failed to publish a message (chMistos)")

			// Delay para simular que está ocupado y avisa que se va
			time.Sleep(3 * time.Second)
			log.Printf("Uyuyuy la policia! Men vaig")
			for i := 0; i < 3; i++ {
				log.Printf("·")
				time.Sleep(1 * time.Second)
			}
			log.Printf("Men duc la taula!!!")
		}

		if salir {
			break
		}
	}

	// CONDICIÓN DE SALIDA

	if !salir {
		<-forever
	}
}
