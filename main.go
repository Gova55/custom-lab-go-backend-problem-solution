package main

import (
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

type Request struct {
	Ev         string `json:"ev"`
	Et         string `json:"et"`
	ID         string `json:"id"`
	UID        string `json:"uid"`
	MID        string `json:"mid"`
	T          string `json:"t"`
	P          string `json:"p"`
	L          string `json:"l"`
	SC         string `json:"sc"`
	ATRK1      string `json:"atrk1"`
	ATRV1      string `json:"atrv1"`
	ATRT1      string `json:"atrt1"`
	ATRK2      string `json:"atrk2"`
	ATRV2      string `json:"atrv2"`
	ATRT2      string `json:"atrt2"`
	ATRK3      string `json:"atrk3"`
	ATRV3      string `json:"atrv3"`
	ATRT3      string `json:"atrt3"`
	ATRK4      string `json:"atrk4"`
	ATRV4      string `json:"atrv4"`
	ATRT4      string `json:"atrt4"`
	ATRK5      string `json:"atrk5"`
	ATRV5      string `json:"atrv5"`
	ATRT5      string `json:"atrt5"`
	ATRK6      string `json:"atrk6"`
	ATRV6      string `json:"atrv6"`
	ATRT6      string `json:"atrt6"`
	UATRK1     string `json:"uatrk1"`
	UATRV1     string `json:"uatrv1"`
	UATRT1     string `json:"uatrt1"`
	UATRK2     string `json:"uatrk2"`
	UATRV2     string `json:"uatrv2"`
	UATRT2     string `json:"uatrt2"`
	UATRK3     string `json:"uatrk3"`
	UATRV3     string `json:"uatrv3"`
	UATRT3     string `json:"uatrt3"`
	UATRK4     string `json:"uatrk4,omitempty"`
	UATRV4     string `json:"uatrv4,omitempty"`
	UATRT4     string `json:"uatrt4,omitempty"`
	UATRK5     string `json:"uatrk5,omitempty"`
	UATRV5     string `json:"uatrv5,omitempty"`
	UATRT5     string `json:"uatrt5,omitempty"`
	UATRK6     string `json:"uatrk6,omitempty"`
	UATRV6     string `json:"uatrv6,omitempty"`
	UATRT6     string `json:"uatrt6,omitempty"`
}

type Message struct {
	Request      Request
	ResponseChan chan map[string]interface{}
}

// Creating an channel to communicate between HTTP server and worker
var workerChannel = make(chan Message)

func main() {
	// Initialize Gin router
	router := gin.Default()


	router.POST("/process-event", func(c *gin.Context) {
		var req Request
		if err := c.BindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		// Creating an response channel for this request
		responseChan := make(chan map[string]interface{})

		// Sending the request and response channel to the worker
		workerChannel <- Message{Request: req, ResponseChan: responseChan}

		// Wait for the worker to send the transformed message
		transformedMessage := <-responseChan

		c.JSON(http.StatusOK, transformedMessage)
	})

	// Start the worker as a goroutine when the program starts
	go processMessages()

	// Run the HTTP server
	if err := router.Run(":8080"); err != nil {
		log.Fatal("Failed to start server: ", err)
	}
}

// Worker function to process messages
func processMessages() {
	for {
		select {
		case msg := <-workerChannel:

			req := msg.Request

			// Initialize attributes map
			attributes := make(map[string]interface{})
			traits := make(map[string]interface{})


			// Add known attributes
			attributes[req.ATRK1] = map[string]interface{}{
				"value": req.ATRV1,
				"type":  req.ATRT1,
			}
			attributes[req.ATRK2] = map[string]interface{}{
				"value": req.ATRV2,
				"type":  req.ATRT2,
			}

			// Add "atrk3" attributes to the attributes map
			if req.ATRK3 != "" {
				attributes[req.ATRK3] = map[string]interface{}{
					"value": req.ATRV3,
					"type":  req.ATRT3,
				}
			}

			if req.ATRK4 != "" {
				attributes[req.ATRK4] = map[string]interface{}{
					"value": req.ATRV4,
					"type":  req.ATRT4,
				}
			}

			if req.ATRK5 != "" {
				attributes[req.ATRK5] = map[string]interface{}{
					"value": req.ATRV5,
					"type":  req.ATRT5,
				}
			}
			if req.ATRK6 != "" {
				attributes[req.ATRK6] = map[string]interface{}{
					"value": req.ATRV6,
					"type":  req.ATRT6,
				}
			}

			// Add traits for UATRK1, UATRK2, UATRK3
			traits[req.UATRK1] = map[string]interface{}{
				"value": req.UATRV1,
				"type":  req.UATRT1,
			}
			traits[req.UATRK2] = map[string]interface{}{
				"value": req.UATRV2,
				"type":  req.UATRT2,
			}
			traits[req.UATRK3] = map[string]interface{}{
				"value": req.UATRV3,
				"type":  req.UATRT3,
			}


			if req.UATRK4 != "" {
				traits[req.UATRK4] = map[string]interface{}{
					"value": req.UATRV4,
					"type":  req.UATRT4,
				}
			}
			if req.UATRK5 != "" {
				traits[req.UATRK5] = map[string]interface{}{
					"value": req.UATRV5,
					"type":  req.UATRT5,
				}
			}
			if req.UATRK6 != "" {
				traits[req.UATRK6] = map[string]interface{}{
					"value": req.UATRV6,
					"type":  req.UATRT6,
				}
			}

			// Create the transformed message
			transformedMessage := map[string]interface{}{
				"event":             req.Ev,
				"event_type":        req.Et,
				"app_id":            req.UID,
				"title":             req.T,
				"url":               req.P,
				"language":          req.L,
				"screen_resolution": req.SC,
				"attributes":        attributes,
				"traits": traits,
			}

			// Simulate processing time
			time.Sleep(time.Second)

			// Send the transformed message back to the response channel
			msg.ResponseChan <- transformedMessage
		}
	}
}
