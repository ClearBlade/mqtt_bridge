/*

Example run command:

mqtt_bridge -cb_systemKey="82b8b78c0bcc90a7d5bcc2fcf5e901" -cb_systemSecret="82B8B78C0BB4B8CF8AB695AAD7C101" -cb_platformURL="https://dev-sbd.clearblade.com" -cb_messagingURL="dev-sbd.clearblade.com:1883" -cb_email="rreinold@clearblade.com" -cb_password="clearblade" -vt_systemKey="bcb3f4870b92f884a4d9e6ffcb4f" -vt_systemSecret="BCB3F4870BACC3D095D196988836" -vt_platformURL="https://staging.clearblade.com" -vt_messagingURL="staging.clearblade.com:1883" -vt_email="rreinold@clearblade.com" -vt_password="clearblade"

*/


package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"sync"
	cb "github.com/clearblade/Go-SDK"
	mqtt "github.com/clearblade/mqtt_parsing"
)

var DEBUG bool = true
var DEFAULT_NUMBER_OF_BRIDGE_WORKERS_PER_CLIENT = 5

// TODO Refactor into struct
var CLEARBLADE_CLIENT_ID string = "cb_mqtt_bridge"
// Topics to forward to VT
var DEFAULT_CLEARBLADE_TOPICS = []string{
	"viewtech/update", 
	"viewtech/status"}

var VIEWTECH_CLIENT_ID string = "vt_mqtt_bridge"
// Topics to forward to ClearBlade
var DEFAULT_VIEWTECH_TOPICS = []string{
	"monitor/viewtech/+", 
	"sensor/antenna"}

// Quality of service
var QoS int = 0

// TODO Establish device auth
var (
	// ClearBlade MQTT Client
	clearBladeClient *cb.UserClient
	// ClearBlade Configuration
	cb_platformURL    string
	cb_messagingURL    string
	cb_systemKey     string
	cb_systemSecret     string
	cb_email      string
	cb_password   string
	vt_password   string
	// View Technologies MQTT Client
	viewTechClient *cb.UserClient
	// View Technologies Configuration
	vt_platformURL    string
	vt_messagingURL    string
	vt_systemKey     string
	vt_systemSecret     string
	vt_email      string
)

// TODO Add flag for topics
func init() {
	flag.StringVar(&cb_systemKey, "cb_systemKey", "", "ClearBlade System Key (required)")
	flag.StringVar(&cb_systemSecret, "cb_systemSecret", "", "ClearBlade System Secret (required)")
	flag.StringVar(&cb_platformURL, "cb_platformURL", "", "ClearBlade Platform URL)")
	flag.StringVar(&cb_messagingURL, "cb_messagingURL", "", "ClearBlade Messaging URL")
	flag.StringVar(&cb_email, "cb_email", "", "ClearBlade Email Credentials (required)")
	flag.StringVar(&cb_password, "cb_password", "", "cb_password (required)")

	if(DEBUG){
		flag.StringVar(&vt_systemKey, "vt_systemKey", "", "system key (required)")
		flag.StringVar(&vt_systemSecret, "vt_systemSecret", "", "system secret (required)")
		flag.StringVar(&vt_platformURL, "vt_platformURL", "", "platform url)")
	}

	flag.StringVar(&vt_messagingURL, "vt_messagingURL", "", "system key")
	flag.StringVar(&vt_email, "vt_email", "", "vt_email (required)")
	flag.StringVar(&vt_password, "vt_password", "", "vt_password (required)")
}

func main() {
	flag.Parse()

	log.Println("Initializing Bridge(s):")
	
	// Initialize MQTT Clients
	clearBladeClient, err := initClient(
		CLEARBLADE_CLIENT_ID,
		clearBladeClient, 
		cb_platformURL, 
		cb_messagingURL, 
		cb_systemKey, 
		cb_systemSecret, 
		cb_email, 
		cb_password)

	if err != nil {
		fmt.Println(err.Error())
		fmt.Println("Unable to initialize ClearBlade MQTT Client")
		return
	}

	viewTechClient, err = initClient(
		VIEWTECH_CLIENT_ID,
		viewTechClient, 
		vt_platformURL, 
		vt_messagingURL, 
		vt_systemKey, 
		vt_systemSecret, 
		vt_email, 
		vt_password)

	if err != nil {
		fmt.Println(err.Error())
		errorMessage := "Unable to initialize View Technologies MQTT Client"
		fmt.Println(errorMessage)
		// Publish monitor message
    	publish(clearBladeClient, "log/fatal/mqtt_bridge",errorMessage)
		return
	}

	log.Println("Begin Configure Subscription(s)")
	fromClearBlade,_ 	:= 	subscribe(clearBladeClient, DEFAULT_CLEARBLADE_TOPICS[0:])
	fromVT,_			:= 	subscribe(viewTechClient, 	DEFAULT_VIEWTECH_TOPICS[0:])

	log.Printf("Deploying %d Workers",DEFAULT_NUMBER_OF_BRIDGE_WORKERS_PER_CLIENT * 2)
    for w := 0; w < DEFAULT_NUMBER_OF_BRIDGE_WORKERS_PER_CLIENT; w++ {
        go bridgeWorker(w, "ClearBlade", "View Technologies", fromClearBlade, viewTechClient)
        go bridgeWorker(w, "View Technologies", "ClearBlade", fromVT, clearBladeClient)
    }

    if(DEBUG){ log.Println("Sending monitor messages to ClearBlade")}
    
    // Publish monitor message
    publish(clearBladeClient, "log/info/mqtt_bridge","Bridge is Online")

	if(DEBUG){
		// Log to our test broker, as well
		publish(viewTechClient, "log/info/mqtt_bridge","Bridge is Online")
	}

	// TODO: Keep main thread running
    select { }
}

// Waits for messages and publishes them to recipient
func bridgeWorker(id int, origin string, recipient string, channels []<-chan *mqtt.Publish, recipientClient *cb.UserClient) {
	
	bridgeWorkerID := id + 1

    for message := range merge(channels) { // each new message
    	var payload string = string(message.Payload)
        var topic string = message.Topic.Whole

		log.Printf("  Bridge Worker %d: Bridging %s to %s", bridgeWorkerID, origin, recipient)
		messageDescription := "Forwarding topic: " + topic + ", payload: " + payload
        log.Println("    " + messageDescription)

        if err := publish(recipientClient, topic, payload);err != nil {
        	// Publish monitor message
    		publish(clearBladeClient, "log/fatal/mqtt_bridge","Bridge Failed To Publish " + messageDescription + " to " + recipient)
        }
    }
}

// Subscribes to an array of topics
func subscribe(userClient *cb.UserClient, topics []string) ([]<-chan *mqtt.Publish, error) {
	var size = len(topics)
	var subscriptions = make([]<-chan *mqtt.Publish, size)
	log.Printf("Created %x subscription(s) for %s",size, userClient.MQTTClient.Clientid)
	for i := range topics {
		topic := topics[i]
		log.Println("  " + topic)
		subscription, error := userClient.Subscribe(topic, QoS)
		if error != nil {
			fmt.Printf("Unable to subscribe to topic: %s due to error: %s",topic,error)
			return nil, error
		}
		subscriptions[i] = subscription
    }
	return subscriptions, nil
}



// MQTT Client init helper
func initClient(clientID string, userClient *cb.UserClient, cb_platformURL, cb_messagingURL, cb_systemKey, cb_systemSecret, cb_email, cb_password string) (*cb.UserClient,error) {
	
	userClient = cb.NewUserClientWithAddrs(cb_platformURL, cb_messagingURL, cb_systemKey, cb_systemSecret, cb_email, cb_password)
	
	log.Println("  " + clientID)

	if err := userClient.Authenticate(); err != nil {
		log.Fatalf("Error authenticating: %s", err.Error())
		return nil, err
	}

	if err := userClient.InitializeMQTT(clientID, "", 30); err != nil {
		log.Fatalf("Unable to initialize MQTT: %s", err.Error())
		return nil, err
	}

	if err := userClient.ConnectMQTT(nil, nil); err != nil {
		log.Fatalf("Unable to connect MQTT: %s", err.Error())
		return nil, err
	}
	return userClient, nil
}

// Publish helper
func publish(userClient *cb.UserClient, topic string, payload string) error {

	marshalledPayload, err := json.Marshal(payload)
	if err != nil {
		fmt.Println(err)
		return err
	}
	if err := userClient.Publish(topic, marshalledPayload, 2); err != nil {
		log.Printf("Unable to publish request: %s", err.Error())
		return err
	} else{
		log.Printf("  published to %s", userClient.MQTTClient.Clientid)
		return nil
	}
}

// https://blog.golang.org/pipelines
// Fan-In Channel Helper
func merge(cs []<-chan *mqtt.Publish) <-chan *mqtt.Publish {
    var wg sync.WaitGroup
    out := make(chan *mqtt.Publish)

    // Start an output goroutine for each input channel in cs.  output
    // copies values from c to out until c is closed, then calls wg.Done.
    output := func(c <-chan *mqtt.Publish) {
        for n := range c {
            out <- n
        }
        wg.Done()
    }
    wg.Add(len(cs))
    for _, c := range cs {
        go output(c)
    }

    // Start a goroutine to close out once all the output goroutines are
    // done.  This must start after the wg.Add call.
    go func() {
        wg.Wait()
        close(out)
    }()
    return out
}
