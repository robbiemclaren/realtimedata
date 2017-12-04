package main

import (
    "bufio"
    "encoding/csv"
    "os"
    "flag"	
    "fmt"
    "io"
    "time"
    "strconv"
    "math/rand"
    "strings"
    "github.com/Shopify/sarama"
    log "github.com/Sirupsen/logrus"
)

var (
	version bool
	// name of the kafka topic
	topic string
	// FQDN/IP + port of a Kafka broker:
	broker string
	// how many seconds to wait between generating a message (default is 2):
	genwaitsec time.Duration
	// the Kafka producer:
	producer sarama.SyncProducer
)

func init() {
	flag.BoolVar(&version, "version", false, "Display version information")
	flag.StringVar(&broker, "broker", "", "The FQDN or IP address and port of a Kafka broker. Example: broker-1.kafka.mesos:9382 or 10.0.3.178:9398")
        flag.StringVar(&topic, "topic", "webserverlog", "The name of the Kafka into which the data will be written.")	
        flag.Usage = func() {
		fmt.Printf("Usage: %s [args]\n\n", os.Args[0])
		fmt.Println("Arguments:")
		flag.PrintDefaults()
	}
	flag.Parse()

	genwaitsec = 1
	if gw := os.Getenv("GEN_WAIT_SEC"); gw != "" {
		if gwi, err := strconv.Atoi(gw); err == nil {
			genwaitsec = time.Duration(gwi)
		}
	}
}

func generatetx(cust int, ipaddress string, frequency time.Duration) {
    // keep going till main dies
    //
    // wait a random time so that all the customers don't access simultaneously
    s := rand.NewSource(int64(cust))
    r := rand.New(s)
    d := time.Duration(r.Int63n(int64(frequency))) 
    fmt.Println("going to sleep for " + d.String())
    time.Sleep(d)
    // front part of log entry is ip address, user-identifier
    logstart := ipaddress + " - cust" + strconv.Itoa(cust) + " ["
    for {
        logentry := ""

        // Load the transaction pattern file.
        f, _ := os.Open("D:\\Code\\realtimedata\\generator\\transaction pattern.csv")

        // Create a new reader.
        r := csv.NewReader(bufio.NewReader(f))

        today := time.Now()

        for {
            record, err := r.Read()
            // Stop at EOF.
            if err == io.EOF {
                break 
            }
            
            // transaction pattern record should have three values per line
   	    // ignore lines that don't have three records
	    if len(record) == 3 {
                // add the records to the log entry
                logentry = logstart
                // use the current date and convert to web server log format
                logentry = logentry + strconv.Itoa(today.Day()) + "\\" + today.Month().String() + "\\" + strconv.Itoa(today.Year()) + ":" 
                // add the time from the pattern file to the start time
                faketime := today
                // offset comes as hh:mm:ss in a string
                offset := strings.Split(record[0],":")
                var offsetduration time.Duration = 0;
                if len(offset) == 3 {
                    durhour, err := strconv.Atoi(offset[0])
                    durminute, err := strconv.Atoi(offset[1])
                    dursecond, err := strconv.Atoi(offset[2])
                    fmt.Println(err)
                    offsetduration = time.Duration(durhour) * time.Hour + time.Duration(durminute) * time.Minute + time.Duration(dursecond) * time.Second
                }
                faketime = faketime.Add(offsetduration)  
                logentry = logentry + strconv.Itoa(faketime.Hour()) + ":" + strconv.Itoa(faketime.Minute()) + ":" + strconv.Itoa(faketime.Second()) + " 0000]"
                logentry = logentry + " \"" + record[1] + "\" 200 "+ record[2]
	    }
            fmt.Println(logentry)
	    rawmsg := logentry // fmt.Sprintf("%s;%d;%d;%d", currentTime, source, target, amount)
            msg := &sarama.ProducerMessage{Topic: string(topic), Value: sarama.StringEncoder(rawmsg)}
            if _, _, err := producer.SendMessage(msg); err != nil {
                log.Error("Failed to send message ", err)
            } else {
                log.Info(fmt.Sprintf("%#v", msg))
            }
        }
    	time.Sleep(frequency)
    }
}

func transact (cust int, ipaddress string, aggregator bool) {
    if aggregator == false {
        // customer not using aggregator
        go generatetx(cust, ipaddress, 3 * time.Minute) 
    } else {
        // customer using aggregator
    	go generatetx(cust, "208.78.71.141", 1 * time.Minute)
    }    
}

func main() {
    if version {
        // about()
        os.Exit(0)
    }
    if broker == "" {
        flag.Usage()
        os.Exit(1)
    }

    if p, err := sarama.NewSyncProducer([]string{broker}, nil); err != nil {
        log.Error(err)
        os.Exit(1)
    } else {
        producer = p
    }
    defer func() {
        if err := producer.Close(); err != nil {
            log.Error(err)
            os.Exit(1)
        }
    }()

    // kick off concurrent customers to generate transactions
    for i := 1; i < 10; i++ {
        thisip := "77.103." + strconv.Itoa(i) + "." + strconv.Itoa(i)
	thisaggregator := false
	// just make an arbitary number of the processes use aggregator
	if i > 8 {
	    thisaggregator = true
	}
        go transact(i, thisip, thisaggregator)
    }
    // wait a while to let the concurrent customers processes run
    time.Sleep(10 * time.Minute)
}