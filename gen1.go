package main

import (
    "bufio"
    "encoding/csv"
    "os"
    "fmt"
    "io"
    "time"
    "strconv"
    "math/rand"
    "strings"
)

func generatetx(cust int, ipaddress string, frequency time.Duration) {
    // keep going till main dies
    //
    // wait a random time so that all the customers don't access simultaneously
    s := rand.NewSource(int64(cust))
    r := rand.New(s)
    d := time.Duration(r.Int63n(int64(frequency))) 
    fmt.Println("going to sleep for " + d.String())
    time.Sleep(d)
    fmt.Println("woke up")
    // front part of log entry is ip address, user-identifier
    logstart := ipaddress + " - cust" + strconv.Itoa(cust) + " ["
    for {
        logentry := ""

        // Load the transaction pattern file.
        f, _ := os.Open("transactionpattern.csv")

        // Create a new reader.
        r := csv.NewReader(bufio.NewReader(f))

        today := time.Now()
        fmt.Println(today)

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
	    } else {
                fmt.Println("wrong input")
            }
            fmt.Println(logentry)        
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