package main

import "fmt"
import "math/rand"
import "os"
import "os/signal"
import "time"
import "net"
import kafka "github.com/Shopify/sarama"
import geoip "github.com/oschwald/geoip2-golang"

func producer(ints chan<- int32, duration time.Duration) {
	for {
		rand_int := rand.Int31() 
		ints <- rand_int
		jitter := duration - time.Duration(rand.Int31n(999))*time.Millisecond
		time.Sleep(jitter)
	}
}

func intToIP(i int32) string {
	a := (byte(i >> 24))
	b := (byte(i >> 16))
	c := (byte(i >> 8))
	d := (byte(i))
	str := fmt.Sprintf("%d.%d.%d.%d", a, b, c, d)
	return str
}

func consumer(src <-chan int32, dst <-chan int32, producer kafka.AsyncProducer) {
	topic := "network"

	db, err := geoip.Open("GeoLite2-City.mmdb")
	if err != nil {
		fmt.Println(err)
	}
	defer db.Close()

	for {
		src_ip := <-src
		src_str := intToIP(src_ip)
		dst_ip := <-dst
		dst_str := intToIP(dst_ip)

		src_ip_parsed := net.ParseIP(src_str)
		dst_ip_parsed := net.ParseIP(dst_str)
		src_record, err := db.City(src_ip_parsed)
		if err != nil {
			fmt.Println(err)
		}
		dst_record, err := db.City(dst_ip_parsed)
		if err != nil {
			fmt.Println(err)
		}
        src_coords := fmt.Sprintf("%v,%v",src_record.Location.Latitude, src_record.Location.Longitude)
        dst_coords := fmt.Sprintf("%v,%v",dst_record.Location.Latitude, dst_record.Location.Longitude)
		str := src_str + "," + dst_str + "," + "\"" + src_coords + "\"" + "," + "\"" + dst_coords + "\"" + "," + time.Now().UTC().String()
    fmt.Println(str)
		message := kafka.ProducerMessage{Topic: topic, Value: kafka.StringEncoder(str)}
		producer.Input() <- &message
	}
}

func main() {
	duration := 1000 * time.Millisecond
	src := make(chan int32)
	dst := make(chan int32)
	notify := make(chan os.Signal, 1)
	signal.Notify(notify, os.Interrupt, os.Kill)

	host := "127.0.0.1:9092"
	config := kafka.NewConfig()
	config.Producer.Return.Successes = true
	k_producer, err := kafka.NewAsyncProducer([]string{host}, config)
	if err != nil {
		panic(err)
	}

	go producer(src, duration)
	go producer(dst, duration)
	go consumer(src, dst, k_producer)

	go func(producer kafka.AsyncProducer) {
		for {
			<-producer.Successes()
		}
	}(k_producer)

	s := <-notify
	fmt.Println("signal:", s)
	fmt.Println("done.")

}
